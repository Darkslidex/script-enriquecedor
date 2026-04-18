"""Unificador de fuentes: scraper automático + LinkedIn Sales Navigator.

Lee CSVs de ambas fuentes, hace merge con reglas de prioridad por campo,
deduplica por nombre+dominio y exporta un único CSV con schema Prisma válido.

Reglas de prioridad en colisión (dos registros con el mismo _key):
  nombre, provincia, partido, sitio_web → Fuente A (scraper, más completa)
  contacto_nombre, contacto_cargo       → Fuente B (LinkedIn, más preciso para personas)
  email                                 → Fuente B si email_validado=true, si no → Fuente A
  telefono                              → Fuente B si existe, si no → Fuente A
  fuente_descubrimiento                 → "scraper+linkedin" cuando vienen de ambas

Uso:
    from merger.unifier import unify
    result = unify(vertical="parques_industriales")
    print(result.summary)
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ..core.config import get_settings
from ..core.logger import get_logger
from ..storage.csv_writer import CSV_HEADERS
from .normalizers import make_key, normalize_linkedin_row, normalize_scraper_row
from .validators import summarize_validation, validate_batch

log = get_logger("merger")

# Campos internos que NO deben aparecer en el CSV final
_INTERNAL_FIELDS = {"_key", "_source", "_issues"}


@dataclass
class UnifyResult:
    vertical: str
    scraper_count: int = 0
    linkedin_count: int = 0
    merged_count: int = 0       # registros que estaban en ambas fuentes
    only_scraper: int = 0
    only_linkedin: int = 0
    total: int = 0
    with_email: int = 0
    with_contact: int = 0
    invalid_count: int = 0
    output_path: Path | None = None
    validation_summary: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        lines = [
            f"{'═' * 47}",
            f"  RESUMEN DEL MERGE — {self.vertical.replace('_', ' ').title()}",
            f"{'═' * 47}",
            f"  Fuente A (scraper):     {self.scraper_count:>6} registros",
            f"  Fuente B (LinkedIn):    {self.linkedin_count:>6} registros",
            f"  Registros en común:     {self.merged_count:>6} (mergeados con prioridad)",
            f"  Solo scraper:           {self.only_scraper:>6}",
            f"  Solo LinkedIn:          {self.only_linkedin:>6}",
            f"  {'─' * 43}",
            f"  TOTAL UNIFICADO:        {self.total:>6} registros",
            f"  Con email:              {self.with_email:>6} ({self._pct(self.with_email)}%)",
            f"  Con contacto nombrado:  {self.with_contact:>6} ({self._pct(self.with_contact)}%)",
        ]
        if self.invalid_count:
            lines.append(f"  Inválidos (excluidos):  {self.invalid_count:>6}")
        if self.output_path:
            lines.append(f"{'═' * 47}")
            lines.append(f"  → Exportado: {self.output_path}")
        return "\n".join(lines)

    def _pct(self, n: int) -> int:
        return 100 * n // self.total if self.total else 0


# ── Carga de fuentes ───────────────────────────────────────────────────────────

def _load_scraper_csvs(vertical: str, data_dir: Path) -> list[dict[str, str]]:
    """Lee todos los CSVs del scraper para un vertical dado."""
    output_dir = data_dir / "output"
    rows: list[dict[str, str]] = []
    for path in sorted(output_dir.glob(f"scraper_{vertical}_*.csv")):
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(normalize_scraper_row(dict(row)))
            log.debug("scraper_csv_loaded", file=path.name, rows=len(rows))
        except Exception as e:
            log.warning("scraper_csv_error", file=path.name, error=str(e)[:80])
    return rows


def _load_linkedin_csvs(data_dir: Path) -> list[dict[str, str]]:
    """Lee todos los CSVs de LinkedIn desde data/input/linkedin/."""
    input_dir = data_dir / "input" / "linkedin"
    rows: list[dict[str, str]] = []
    if not input_dir.exists():
        log.info("linkedin_input_dir_missing", path=str(input_dir))
        return rows
    for path in sorted(input_dir.glob("*.csv")):
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    normalized = normalize_linkedin_row(dict(row))
                    if normalized.get("nombre"):
                        rows.append(normalized)
            log.debug("linkedin_csv_loaded", file=path.name)
        except Exception as e:
            log.warning("linkedin_csv_error", file=path.name, error=str(e)[:80])
    return rows


# ── Merge con prioridad ────────────────────────────────────────────────────────

def _merge_rows(scraper_row: dict[str, str], linkedin_row: dict[str, str]) -> dict[str, str]:
    """Merge de dos filas con la misma _key. Aplica reglas de prioridad del handoff."""
    merged = dict(scraper_row)  # base: Fuente A

    # contacto_nombre y contacto_cargo → LinkedIn gana siempre
    if linkedin_row.get("contacto_nombre"):
        merged["contacto_nombre"] = linkedin_row["contacto_nombre"]
    if linkedin_row.get("contacto_cargo"):
        merged["contacto_cargo"] = linkedin_row["contacto_cargo"]

    # email → LinkedIn si está validado, si no queda el del scraper
    ln_email = linkedin_row.get("email", "").strip()
    ln_validado = linkedin_row.get("email_validado", "false") == "true"
    if ln_email and (ln_validado or not merged.get("email")):
        merged["email"] = ln_email
        if ln_validado:
            merged["email_validado"] = "true"
            merged["email_score"] = linkedin_row.get("email_score", "")

    # telefono → LinkedIn si existe
    ln_phone = linkedin_row.get("telefono", "").strip()
    if ln_phone and not merged.get("telefono"):
        merged["telefono"] = ln_phone

    # metadata → combinar (LinkedIn agrega linkedin_url)
    ln_meta = linkedin_row.get("metadata", "")
    if ln_meta and ln_meta not in merged.get("metadata", ""):
        existing = merged.get("metadata", "")
        if existing and existing != "{}":
            # Merge simple: mantener scraper, agregar linkedin_url si no está
            merged["metadata"] = existing  # conservar scraper (más rico)
        else:
            merged["metadata"] = ln_meta

    # fuente_descubrimiento → combinar
    src_a = merged.get("fuente_descubrimiento", "")
    src_b = linkedin_row.get("fuente_descubrimiento", "")
    if src_a and src_b and src_a != src_b:
        merged["fuente_descubrimiento"] = "scraper+linkedin"
    elif src_b and not src_a:
        merged["fuente_descubrimiento"] = src_b

    merged["_source"] = "scraper+linkedin"
    return merged


def _clean_row(row: dict[str, str]) -> dict[str, str]:
    """Elimina campos internos y asegura que la fila tenga todos los CSV_HEADERS."""
    clean = {k: row.get(k, "") for k in CSV_HEADERS}
    # Timestamp si falta
    if not clean.get("fecha_enriquecimiento"):
        clean["fecha_enriquecimiento"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # estado_comercial default
    if not clean.get("estado_comercial"):
        clean["estado_comercial"] = "SIN_CONTACTAR"
    # pais default
    if not clean.get("pais"):
        clean["pais"] = "Argentina"
    return clean


# ── Entry point principal ──────────────────────────────────────────────────────

def unify(vertical: str, data_dir: Path | None = None) -> UnifyResult:
    """Merge de fuentes scraper + LinkedIn para un vertical.

    Args:
        vertical: valor del enum Vertical (ej: "parques_industriales")
        data_dir: directorio raíz de datos. Si None, usa settings.data_dir.

    Returns:
        UnifyResult con métricas y path al CSV exportado.
    """
    settings = get_settings()
    if data_dir is None:
        data_dir = Path(settings.data_dir)

    result = UnifyResult(vertical=vertical)

    # ── Carga ──────────────────────────────────────────────────────────────
    scraper_rows = _load_scraper_csvs(vertical, data_dir)
    linkedin_rows = _load_linkedin_csvs(data_dir)

    result.scraper_count  = len(scraper_rows)
    result.linkedin_count = len(linkedin_rows)

    log.info(
        "merge_start",
        vertical=vertical,
        scraper=result.scraper_count,
        linkedin=result.linkedin_count,
    )

    # ── Indexar por _key ───────────────────────────────────────────────────
    scraper_index: dict[str, dict[str, str]] = {}
    for row in scraper_rows:
        key = row["_key"]
        # Si hay duplicados en el scraper, quedarse con el que tenga más datos
        existing = scraper_index.get(key)
        if existing is None or _richness(row) > _richness(existing):
            scraper_index[key] = row

    linkedin_index: dict[str, dict[str, str]] = {}
    for row in linkedin_rows:
        key = row["_key"]
        existing = linkedin_index.get(key)
        if existing is None or _richness(row) > _richness(existing):
            linkedin_index[key] = row

    # ── Merge ──────────────────────────────────────────────────────────────
    all_keys = set(scraper_index) | set(linkedin_index)
    merged_rows: list[dict[str, str]] = []

    for key in all_keys:
        in_scraper  = key in scraper_index
        in_linkedin = key in linkedin_index

        if in_scraper and in_linkedin:
            row = _merge_rows(scraper_index[key], linkedin_index[key])
            result.merged_count += 1
        elif in_scraper:
            row = scraper_index[key]
            result.only_scraper += 1
        else:
            row = linkedin_index[key]
            result.only_linkedin += 1

        merged_rows.append(row)

    # ── Validación ─────────────────────────────────────────────────────────
    clean_rows = [_clean_row(r) for r in merged_rows]
    valid_rows, invalid_rows = validate_batch(clean_rows)

    result.invalid_count       = len(invalid_rows)
    result.total               = len(valid_rows)
    result.with_email          = sum(1 for r in valid_rows if r.get("email"))
    result.with_contact        = sum(1 for r in valid_rows if r.get("contacto_nombre"))
    result.validation_summary  = summarize_validation(valid_rows, invalid_rows)

    # ── Export ─────────────────────────────────────────────────────────────
    output_dir = data_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"unified_{vertical}_{today}.csv"

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(valid_rows)

        result.output_path = output_path
        log.info(
            "merge_done",
            vertical=vertical,
            total=result.total,
            output=str(output_path),
        )
    except Exception as e:
        result.errors.append(str(e))
        log.error("merge_export_error", error=str(e)[:120])

    # Exportar inválidos por separado para revisión
    if invalid_rows:
        invalid_path = output_dir / f"unified_{vertical}_{today}_INVALIDOS.csv"
        try:
            with open(invalid_path, "w", newline="", encoding="utf-8") as f:
                invalid_headers = CSV_HEADERS + ["_issues"]
                writer = csv.DictWriter(f, fieldnames=invalid_headers, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(invalid_rows)
            log.warning(
                "merge_invalidos_exportados",
                count=len(invalid_rows),
                path=str(invalid_path),
            )
        except Exception:
            pass

    return result


# ── Helpers ────────────────────────────────────────────────────────────────────

def _richness(row: dict[str, str]) -> int:
    """Cuenta campos no vacíos — úsalo para desempate entre duplicados."""
    return sum(1 for v in row.values() if v and not v.startswith("_"))
