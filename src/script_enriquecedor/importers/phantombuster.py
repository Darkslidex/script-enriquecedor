"""Importador de CSVs generados por PhantomBuster LinkedIn Scraper.

PhantomBuster exporta perfiles de LinkedIn en CSV con columnas estándar:
  firstName, lastName, title, companyName, linkedInUrl

Este módulo lee esos archivos desde PHANTOMBUSTER_INPUT_DIR y los normaliza
al modelo Lead, listo para entrar al pipeline waterfall de enriquecimiento
(Hunter → Snov → Apollo → export).

Uso:
    importer = get_phantombuster_importer()
    leads = importer.load_all()
    for lead in leads:
        # lead es un Lead con vertical=EMPRESAS, contacto_nombre, etc.
        pipeline.enrich(lead)
"""

import csv
from pathlib import Path

from ..core.config import get_settings
from ..core.logger import get_logger
from ..core.models import EstadoComercial, Lead, Vertical

log = get_logger("phantombuster")

# Columnas estándar del LinkedIn Scraper de PhantomBuster
_COL_FIRST = "firstName"
_COL_LAST = "lastName"
_COL_TITLE = "title"
_COL_COMPANY = "companyName"
_COL_LINKEDIN = "linkedInUrl"

# Columnas alternativas que PhantomBuster usa en algunas versiones del agente
_COL_ALIASES: dict[str, list[str]] = {
    _COL_FIRST: ["first_name", "First Name", "firstname"],
    _COL_LAST: ["last_name", "Last Name", "lastname"],
    _COL_TITLE: ["job_title", "Title", "jobTitle", "headline"],
    _COL_COMPANY: ["company", "Company", "company_name", "organization"],
    _COL_LINKEDIN: ["linkedin", "LinkedIn", "profile_url", "profileUrl"],
}


def _resolve_col(header: list[str], canonical: str) -> str | None:
    """Busca el nombre real de la columna en el header del CSV."""
    if canonical in header:
        return canonical
    for alias in _COL_ALIASES.get(canonical, []):
        if alias in header:
            return alias
    return None


def _normalize_domain(linkedin_url: str) -> str | None:
    """Extrae el dominio a partir de la URL de LinkedIn (para fuente_contacto)."""
    if not linkedin_url:
        return None
    return "linkedin.com"


def _row_to_lead(row: dict[str, str], col_map: dict[str, str | None]) -> Lead | None:
    """Convierte una fila del CSV al modelo Lead. Retorna None si no hay empresa."""

    def get(col: str) -> str:
        mapped = col_map.get(col)
        return row.get(mapped, "").strip() if mapped else ""

    company = get(_COL_COMPANY)
    if not company:
        return None

    first = get(_COL_FIRST)
    last = get(_COL_LAST)
    title = get(_COL_TITLE)
    linkedin = get(_COL_LINKEDIN)

    full_name = f"{first} {last}".strip() or None

    lead = Lead(
        nombre=company,
        vertical=Vertical.EMPRESAS,
        estado_comercial=EstadoComercial.SIN_CONTACTAR,
        contacto_nombre=full_name,
        contacto_cargo=title or None,
        fuente_descubrimiento="phantombuster_linkedin",
        metadata={
            "linkedin_url": linkedin or None,
            "linkedin_contacto": linkedin or None,
        },
    )
    return lead


class PhantomBusterImporter:
    """Lee CSVs de PhantomBuster desde el directorio configurado."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._input_dir = Path(self._settings.phantombuster_input_dir)

    def _list_csv_files(self) -> list[Path]:
        if not self._input_dir.exists():
            log.warning("phantombuster_dir_missing", path=str(self._input_dir))
            return []
        files = sorted(self._input_dir.glob("*.csv"))
        log.debug("phantombuster_files_found", count=len(files))
        return files

    def load_file(self, path: Path) -> list[Lead]:
        """Carga un CSV de PhantomBuster y retorna lista de Leads."""
        leads: list[Lead] = []

        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    log.warning("phantombuster_empty_file", file=path.name)
                    return []

                header = list(reader.fieldnames)
                col_map = {col: _resolve_col(header, col) for col in _COL_ALIASES}

                if not col_map.get(_COL_COMPANY):
                    log.warning(
                        "phantombuster_missing_company_col",
                        file=path.name,
                        header=header,
                    )
                    return []

                skipped = 0
                for row in reader:
                    lead = _row_to_lead(row, col_map)
                    if lead:
                        leads.append(lead)
                    else:
                        skipped += 1

                log.info(
                    "phantombuster_file_loaded",
                    file=path.name,
                    loaded=len(leads),
                    skipped=skipped,
                )

        except Exception as e:
            log.error("phantombuster_read_error", file=path.name, error=str(e)[:120])

        return leads

    def load_all(self) -> list[Lead]:
        """Carga todos los CSVs del directorio y retorna lista combinada de Leads."""
        all_leads: list[Lead] = []
        for csv_file in self._list_csv_files():
            all_leads.extend(self.load_file(csv_file))

        log.info("phantombuster_total_loaded", total=len(all_leads))
        return all_leads


# ── Singleton ──────────────────────────────────────────────────────────────────

_importer: PhantomBusterImporter | None = None


def get_phantombuster_importer() -> PhantomBusterImporter:
    """Retorna el singleton de PhantomBusterImporter."""
    global _importer
    if _importer is None:
        _importer = PhantomBusterImporter()
    return _importer
