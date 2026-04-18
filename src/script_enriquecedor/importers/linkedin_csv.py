"""Importador de CSV nativo de LinkedIn Sales Navigator.

Flujo correcto (sin riesgo de baneo):
  1. Operador busca y filtra en Sales Navigator
  2. Guarda la lista en Sales Navigator
  3. Exporta CSV con el botón nativo de LinkedIn
  4. Copia el archivo en data/input/linkedin/
  5. Este módulo lo lee y normaliza al schema Lead

Sales Navigator exporta con columnas propias. Este módulo las mapea al
schema base y maneja las variantes de columna entre versiones del export.

Columnas esperadas (Sales Navigator):
  First Name, Last Name, Title, Company,
  LinkedIn Profile URL, Email, Phone, Website, Geography

Uso:
    importer = get_linkedin_importer()
    leads = importer.load_all()
    # → lista de Lead con contacto_nombre, contacto_cargo, fuente_descubrimiento
"""

import csv
import re
import unicodedata
from pathlib import Path

from ..core.config import get_settings
from ..core.logger import get_logger
from ..core.models import EstadoComercial, Lead, Vertical

log = get_logger("linkedin_csv")

# Columnas del export nativo de Sales Navigator (múltiples aliases por versión)
_FIELD_ALIASES: dict[str, list[str]] = {
    "first_name":    ["First Name", "firstName", "first_name", "Nombre"],
    "last_name":     ["Last Name",  "lastName",  "last_name",  "Apellido"],
    "title":         ["Title", "Job Title", "title", "jobTitle", "Cargo"],
    "company":       ["Company", "Company Name", "company", "Organization", "Empresa"],
    "linkedin_url":  ["LinkedIn Profile URL", "LinkedIn URL", "Profile URL", "profileUrl", "linkedInUrl"],
    "email":         ["Email", "Email Address", "email", "Email 1"],
    "phone":         ["Phone", "Phone Number", "phone", "Teléfono", "Telefono"],
    "website":       ["Website", "Company Website", "website", "URL", "sitio_web"],
    "geography":     ["Geography", "Location", "geography", "Ubicación", "Country"],
}


def _resolve_columns(header: list[str]) -> dict[str, str | None]:
    """Mapea nombres canónicos → nombre real en el header del CSV."""
    result: dict[str, str | None] = {}
    for canonical, aliases in _FIELD_ALIASES.items():
        found = next((a for a in aliases if a in header), None)
        result[canonical] = found
    return result


def _get(row: dict[str, str], col_map: dict[str, str | None], key: str) -> str:
    mapped = col_map.get(key)
    return row.get(mapped, "").strip() if mapped else ""


def _parse_geography(geo: str) -> tuple[str | None, str | None]:
    """Intenta extraer provincia/país de la columna Geography.

    Sales Navigator devuelve strings como:
      "Buenos Aires, Argentina"
      "Córdoba, Argentina"
      "Argentina"
    """
    if not geo:
        return None, None
    parts = [p.strip() for p in geo.split(",")]
    if len(parts) >= 2:
        return parts[0], parts[-1]  # (provincia, país)
    return None, parts[0] if parts else None


def _normalize_url(url: str) -> str | None:
    """Agrega https:// si falta el esquema."""
    if not url:
        return None
    url = url.strip()
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url or None


def _row_to_lead(row: dict[str, str], col_map: dict[str, str | None]) -> Lead | None:
    company = _get(row, col_map, "company")
    if not company:
        return None

    first = _get(row, col_map, "first_name")
    last  = _get(row, col_map, "last_name")
    full_name = f"{first} {last}".strip() or None

    title    = _get(row, col_map, "title") or None
    email    = _get(row, col_map, "email") or None
    phone    = _get(row, col_map, "phone") or None
    website  = _normalize_url(_get(row, col_map, "website"))
    ln_url   = _get(row, col_map, "linkedin_url") or None
    geo      = _get(row, col_map, "geography")

    provincia, _ = _parse_geography(geo)

    fuente = f"linkedin:{ln_url}" if ln_url else "linkedin_csv"

    return Lead(
        nombre=company,
        vertical=Vertical.EMPRESAS,
        estado_comercial=EstadoComercial.SIN_CONTACTAR,
        contacto_nombre=full_name,
        contacto_cargo=title,
        email=email,  # type: ignore[arg-type]
        telefono=phone,
        sitio_web=website,  # type: ignore[arg-type]
        provincia=provincia,
        fuente_descubrimiento=fuente,
        metadata={"linkedin_url": ln_url} if ln_url else {},
    )


class LinkedInCSVImporter:
    """Lee CSVs de Sales Navigator desde el directorio configurado."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._input_dir = Path(self._settings.linkedin_input_dir)

    def _list_csv_files(self) -> list[Path]:
        if not self._input_dir.exists():
            log.warning("linkedin_dir_missing", path=str(self._input_dir))
            return []
        files = sorted(self._input_dir.glob("*.csv"))
        log.debug("linkedin_files_found", count=len(files))
        return files

    def load_file(self, path: Path) -> list[Lead]:
        leads: list[Lead] = []
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    log.warning("linkedin_empty_file", file=path.name)
                    return []

                header = list(reader.fieldnames)
                col_map = _resolve_columns(header)

                if not col_map.get("company"):
                    log.warning(
                        "linkedin_missing_company_col",
                        file=path.name,
                        header=header[:8],
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
                "linkedin_file_loaded",
                file=path.name,
                loaded=len(leads),
                skipped=skipped,
            )
        except Exception as e:
            log.error("linkedin_read_error", file=path.name, error=str(e)[:120])

        return leads

    def load_all(self) -> list[Lead]:
        """Carga todos los CSVs del directorio y retorna lista combinada."""
        all_leads: list[Lead] = []
        for csv_file in self._list_csv_files():
            all_leads.extend(self.load_file(csv_file))
        log.info("linkedin_total_loaded", total=len(all_leads))
        return all_leads


# ── Singleton ──────────────────────────────────────────────────────────────────

_importer: LinkedInCSVImporter | None = None


def get_linkedin_importer() -> LinkedInCSVImporter:
    global _importer
    if _importer is None:
        _importer = LinkedInCSVImporter()
    return _importer
