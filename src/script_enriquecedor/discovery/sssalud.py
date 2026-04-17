"""Descubrimiento de Clínicas Privadas via SSSALUD.

Fuente: sssalud.gob.ar/index.php?cat=buscar&tipo=clinicas
Parsea el buscador de prestadores de SSSALUD.
"""

from __future__ import annotations

import httpx
import structlog
from bs4 import BeautifulSoup

from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

_SSSALUD_BASE = "https://www.sssalud.gob.ar"
_SSSALUD_SEARCH = f"{_SSSALUD_BASE}/index.php"
_TIMEOUT = 20
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "es-AR,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Parámetros de búsqueda por defecto
_DEFAULT_PARAMS = {
    "cat": "buscar",
    "tipo": "clinicas",
    "provincia": "",  # todas las provincias
    "nombre": "",
}


class SSSALUDDiscovery(DiscoveryStrategy):
    """Descubre Clínicas Privadas desde el buscador SSSALUD."""

    def __init__(self, provincias: list[str] | None = None) -> None:
        """Args:
            provincias: lista de provincias a consultar (None = todas).
        """
        self.provincias = provincias or [""]  # "" = todas

    @property
    def vertical(self) -> Vertical:
        return Vertical.CLINICAS

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Consulta SSSALUD por provincia y retorna clínicas."""
        leads: list[DiscoveredLead] = []

        for provincia in self.provincias:
            if len(leads) >= limit:
                break

            batch = await self._fetch_provincia(provincia, limit - len(leads))
            leads.extend(batch)

        log.info("sssalud.done", found=len(leads))
        return leads[:limit]

    async def _fetch_provincia(
        self, provincia: str, limit: int
    ) -> list[DiscoveredLead]:
        """Consulta clínicas de una provincia."""
        params = {**_DEFAULT_PARAMS, "provincia": provincia}

        try:
            async with httpx.AsyncClient(
                timeout=_TIMEOUT,
                follow_redirects=True,
                headers=_HEADERS,
            ) as client:
                r = await client.get(_SSSALUD_SEARCH, params=params)
                r.raise_for_status()
                return _parse_sssalud_results(r.text, limit)

        except Exception as exc:
            log.error(
                "sssalud.fetch_error",
                provincia=provincia,
                error=str(exc)[:80],
            )
            return await _dorks_fallback(limit)


def _parse_sssalud_results(html: str, limit: int) -> list[DiscoveredLead]:
    """Parsea tabla de resultados de SSSALUD."""
    soup = BeautifulSoup(html, "html.parser")
    leads = []

    for row in soup.select("table.table tr, table tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        nombre = cells[0].get_text(strip=True)
        provincia = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        localidad = cells[2].get_text(strip=True) if len(cells) > 2 else ""

        # Saltar headers
        if nombre.lower() in ("nombre", "prestador", "establecimiento"):
            continue

        if nombre and len(nombre) > 3:
            leads.append(DiscoveredLead(
                nombre=nombre,
                localidad=localidad or None,
                provincia=provincia or None,
                fuente="sssalud",
            ))

        if len(leads) >= limit:
            break

    return leads


async def _dorks_fallback(limit: int) -> list[DiscoveredLead]:
    """Fallback Google Dorks si SSSALUD no está disponible."""
    from .dorks import make_vertical_dorks_discovery
    from ..core.models import Vertical

    dorks = [
        'site:.ar "clinica privada" "seguridad" "contacto" Argentina',
        '"clinica" "sanatorio" Argentina "sistema de seguridad" site:.ar',
        'inurl:clinica site:.ar "SSSALUD" OR "habilitacion" "administracion"',
    ]
    strategy = make_vertical_dorks_discovery(Vertical.CLINICAS, dorks)
    return await strategy.discover(limit=limit)
