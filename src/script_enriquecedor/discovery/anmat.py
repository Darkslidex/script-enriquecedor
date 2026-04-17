"""Descubrimiento de Droguerías via registro oficial ANMAT.

Fuente: anmat.gov.ar/webanmat/RegistroFederal/PM_BuscarEmpresa.aspx
Consulta con httpx + BeautifulSoup.
"""

from __future__ import annotations

import httpx
import structlog
from bs4 import BeautifulSoup

from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

_ANMAT_BASE = "https://www.anmat.gov.ar"
_ANMAT_SEARCH = (
    f"{_ANMAT_BASE}/webanmat/RegistroFederal/PM_BuscarEmpresa.aspx"
)
_TIMEOUT = 20
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "es-AR,es;q=0.9",
}

# Rubros ANMAT que corresponden a droguerías
_DROGUERIAS_TIPOS = ["DROGUERIA", "DROGUER", "DISTRIBUIDORA FARMACEUT"]


class ANMATDiscovery(DiscoveryStrategy):
    """Descubre Droguerías desde el registro oficial ANMAT."""

    @property
    def vertical(self) -> Vertical:
        return Vertical.DROGUERIAS

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Consulta el registro ANMAT y retorna droguerías."""
        leads: list[DiscoveredLead] = []

        # La búsqueda ANMAT requiere POST con nombre o CUIT
        # Intentamos búsqueda por tipo "DROGUERIA" con nombre vacío
        try:
            async with httpx.AsyncClient(
                timeout=_TIMEOUT,
                follow_redirects=True,
                headers=_HEADERS,
            ) as client:
                # Primero GET para obtener ViewState (ASP.NET)
                r = await client.get(_ANMAT_SEARCH)
                r.raise_for_status()
                viewstate = _extract_viewstate(r.text)

                # POST búsqueda
                payload = {
                    "__VIEWSTATE": viewstate,
                    "ctl00$ContentPlaceHolder1$txtNombreEmpresa": "",
                    "ctl00$ContentPlaceHolder1$ddlTipo": "DROGUERIA",
                    "ctl00$ContentPlaceHolder1$btnBuscar": "Buscar",
                }
                r2 = await client.post(_ANMAT_SEARCH, data=payload)
                r2.raise_for_status()
                leads = _parse_anmat_results(r2.text, limit)

        except Exception as exc:
            log.error("anmat.fetch_error", error=str(exc)[:80])
            # Fallback: DorksDiscovery para droguerías
            log.info("anmat.fallback_dorks")
            leads = await _dorks_fallback(limit)

        log.info("anmat.done", found=len(leads))
        return leads[:limit]


def _extract_viewstate(html: str) -> str:
    """Extrae el __VIEWSTATE de ASP.NET."""
    soup = BeautifulSoup(html, "html.parser")
    vs = soup.find("input", {"name": "__VIEWSTATE"})
    return vs.get("value", "") if vs else ""


def _parse_anmat_results(html: str, limit: int) -> list[DiscoveredLead]:
    """Parsea la tabla de resultados del ANMAT."""
    soup = BeautifulSoup(html, "html.parser")
    leads = []

    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        nombre = cells[0].get_text(strip=True)
        provincia = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        localidad = cells[1].get_text(strip=True) if len(cells) > 1 else ""

        # Filtrar solo droguerías
        tipo = cells[-1].get_text(strip=True).upper() if cells else ""
        is_drogueria = any(t in tipo for t in _DROGUERIAS_TIPOS) or any(
            t in nombre.upper() for t in _DROGUERIAS_TIPOS
        )
        if not is_drogueria and cells:
            # Aceptar cualquier fila de la tabla de resultados (ya filtrado por POST)
            pass

        if nombre and len(nombre) > 3:
            leads.append(DiscoveredLead(
                nombre=nombre,
                localidad=localidad or None,
                provincia=provincia or None,
                fuente="anmat",
            ))
        if len(leads) >= limit:
            break

    return leads


async def _dorks_fallback(limit: int) -> list[DiscoveredLead]:
    """Fallback con Google Dorks si ANMAT no está disponible."""
    from .dorks import make_vertical_dorks_discovery
    from ..core.models import Vertical

    dorks = [
        'site:.ar "drogueria" "habilitacion" OR "ANMAT" "contacto"',
        '"drogueria farmaceutica" Argentina "deposito" "contacto"',
        'inurl:drogueria site:.ar "seguridad" OR "administracion"',
    ]
    strategy = make_vertical_dorks_discovery(Vertical.DROGUERIAS, dorks)
    return await strategy.discover(limit=limit)
