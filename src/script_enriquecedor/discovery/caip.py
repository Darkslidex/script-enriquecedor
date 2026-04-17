"""Descubrimiento de Parques Industriales via CAIP (caip.org.ar).

Fuente oficial: caip.org.ar/parques
Parsea el listado de parques con httpx + BeautifulSoup.
"""

from __future__ import annotations

import httpx
import structlog
from bs4 import BeautifulSoup

from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

_CAIP_URL = "https://www.caip.org.ar/parques"
_TIMEOUT = 20


class CAIPDiscovery(DiscoveryStrategy):
    """Descubre Parques Industriales desde el listado oficial de CAIP."""

    @property
    def vertical(self) -> Vertical:
        return Vertical.PARQUES_INDUSTRIALES

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Scrapea caip.org.ar/parques y retorna listado de parques."""
        leads: list[DiscoveredLead] = []

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
                r = await client.get(_CAIP_URL)
                r.raise_for_status()
        except Exception as exc:
            log.error("caip.fetch_error", url=_CAIP_URL, error=str(exc)[:80])
            return leads

        soup = BeautifulSoup(r.text, "html.parser")

        # CAIP lista parques en tabla o lista — parsear texto de celdas/items
        # Intentar distintos selectores en orden de probabilidad
        candidates: list[tuple[str, str, str]] = []  # (nombre, url, provincia)

        # Estrategia 1: tabla con filas de parques
        for row in soup.select("table tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                nombre = cells[0].get_text(strip=True)
                provincia = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                link = row.find("a")
                url = link.get("href", "") if link else ""
                if nombre and len(nombre) > 3:
                    candidates.append((nombre, _normalize_url(url), provincia))

        # Estrategia 2: lista de links (si tabla vacía)
        if not candidates:
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                nombre = a.get_text(strip=True)
                if nombre and len(nombre) > 5 and "parque" in nombre.lower():
                    candidates.append((nombre, _normalize_url(href), ""))

        for nombre, url, provincia in candidates[:limit]:
            leads.append(DiscoveredLead(
                nombre=nombre,
                sitio_web=url or None,
                provincia=provincia or "Buenos Aires",
                fuente="caip",
            ))

        log.info("caip.done", found=len(leads))
        return leads[:limit]


def _normalize_url(url: str) -> str:
    """Convierte URLs relativas de CAIP a absolutas."""
    if not url:
        return ""
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://www.caip.org.ar{url}"
    return ""
