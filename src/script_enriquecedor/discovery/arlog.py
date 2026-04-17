"""Descubrimiento de Logísticas via ARLOG + Google Dorks.

Fuente primaria: arlog.org (Asociación Argentina de Logística Empresaria)
Fallback: Google Dorks para empresas de logística en Argentina.
"""

from __future__ import annotations

import httpx
import structlog
from bs4 import BeautifulSoup

from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

_ARLOG_BASE = "https://www.arlog.org"
_ARLOG_SOCIOS = f"{_ARLOG_BASE}/socios"
_ARLOG_EMPRESAS = f"{_ARLOG_BASE}/empresas-asociadas"
_TIMEOUT = 20
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "es-AR,es;q=0.9",
}


class ARLOGDiscovery(DiscoveryStrategy):
    """Descubre empresas logísticas desde ARLOG + Google Dorks."""

    @property
    def vertical(self) -> Vertical:
        return Vertical.LOGISTICAS

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Intenta ARLOG primero, luego Dorks como fallback."""
        leads = await self._fetch_arlog(limit)

        if not leads:
            log.info("arlog.fallback_dorks")
            leads = await _dorks_fallback(limit)

        # Complementar con dorks si hay pocos resultados
        if len(leads) < limit // 2:
            extra = await _dorks_fallback(limit - len(leads))
            # Dedup por sitio_web
            existing_urls = {l.sitio_web for l in leads if l.sitio_web}
            for lead in extra:
                if lead.sitio_web not in existing_urls:
                    leads.append(lead)

        log.info("arlog.done", found=len(leads))
        return leads[:limit]

    async def _fetch_arlog(self, limit: int) -> list[DiscoveredLead]:
        """Scrapea el listado de socios/empresas de ARLOG."""
        for url in [_ARLOG_SOCIOS, _ARLOG_EMPRESAS]:
            try:
                async with httpx.AsyncClient(
                    timeout=_TIMEOUT,
                    follow_redirects=True,
                    headers=_HEADERS,
                ) as client:
                    r = await client.get(url)
                    if r.status_code == 200:
                        leads = _parse_arlog_page(r.text, limit)
                        if leads:
                            return leads
            except Exception as exc:
                log.warning("arlog.fetch_error", url=url, error=str(exc)[:80])

        return []


def _parse_arlog_page(html: str, limit: int) -> list[DiscoveredLead]:
    """Parsea la página de socios de ARLOG."""
    soup = BeautifulSoup(html, "html.parser")
    leads = []

    # Estrategia 1: divs con nombre de empresa y link
    for item in soup.select(".empresa, .socio, .member, article, .card"):
        nombre = ""
        url = ""

        h = item.find(["h2", "h3", "h4", "strong", "b"])
        if h:
            nombre = h.get_text(strip=True)
        a = item.find("a", href=True)
        if a:
            href = a.get("href", "")
            if href.startswith("http"):
                url = href
            elif nombre and not nombre:
                nombre = a.get_text(strip=True)

        if nombre and len(nombre) > 3:
            leads.append(DiscoveredLead(
                nombre=nombre,
                sitio_web=url or None,
                fuente="arlog",
            ))
        if len(leads) >= limit:
            break

    # Estrategia 2: tabla
    if not leads:
        for row in soup.select("table tr"):
            cells = row.find_all("td")
            if not cells:
                continue
            nombre = cells[0].get_text(strip=True)
            link = row.find("a", href=True)
            url = link.get("href", "") if link else ""
            if nombre and len(nombre) > 3 and nombre.lower() != "empresa":
                leads.append(DiscoveredLead(
                    nombre=nombre,
                    sitio_web=url if url.startswith("http") else None,
                    fuente="arlog",
                ))
            if len(leads) >= limit:
                break

    return leads


async def _dorks_fallback(limit: int) -> list[DiscoveredLead]:
    """Google Dorks para empresas logísticas en Argentina."""
    from .dorks import make_vertical_dorks_discovery

    dorks = [
        'site:.ar "empresa de logistica" "seguridad" "contacto" Argentina',
        '"operador logistico" Argentina "deposito" "seguridad" site:.ar',
        '"logistica" "almacenamiento" "distribucion" Argentina site:.ar "contacto"',
        'inurl:logistica site:.ar "gerencia" OR "administracion" Argentina',
        '"transporte" "logistica" Argentina CCTV OR "camara de seguridad" "contacto"',
    ]
    strategy = make_vertical_dorks_discovery(Vertical.LOGISTICAS, dorks)
    return await strategy.discover(limit=limit)
