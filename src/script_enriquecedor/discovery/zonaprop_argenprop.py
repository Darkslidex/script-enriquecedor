"""Descubrimiento de Barrios Privados via Zonaprop y Argenprop.

Estrategia en dos etapas:
  1. Scraping de listings en Zonaprop/Argenprop → extrae nombre, partido, zona
  2. Búsqueda del sitio web oficial de cada barrio (DDG → googlesearch fallback)

Porta y moderniza la lógica de v1 (pipeline_integrado_ia.py):
  - DDG HTML scraping → duckduckgo_search library
  - Bing fallback → googlesearch-python
  - curl_cffi Chrome120 → httpx + headers realistas
  - Subpáginas de contacto (misma lista que v1)
"""

import asyncio
import random
import re
import time
from urllib.parse import urlparse, quote

import httpx
from bs4 import BeautifulSoup

from ..core.config import get_settings
from ..core.logger import get_logger
from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = get_logger("discovery.zonaprop")

# ── Constantes (portadas de v1) ────────────────────────────────────────────────

# Dominios a ignorar en resultados de búsqueda
BANNED_DOMAINS: set[str] = {
    "facebook.com", "instagram.com", "zonaprop.com.ar", "argenprop.com",
    "mercadolibre.com.ar", "twitter.com", "x.com", "linkedin.com",
    "tiktok.com", "youtube.com", "pinterest.com", "google.com",
    "properati.com.ar", "remax.com.ar", "mudafy.com.ar", "foursquare.com",
    "tripadvisor.com", "navent.com", "infocasas.com", "lamudi.com",
    "bnpropiedades.com", "sidomus.com", "roomix.ai", "urbannext.net",
    "todosnegocios.com", "eldia.com", "infobae.com", "clarin.com",
    "lanacion.com.ar", "wikipedia.org", "wikidata.org", "yellowpages",
    "grupobaigun.com", "realestate.com",
}

# Subpáginas de contacto/administración a probar (orden de prioridad)
CONTACT_SUBPAGES: list[str] = [
    "/contacto", "/administracion", "/contactenos", "/contact",
    "/contacto/", "/administracion/", "/quienes-somos",
    "/la-administracion", "/consorcio", "/propietarios",
    "/contacto.html", "/administracion.html",
]

# Headers realistas para no ser bloqueados (httpx, sin curl_cffi)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Queries de búsqueda progresivas (de más específica a más general)
_SEARCH_QUERIES = [
    '"{nombre}" administracion contacto site:com.ar',
    '"{nombre}" {partido} administracion email contacto',
    '"{nombre}" barrio privado intendencia email Argentina',
    '"{nombre}" barrio cerrado contacto',
]

# URLs base de Zonaprop/Argenprop para listings de barrios
_ZONAPROP_URLS = [
    "https://www.zonaprop.com.ar/barrios-privados-venta-pagina-{page}.html",
    "https://www.zonaprop.com.ar/countries-venta-pagina-{page}.html",
]
_ARGENPROP_URLS = [
    "https://www.argenprop.com/barrio-privado?pagina={page}",
]


# ── Búsqueda del sitio web oficial ────────────────────────────────────────────

def _is_valid_url(url: str) -> bool:
    """True si la URL no pertenece a un dominio baneado."""
    try:
        domain = urlparse(url).netloc.lower()
        return not any(banned in domain for banned in BANNED_DOMAINS)
    except Exception:
        return False


def _prefer_ar(urls: list[str]) -> list[str]:
    """Prioriza dominios .com.ar y .org.ar sobre otros."""
    ar = [u for u in urls if ".com.ar" in u or ".org.ar" in u]
    rest = [u for u in urls if u not in ar]
    return ar + rest


async def _search_ddg(query: str) -> list[str]:
    """Busca en DuckDuckGo con duckduckgo_search library.

    Retorna lista de URLs (ya filtradas por BANNED_DOMAINS).
    Retorna [] si el módulo no está disponible o hay error.
    """
    try:
        from duckduckgo_search import DDGS
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, region="ar-es", max_results=10):
                url = r.get("href", "")
                if url and _is_valid_url(url):
                    results.append(url)
        return results
    except Exception as e:
        log.warning("ddg_search_error", query=query[:60], error=str(e)[:80])
        return []


async def _search_google(query: str) -> list[str]:
    """Busca en Google con googlesearch-python.

    Retorna [] si el módulo no está disponible o hay error/bloqueo.
    """
    try:
        from googlesearch import search
        results = []
        for url in search(query, num_results=10, lang="es", sleep_interval=random.uniform(5, 10)):
            if _is_valid_url(url):
                results.append(url)
        return results
    except Exception as e:
        log.warning("google_search_error", query=query[:60], error=str(e)[:80])
        return []


async def search_official_website(nombre: str, partido: str = "") -> str:
    """Busca el sitio web oficial de un barrio usando DDG → Google fallback.

    Lógica portada de v1 (buscar_sitio_web) con:
    - duckduckgo_search en vez de scraping HTML de DDG
    - googlesearch-python en vez de Bing scraping

    Returns:
        URL del sitio oficial o "" si no se encontró.
    """
    settings = get_settings()

    for query_tpl in _SEARCH_QUERIES:
        query = query_tpl.format(nombre=nombre, partido=partido)

        # Capa 1: DDG
        results = await _search_ddg(query)

        # Capa 2: Google si DDG no dio nada
        if not results:
            await asyncio.sleep(random.uniform(2, 4))
            results = await _search_google(query)

        if results:
            prioritized = _prefer_ar(results)
            log.info("website_found", nombre=nombre, url=prioritized[0])
            return prioritized[0]

        await asyncio.sleep(settings.rate_limit_seconds * 0.5)

    log.warning("website_not_found", nombre=nombre, partido=partido)
    return ""


# ── Scraping de contenido web ──────────────────────────────────────────────────
# Estas funciones se usan aquí para obtener texto de listing pages.
# El fetcher completo (con Playwright fallback) está en scraping/fetcher.py (paso 7).

async def _fetch_html(url: str, client: httpx.AsyncClient) -> str:
    """Descarga HTML de una URL con httpx. Retorna '' si falla."""
    try:
        r = await client.get(url, headers=_HEADERS, follow_redirects=True, timeout=15)
        if r.status_code == 200:
            return r.text
        log.warning("fetch_failed", url=url, status=r.status_code)
        return ""
    except Exception as e:
        log.warning("fetch_error", url=url, error=str(e)[:80])
        return ""


def _clean_text(html: str) -> str:
    """Extrae texto limpio del HTML (sin scripts, estilos, nav, footer)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


# ── Scraping de Zonaprop ───────────────────────────────────────────────────────

def _parse_zonaprop_page(html: str) -> list[DiscoveredLead]:
    """Extrae listados de barrios de una página de Zonaprop.

    Zonaprop renderiza con React/SSR. Los datos de cada card están en
    atributos `data-id`, clases de artículo y texto visible.
    """
    leads: list[DiscoveredLead] = []
    soup = BeautifulSoup(html, "html.parser")

    # Cards de propiedad: selector CSS generalizado
    # El nombre del barrio suele estar en el título de la card
    for card in soup.select("article[data-id], div.postingCard, div[class*='posting']"):
        try:
            # Nombre: h2, h3, o elemento con clase que contiene "title"
            title_el = (
                card.select_one("h2, h3")
                or card.select_one("[class*='title']")
                or card.select_one("[class*='name']")
            )
            if not title_el:
                continue
            nombre = title_el.get_text(strip=True)
            if not nombre or len(nombre) < 3:
                continue

            # Partido / localidad
            location_el = card.select_one(
                "[class*='location'], [class*='address'], [class*='partido']"
            )
            location_text = location_el.get_text(strip=True) if location_el else ""

            # Intentar extraer partido de "Barrio, Partido, Buenos Aires"
            partido, provincia = _parse_location(location_text)

            leads.append(
                DiscoveredLead(
                    nombre=nombre,
                    partido=partido,
                    provincia=provincia,
                    fuente="zonaprop",
                    raw_data={"location_raw": location_text},
                )
            )
        except Exception:
            continue

    return leads


def _parse_argenprop_page(html: str) -> list[DiscoveredLead]:
    """Extrae listados de barrios de una página de Argenprop."""
    leads: list[DiscoveredLead] = []
    soup = BeautifulSoup(html, "html.parser")

    for card in soup.select("article, div[class*='card'], div[class*='listing']"):
        try:
            title_el = card.select_one("h2, h3, [class*='title'], [class*='name']")
            if not title_el:
                continue
            nombre = title_el.get_text(strip=True)
            if not nombre or len(nombre) < 3:
                continue

            location_el = card.select_one(
                "[class*='location'], [class*='address'], [class*='zone']"
            )
            location_text = location_el.get_text(strip=True) if location_el else ""
            partido, provincia = _parse_location(location_text)

            leads.append(
                DiscoveredLead(
                    nombre=nombre,
                    partido=partido,
                    provincia=provincia,
                    fuente="argenprop",
                    raw_data={"location_raw": location_text},
                )
            )
        except Exception:
            continue

    return leads


def _parse_location(text: str) -> tuple[str | None, str | None]:
    """Extrae partido y provincia de un string de ubicación.

    Ejemplos:
        "Tigre, Buenos Aires"           → ("Tigre", "Buenos Aires")
        "Nordelta, Tigre, Buenos Aires" → ("Tigre", "Buenos Aires")
        "Pilar, GBA"                    → ("Pilar", None)
    """
    PROVINCIAS = {
        "Buenos Aires", "Córdoba", "Santa Fe", "Mendoza",
        "Tucumán", "Salta", "Misiones", "Entre Ríos",
    }
    parts = [p.strip() for p in re.split(r"[,·|]", text) if p.strip()]
    provincia = None
    partido = None

    for i, part in enumerate(parts):
        if part in PROVINCIAS:
            provincia = part
            if i > 0:
                partido = parts[i - 1]
            break

    if partido is None and parts:
        partido = parts[-1] if len(parts) == 1 else parts[0]

    return partido or None, provincia or None


# ── Deduplicación simple ───────────────────────────────────────────────────────

def _dedup(leads: list[DiscoveredLead]) -> list[DiscoveredLead]:
    """Elimina duplicados por nombre normalizado."""
    seen: set[str] = set()
    result = []
    for lead in leads:
        key = re.sub(r"\s+", " ", lead.nombre.lower().strip())
        if key not in seen:
            seen.add(key)
            result.append(lead)
    return result


# ── Discovery Strategy ────────────────────────────────────────────────────────

class ZonapropArgenpropDiscovery(DiscoveryStrategy):
    """Descubre barrios privados scrapeando Zonaprop y Argenprop.

    Etapa 1: Scraping de páginas de listings → obtiene nombre + partido.
    Etapa 2 (opcional): Para cada barrio sin URL, busca el sitio oficial.
              Esta etapa es costosa (1 búsqueda DDG/Google por barrio).
              Se puede omitir si solo se quiere el listado de nombres.
    """

    @property
    def vertical(self) -> Vertical:
        return Vertical.BARRIOS_PRIVADOS

    async def discover(
        self,
        limit: int = 100,
        search_websites: bool = False,
        max_pages: int = 5,
    ) -> list[DiscoveredLead]:
        """Descubre barrios de Zonaprop + Argenprop.

        Args:
            limit:           Máximo de barrios a retornar.
            search_websites: Si True, busca la URL oficial de cada barrio
                             (más lento, ~5-10s por barrio).
            max_pages:       Páginas de listing a scrapear por portal.

        Returns:
            Lista de DiscoveredLead, deduplicados por nombre.
        """
        settings = get_settings()
        all_leads: list[DiscoveredLead] = []

        async with httpx.AsyncClient(headers=_HEADERS, timeout=15) as client:
            # Zonaprop
            zonaprop_leads = await self._scrape_portal(
                client=client,
                url_template=_ZONAPROP_URLS[0],
                parser=_parse_zonaprop_page,
                max_pages=max_pages,
                rate=settings.rate_limit_seconds,
            )
            all_leads.extend(zonaprop_leads)
            log.info("zonaprop_scraped", count=len(zonaprop_leads))

            if len(all_leads) < limit:
                argenprop_leads = await self._scrape_portal(
                    client=client,
                    url_template=_ARGENPROP_URLS[0],
                    parser=_parse_argenprop_page,
                    max_pages=max_pages,
                    rate=settings.rate_limit_seconds,
                )
                all_leads.extend(argenprop_leads)
                log.info("argenprop_scraped", count=len(argenprop_leads))

        unique = _dedup(all_leads)[:limit]

        # Buscar URLs oficiales si se solicitó
        if search_websites:
            for lead in unique:
                if not lead.sitio_web:
                    lead.sitio_web = await search_official_website(
                        lead.nombre, lead.partido or ""
                    )
                    await asyncio.sleep(random.uniform(3, 6))

        log.info(
            "discovery_complete",
            total=len(unique),
            with_website=sum(1 for l in unique if l.sitio_web),
        )
        return unique

    async def _scrape_portal(
        self,
        client: httpx.AsyncClient,
        url_template: str,
        parser,
        max_pages: int,
        rate: float,
    ) -> list[DiscoveredLead]:
        """Scrapea múltiples páginas de un portal inmobiliario."""
        leads: list[DiscoveredLead] = []
        empty_pages = 0

        for page in range(1, max_pages + 1):
            url = url_template.format(page=page)
            html = await _fetch_html(url, client)

            if not html:
                empty_pages += 1
                if empty_pages >= 2:
                    # Si 2 páginas consecutivas fallan, probablemente llegamos al final
                    break
                continue

            page_leads = parser(html)
            if not page_leads:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0
                leads.extend(page_leads)

            await asyncio.sleep(rate)

        return leads
