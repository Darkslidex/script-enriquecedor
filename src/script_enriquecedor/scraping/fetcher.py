"""Fetcher HTTP async con fallback automático a Playwright.

Flujo por URL:
  1. Verificar robots.txt → skip si bloqueado
  2. Esperar rate limiter del dominio
  3. Intentar httpx con UA rotation
  4. Si respuesta es 403 / contenido vacío / JS-heavy → reintentar con Playwright
  5. Extraer texto limpio (sin scripts, estilos, nav, footer)
  6. Opcionalmente: intentar subpáginas de contacto (/contacto, /administracion, etc.)

Detección de "JS-heavy" (gatilla Playwright):
  - status 403 / 429
  - Texto extraído < MIN_TEXT_CHARS
  - HTML contiene markers de SPA vacía (noscript, __NEXT_DATA__, reactroot)

Uso:
    fetcher = get_fetcher()
    result = await fetcher.fetch("https://nordelta.com.ar")
    # result.text → texto combinado (home + subpágina de contacto si la encontró)
    # result.used_playwright → True si hubo fallback
"""

import asyncio
import re
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from ..core.config import get_settings
from ..core.logger import get_logger
from ..discovery.zonaprop_argenprop import CONTACT_SUBPAGES
from .rate_limiter import get_rate_limiter
from .robots import get_robots_checker
from .user_agents import get_desktop_ua, get_random_ua

log = get_logger("fetcher")

# Texto mínimo extraído para considerar que la página tiene contenido real
MIN_TEXT_CHARS = 300

# Tamaño máximo de texto a pasar al LLM (portado de v1: 12000)
MAX_TEXT_CHARS = 12_000

# Markers que indican que la página es una SPA vacía (necesita Playwright)
_SPA_MARKERS = [
    "__NEXT_DATA__",
    'id="__NEXT_DATA__"',
    "window.__NUXT__",
    '<div id="app"></div>',
    '<div id="root"></div>',
    "ReactDOM.render",
    "ng-version=",
]

# Status codes que gatillan fallback a Playwright
_PLAYWRIGHT_TRIGGER_STATUSES = {403, 429, 503}


@dataclass
class FetchResult:
    """Resultado de un fetch: texto limpio + metadata."""

    url: str
    text: str = ""
    status_code: int = 0
    used_playwright: bool = False
    subpage_used: str | None = None  # subpágina de contacto que tuvo contenido
    error: str | None = None

    @property
    def has_content(self) -> bool:
        return len(self.text) >= MIN_TEXT_CHARS

    @property
    def truncated_text(self) -> str:
        """Texto truncado a MAX_TEXT_CHARS para el LLM."""
        return self.text[:MAX_TEXT_CHARS]


# ── Extracción de texto ────────────────────────────────────────────────────────

def _extract_text(html: str) -> str:
    """Extrae texto limpio del HTML (porta lógica de v1 _descargar_pagina)."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def _is_spa_empty(html: str, text: str) -> bool:
    """True si la página parece una SPA vacía que necesita Playwright."""
    if len(text) < MIN_TEXT_CHARS:
        return True
    return any(marker in html for marker in _SPA_MARKERS)


# ── Playwright fallback ────────────────────────────────────────────────────────

async def _fetch_with_playwright(url: str) -> tuple[str, int]:
    """Descarga la página usando Playwright (Chromium headless).

    Returns:
        (html_content, status_code) o ("", 0) si falla.
    """
    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=get_desktop_ua(),
                locale="es-AR",
                viewport={"width": 1280, "height": 800},
                # Bloquear recursos pesados que no son necesarios para texto
                java_script_enabled=True,
            )
            page = await context.new_page()

            # Bloquear imágenes, fuentes y otros recursos no esenciales
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,ico,woff,woff2,ttf,eot}",
                lambda route: route.abort(),
            )

            response = await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            status = response.status if response else 0

            # Esperar un poco para JS básico
            await asyncio.sleep(2)
            html = await page.content()

            await browser.close()
            return html, status

    except Exception as e:
        log.warning("playwright_error", url=url, error=str(e)[:120])
        return "", 0


# ── Fetcher principal ──────────────────────────────────────────────────────────

class Fetcher:
    """Fetcher HTTP async con fallback Playwright, rate limiting y robots.txt."""

    def __init__(self) -> None:
        self._rate_limiter = get_rate_limiter()
        self._robots = get_robots_checker()

    async def fetch(
        self,
        url: str,
        try_contact_subpages: bool = True,
        force_playwright: bool = False,
    ) -> FetchResult:
        """Descarga una URL y retorna texto limpio.

        Args:
            url:                  URL a descargar.
            try_contact_subpages: Si True, intenta subpáginas de contacto
                                  cuando la página principal no tiene emails.
            force_playwright:     Si True, usa Playwright directamente.

        Returns:
            FetchResult con el texto combinado de home + subpágina de contacto.
        """
        if not url or not url.startswith("http"):
            return FetchResult(url=url, error="URL inválida")

        # 1. Verificar robots.txt
        if not await self._robots.can_fetch(url):
            return FetchResult(url=url, error="bloqueado por robots.txt")

        # 2. Rate limit
        await self._rate_limiter.acquire(url)

        # 3. Fetch principal
        main_text, status, used_playwright = await self._fetch_url(url, force_playwright)

        result = FetchResult(
            url=url,
            text=main_text,
            status_code=status,
            used_playwright=used_playwright,
        )

        if not result.has_content and not try_contact_subpages:
            result.error = "página sin contenido suficiente"
            return result

        # 4. Intentar subpáginas de contacto
        if try_contact_subpages:
            sub_text, subpage = await self._try_contact_subpages(url, used_playwright)
            if sub_text:
                # Combinar: home + subpágina (máximo MAX_TEXT_CHARS total)
                combined = f"{main_text} | {sub_text}"
                result.text = combined[:MAX_TEXT_CHARS]
                result.subpage_used = subpage
                log.debug(
                    "subpage_found",
                    url=url,
                    subpage=subpage,
                    total_chars=len(result.text),
                )

        if not result.has_content:
            result.error = "sin contenido útil en página principal ni subpáginas"

        return result

    async def _fetch_url(
        self,
        url: str,
        force_playwright: bool = False,
    ) -> tuple[str, int, bool]:
        """Fetch con httpx → Playwright fallback si es necesario.

        Returns:
            (text, status_code, used_playwright)
        """
        if not force_playwright:
            # Intento 1: httpx
            html, status = await self._fetch_httpx(url)

            if status not in _PLAYWRIGHT_TRIGGER_STATUSES and html:
                text = _extract_text(html)
                if not _is_spa_empty(html, text):
                    log.debug("fetch_ok_httpx", url=url, chars=len(text))
                    return text, status, False

            # Si el status es OK pero el contenido parece SPA vacía
            if status not in _PLAYWRIGHT_TRIGGER_STATUSES and html:
                log.info("spa_detected_using_playwright", url=url, status=status)
            elif status in _PLAYWRIGHT_TRIGGER_STATUSES:
                log.info("status_trigger_playwright", url=url, status=status)

        # Intento 2: Playwright
        html, status = await _fetch_with_playwright(url)
        if html:
            text = _extract_text(html)
            return text, status, True

        return "", status, False

    async def _fetch_httpx(self, url: str) -> tuple[str, int]:
        """Descarga HTML con httpx async.

        Returns:
            (html, status_code) o ("", 0) si falla.
        """
        headers = {
            "User-Agent": get_random_ua(),
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        }
        try:
            async with httpx.AsyncClient(
                headers=headers,
                timeout=15,
                follow_redirects=True,
                verify=False,  # algunos sitios de barrios tienen certs vencidos
            ) as client:
                r = await client.get(url)
                return r.text, r.status_code
        except Exception as e:
            log.warning("httpx_error", url=url, error=str(e)[:80])
            return "", 0

    async def _try_contact_subpages(
        self,
        base_url: str,
        use_playwright: bool,
    ) -> tuple[str, str | None]:
        """Intenta subpáginas de contacto en orden de prioridad.

        Porta la lógica de v1 (obtener_texto_web con subpáginas).

        Returns:
            (texto_subpagina, subpage_path) o ("", None) si ninguna tuvo contenido.
        """
        parsed = urlparse(base_url)
        domain_base = f"{parsed.scheme}://{parsed.netloc}"

        for subpage in CONTACT_SUBPAGES:
            sub_url = domain_base + subpage

            # Evitar re-descargar la misma URL que ya descargamos
            if sub_url.rstrip("/") == base_url.rstrip("/"):
                continue

            # Verificar robots.txt para la subpágina
            if not await self._robots.can_fetch(sub_url):
                continue

            await self._rate_limiter.acquire(sub_url)

            html, status = await self._fetch_httpx(sub_url)

            if status == 404 or not html:
                continue

            text = _extract_text(html)
            if len(text) >= MIN_TEXT_CHARS:
                log.debug("contact_subpage_ok", url=sub_url, chars=len(text))
                return text, subpage

        return "", None

    async def fetch_many(
        self,
        urls: list[str],
        concurrency: int = 3,
        **kwargs,
    ) -> list[FetchResult]:
        """Descarga múltiples URLs con concurrencia controlada.

        Args:
            urls:        Lista de URLs.
            concurrency: Número máximo de requests simultáneos.
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def _fetch_one(url: str) -> FetchResult:
            async with semaphore:
                return await self.fetch(url, **kwargs)

        return await asyncio.gather(*[_fetch_one(u) for u in urls])


# ── Singleton ──────────────────────────────────────────────────────────────────

_fetcher: Fetcher | None = None


def get_fetcher() -> Fetcher:
    """Retorna el singleton de Fetcher."""
    global _fetcher
    if _fetcher is None:
        _fetcher = Fetcher()
    return _fetcher
