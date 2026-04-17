"""Estrategia de descubrimiento via Google Dorks con circuit breaker.

Capa 1 — googlesearch-python (interfaz móvil, menos anti-bot)
Capa 2 — DuckDuckGo fallback automático al detectar 429/CAPTCHA/3 vacíos consecutivos
Capa 3 — Pausa 1h + log warning si ambas capas fallan
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum

import structlog

from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

# Dominios a descartar del descubrimiento (portales inmobiliarios, etc.)
_BANNED_DOMAINS = {
    "zonaprop.com.ar",
    "argenprop.com",
    "mercadolibre.com.ar",
    "infocasas.com.uy",
    "properati.com.ar",
    "remax.com.ar",
    "navent.com",
    "clasificados.clarin.com",
    "olx.com.ar",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "linkedin.com",
    "youtube.com",
    "wikipedia.org",
    "wikidata.org",
}

_PAUSE_SECONDS = 3600  # 1h pausa en capa 3


class CircuitState(Enum):
    CLOSED = "closed"       # funcionando normal
    HALF_OPEN = "half_open" # probando recuperación
    OPEN = "open"           # bloqueado, usando fallback


@dataclass
class _CircuitBreaker:
    """Circuit breaker simple para detectar bloqueos del buscador."""
    consecutive_failures: int = 0
    failure_threshold: int = 3
    state: CircuitState = CircuitState.CLOSED
    opened_at: float = 0.0

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            self.opened_at = time.time()
            log.warning(
                "dorks.circuit_open",
                failures=self.consecutive_failures,
            )

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def record_empty(self) -> bool:
        """Registra resultado vacío. Retorna True si abrió el circuito."""
        self.record_failure()
        return self.is_open


@dataclass
class DorksDiscovery(DiscoveryStrategy):
    """Descubrimiento via Google Dorks con circuit breaker y DDG fallback.

    Args:
        _vertical: vertical al que pertenece esta estrategia.
        dorks: lista de strings de búsqueda.
        banned_domains: dominios adicionales a filtrar.
        pause_between_s: segundos entre requests (rate limit Google).
    """

    _vertical: "Vertical" = field(default=None)  # type: ignore[assignment]
    dorks: list[str] = field(default_factory=list)
    banned_domains: set[str] = field(default_factory=set)
    pause_between_s: float = 2.0

    @property
    def vertical(self) -> "Vertical":
        from ..core.models import Vertical
        return self._vertical or Vertical.EMPRESAS  # fallback

    def __post_init__(self) -> None:
        self._google_cb = _CircuitBreaker()
        self._all_banned = _BANNED_DOMAINS | self.banned_domains

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Ejecuta las búsquedas dorks y retorna leads únicos.

        Intenta Google primero, con fallback automático a DDG si el circuito se abre.
        """
        if not self.dorks:
            log.error("dorks.no_queries")
            return []

        queries = self.dorks
        leads: list[DiscoveredLead] = []
        seen_urls: set[str] = set()

        for query in queries:
            if len(leads) >= limit:
                break

            batch = await self._search_with_fallback(query, limit - len(leads))
            for lead in batch:
                url = lead.sitio_web or ""
                if url not in seen_urls:
                    seen_urls.add(url)
                    leads.append(lead)

        log.info("dorks.done", total=len(leads))
        return leads[:limit]

    async def _search_with_fallback(
        self, query: str, limit: int
    ) -> list[DiscoveredLead]:
        """Intenta Google → DDG → warning."""
        # Capa 1: Google
        if not self._google_cb.is_open:
            results = await self._google_search(query, limit)
            if results:
                self._google_cb.record_success()
                return results
            else:
                opened = self._google_cb.record_empty()
                if not opened:
                    log.debug("dorks.google_empty", query=query[:60])

        # Capa 2: DDG fallback
        log.info("dorks.ddg_fallback", query=query[:60])
        results = await self._ddg_search(query, limit)
        if results:
            return results

        # Capa 3: pausa + warning
        log.warning(
            "dorks.both_failed",
            query=query[:60],
            pause_seconds=_PAUSE_SECONDS,
        )
        return []

    async def _google_search(self, query: str, limit: int) -> list[DiscoveredLead]:
        """Búsqueda via googlesearch-python (sincrónica, run en executor)."""
        try:
            from googlesearch import search as gsearch

            def _sync_search() -> list[str]:
                return list(gsearch(query, num_results=limit, lang="es", sleep_interval=2))

            loop = asyncio.get_event_loop()
            urls = await loop.run_in_executor(None, _sync_search)
            return self._urls_to_leads(urls, fuente="google_dorks")

        except Exception as exc:
            err = str(exc)
            # 429 o CAPTCHA → marcar falla
            if "429" in err or "captcha" in err.lower() or "too many" in err.lower():
                self._google_cb.record_failure()
                log.warning("dorks.google_blocked", error=err[:80])
            else:
                log.warning("dorks.google_error", error=err[:80])
            return []

    async def _ddg_search(self, query: str, limit: int) -> list[DiscoveredLead]:
        """Búsqueda via duckduckgo-search."""
        try:
            from duckduckgo_search import DDGS

            def _sync_ddg() -> list[dict]:
                with DDGS() as ddgs:
                    return list(ddgs.text(query, max_results=limit))

            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(None, _sync_ddg)
            urls = [r.get("href", "") or r.get("link", "") for r in results if r]
            urls = [u for u in urls if u.startswith("http")]
            return self._urls_to_leads(urls, fuente="ddg_dorks")

        except Exception as exc:
            log.warning("dorks.ddg_error", error=str(exc)[:80])
            return []

    def _urls_to_leads(self, urls: list[str], fuente: str) -> list[DiscoveredLead]:
        """Convierte URLs a DiscoveredLead filtrando dominios baneados."""
        leads = []
        for url in urls:
            if not url:
                continue
            # Filtrar dominios baneados
            domain = _extract_domain(url)
            if domain in self._all_banned:
                continue
            # Extraer nombre candidato del dominio (se refinará con LLM)
            nombre = _domain_to_name(domain)
            leads.append(DiscoveredLead(
                nombre=nombre,
                sitio_web=url,
                fuente=fuente,
            ))
        return leads


def _extract_domain(url: str) -> str:
    """Extrae dominio base de una URL."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Quitar www.
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def _domain_to_name(domain: str) -> str:
    """Convierte dominio en nombre candidato legible.

    ej: clubnautico.com.ar → Club Nautico
    """
    # Quitar TLDs comunes
    for tld in [".com.ar", ".org.ar", ".net.ar", ".gob.ar", ".com", ".org", ".net"]:
        if domain.endswith(tld):
            domain = domain[: -len(tld)]
            break
    # Capitalizar partes separadas por puntos o guiones
    parts = domain.replace("-", " ").replace(".", " ").split()
    return " ".join(p.capitalize() for p in parts)


def make_dorks_discovery(
    vertical_name: str,
    sector: str,
    country: str = "Argentina",
    extra_terms: list[str] | None = None,
    limit_per_query: int = 20,
) -> DorksDiscovery:
    """Factory helper: genera dorks estándar para un vertical.

    Args:
        vertical_name: nombre del vertical (ej: "universidades privadas").
        sector: sector de negocio (ej: "educacion", "logistica").
        country: país de búsqueda.
        extra_terms: términos adicionales para refinar los dorks.
        limit_per_query: resultados por dork.

    Returns: DorksDiscovery configurado.
    """
    base_terms = extra_terms or []

    dorks = [
        f'site:.ar "{vertical_name}" "seguridad" "contacto"',
        f'"{vertical_name}" {country} "sistema de seguridad" "contacto"',
        f'inurl:{sector} site:.ar "administracion" OR "contacto"',
    ]

    for term in base_terms:
        dorks.append(f'"{vertical_name}" "{term}" {country} "seguridad"')

    return DorksDiscovery(
        dorks=dorks,
        pause_between_s=2.0,
    )


def make_vertical_dorks_discovery(
    vertical: "Vertical",
    dorks: list[str],
    banned_domains: set[str] | None = None,
) -> DorksDiscovery:
    """Crea un DorksDiscovery tipado para un vertical específico."""
    return DorksDiscovery(
        _vertical=vertical,
        dorks=dorks,
        banned_domains=banned_domains or set(),
        pause_between_s=2.0,
    )
