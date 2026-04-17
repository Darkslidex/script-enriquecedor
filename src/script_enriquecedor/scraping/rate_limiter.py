"""Rate limiter por dominio usando token bucket.

Default: 1 request cada 3 segundos por dominio (configurable con RATE_LIMIT_SECONDS).

Diseño: token bucket simple con asyncio.Lock por dominio.
  - Cada dominio tiene su propio bucket independiente.
  - Múltiples dominios se pueden scrapear en paralelo sin bloquearse entre sí.
  - El mismo dominio respeta siempre la pausa mínima.

Uso:
    limiter = get_rate_limiter()
    await limiter.acquire("nordelta.com.ar")
    # → espera si se llamó hace menos de rate_limit_seconds
    response = await client.get(url)
"""

import asyncio
import time
from collections import defaultdict
from urllib.parse import urlparse

from ..core.config import get_settings
from ..core.logger import get_logger

log = get_logger("rate_limiter")


class DomainRateLimiter:
    """Token bucket por dominio. Thread-safe con asyncio.Lock."""

    def __init__(self, seconds_per_request: float | None = None) -> None:
        settings = get_settings()
        self._rate = seconds_per_request or settings.rate_limit_seconds
        # Último timestamp de request por dominio
        self._last_request: dict[str, float] = defaultdict(float)
        # Lock por dominio para evitar race conditions
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _extract_domain(self, url: str) -> str:
        """Extrae el dominio de una URL."""
        try:
            return urlparse(url).netloc.lower().lstrip("www.")
        except Exception:
            return url

    async def acquire(self, url_or_domain: str) -> None:
        """Espera si es necesario para respetar el rate limit del dominio.

        Args:
            url_or_domain: URL completa o dominio (ej: "nordelta.com.ar")
        """
        domain = (
            self._extract_domain(url_or_domain)
            if url_or_domain.startswith("http")
            else url_or_domain
        )

        async with self._locks[domain]:
            elapsed = time.monotonic() - self._last_request[domain]
            wait = self._rate - elapsed

            if wait > 0:
                log.debug("rate_limit_wait", domain=domain, wait_seconds=round(wait, 2))
                await asyncio.sleep(wait)

            self._last_request[domain] = time.monotonic()

    def set_rate(self, seconds: float) -> None:
        """Cambia el rate limit globalmente (para tests o override en runtime)."""
        self._rate = seconds


# ── Singleton ──────────────────────────────────────────────────────────────────

_limiter: DomainRateLimiter | None = None


def get_rate_limiter() -> DomainRateLimiter:
    """Retorna el singleton de DomainRateLimiter."""
    global _limiter
    if _limiter is None:
        _limiter = DomainRateLimiter()
    return _limiter
