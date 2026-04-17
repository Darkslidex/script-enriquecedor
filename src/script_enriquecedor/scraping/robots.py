"""Respeto estricto de robots.txt via urllib.robotparser.

Si un dominio bloquea el path solicitado:
  → skip + log warning
  → NO override posible (no hay --ignore-robots ni similar)

Cache TTL: 24 horas por dominio (para no descargar robots.txt en cada request).

Uso:
    robots = get_robots_checker()
    if await robots.can_fetch("https://nordelta.com.ar/contacto"):
        ...  # proceder con el fetch
    else:
        log.warning("robots_blocked", url=url)
"""

import asyncio
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx

from ..core.logger import get_logger
from .user_agents import get_desktop_ua

log = get_logger("robots")

# TTL del cache de robots.txt por dominio (24 horas en segundos)
_ROBOTS_CACHE_TTL = 86400


class RobotsChecker:
    """Cache de robots.txt con verificación de permisos por URL."""

    def __init__(self) -> None:
        # (robots_parser, timestamp_fetched)
        self._cache: dict[str, tuple[RobotFileParser | None, float]] = {}
        self._lock = asyncio.Lock()

    def _get_robots_url(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    def _is_cache_valid(self, domain: str) -> bool:
        if domain not in self._cache:
            return False
        _, fetched_at = self._cache[domain]
        return (time.monotonic() - fetched_at) < _ROBOTS_CACHE_TTL

    async def _fetch_robots(self, url: str) -> RobotFileParser | None:
        """Descarga y parsea el robots.txt de un dominio."""
        robots_url = self._get_robots_url(url)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    robots_url,
                    headers={"User-Agent": get_desktop_ua()},
                    follow_redirects=True,
                )
            if r.status_code == 200:
                parser = RobotFileParser()
                parser.set_url(robots_url)
                parser.parse(r.text.splitlines())
                return parser
            # 404 / error → sin restricciones
            return None
        except Exception as e:
            log.debug("robots_fetch_failed", url=robots_url, error=str(e)[:60])
            return None

    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """True si el robots.txt del dominio permite fetchear la URL.

        Si no se puede descargar el robots.txt, asume permiso (fail-open).

        Args:
            url:        URL completa a verificar.
            user_agent: User agent a consultar (default "*" → regla general).
        """
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        async with self._lock:
            if not self._is_cache_valid(domain):
                parser = await self._fetch_robots(url)
                self._cache[domain] = (parser, time.monotonic())

        parser, _ = self._cache[domain]

        if parser is None:
            # No se pudo obtener robots.txt → permitir (fail-open)
            return True

        allowed = parser.can_fetch(user_agent, url)

        if not allowed:
            log.warning("robots_blocked", url=url, domain=domain)

        return allowed

    def invalidate(self, domain: str) -> None:
        """Fuerza re-descarga del robots.txt de un dominio."""
        self._cache.pop(domain, None)


# ── Singleton ──────────────────────────────────────────────────────────────────

_checker: RobotsChecker | None = None


def get_robots_checker() -> RobotsChecker:
    """Retorna el singleton de RobotsChecker."""
    global _checker
    if _checker is None:
        _checker = RobotsChecker()
    return _checker
