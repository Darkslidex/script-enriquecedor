"""Búsqueda de emails por dominio via Snov.io API.

Flujo OAuth2 implícito (client_credentials):
  1. POST /v1/oauth/access_token → access_token (expira en 3600s)
  2. POST /v1/get-domain-emails  → lista de emails del dominio

Free tier: 50 créditos/mes.
Usar como fallback de Hunter cuando result.count == 0.

Uso:
    client = get_snov_client()
    emails = await client.find_emails("nordelta.com.ar")
    # → ["admin@nordelta.com.ar", "info@nordelta.com.ar"]
"""

import time
from dataclasses import dataclass, field

import httpx

from ..core.config import get_settings
from ..core.logger import get_logger

log = get_logger("snov")

_TOKEN_URL = "https://api.snov.io/v1/oauth/access_token"
_DOMAIN_EMAILS_URL = "https://api.snov.io/v1/get-domain-emails"


@dataclass
class SnovEmailResult:
    """Resultado de búsqueda de emails en Snov.io."""

    domain: str
    emails: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def found(self) -> bool:
        return bool(self.emails)

    @property
    def skipped(self) -> bool:
        return self.error == "no_credentials"


class SnovClient:
    """Cliente Snov.io con refresh automático de token OAuth2."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    async def _get_token(self) -> str | None:
        """Obtiene (o renueva) el access token. Retorna None si falla."""
        if self._access_token and time.time() < self._token_expires_at - 30:
            return self._access_token

        if not self._settings.has_snov:
            return None

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    _TOKEN_URL,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self._settings.snov_client_id,
                        "client_secret": self._settings.snov_client_secret,
                    },
                )

            if r.status_code != 200:
                log.warning("snov_token_error", status=r.status_code)
                return None

            payload = r.json()
            self._access_token = payload.get("access_token")
            expires_in = payload.get("expires_in", 3600)
            self._token_expires_at = time.time() + expires_in

            log.debug("snov_token_ok", expires_in=expires_in)
            return self._access_token

        except Exception as e:
            log.warning("snov_token_exception", error=str(e)[:80])
            return None

    async def find_emails(self, domain: str) -> SnovEmailResult:
        """Busca todos los emails asociados a un dominio.

        Args:
            domain: dominio sin esquema, ej: "nordelta.com.ar"

        Returns:
            SnovEmailResult con lista de emails encontrados.
        """
        domain = domain.removeprefix("https://").removeprefix("http://").split("/")[0]

        if not self._settings.has_snov:
            log.debug("snov_skip_no_credentials", domain=domain)
            return SnovEmailResult(domain=domain, error="no_credentials")

        token = await self._get_token()
        if not token:
            return SnovEmailResult(domain=domain, error="token_unavailable")

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.post(
                    _DOMAIN_EMAILS_URL,
                    data={"access_token": token, "domain": domain},
                )

            if r.status_code == 401:
                # Token expiró antes de lo esperado → limpiar y reintentar una vez
                self._access_token = None
                token = await self._get_token()
                if not token:
                    return SnovEmailResult(domain=domain, error="token_refresh_failed")
                async with httpx.AsyncClient(timeout=20) as client:
                    r = await client.post(
                        _DOMAIN_EMAILS_URL,
                        data={"access_token": token, "domain": domain},
                    )

            if r.status_code == 429:
                log.warning("snov_rate_limit", domain=domain)
                return SnovEmailResult(domain=domain, error="rate_limit")

            if r.status_code != 200:
                log.warning("snov_http_error", domain=domain, status=r.status_code)
                return SnovEmailResult(domain=domain, error=f"http_{r.status_code}")

            payload = r.json()
            # La API devuelve {"emails": [{"value": "...", "type": "..."}, ...]}
            raw_emails = payload.get("emails", [])
            emails = [e["value"] for e in raw_emails if e.get("value") and "@" in e["value"]]

            result = SnovEmailResult(domain=domain, emails=emails)
            log.info("snov_emails_found", domain=domain, count=len(emails))
            return result

        except Exception as e:
            log.warning("snov_exception", domain=domain, error=str(e)[:80])
            return SnovEmailResult(domain=domain, error=str(e)[:80])


# ── Singleton ──────────────────────────────────────────────────────────────────

_client: SnovClient | None = None


def get_snov_client() -> SnovClient:
    """Retorna el singleton de SnovClient."""
    global _client
    if _client is None:
        _client = SnovClient()
    return _client
