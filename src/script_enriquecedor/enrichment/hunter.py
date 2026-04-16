"""Validación de emails via Hunter.io Email Verifier API.

Plan gratuito de Hunter: 25 verificaciones/mes → usar solo cuando hay un
email candidato real y queremos confirmar antes de hacer outreach.

Retorna:
  - email_validado: bool (True si Hunter dice "valid" o "accept_all")
  - email_score: int 0-100 (confianza de Hunter)

Si HUNTER_API_KEY no está configurada → skip silencioso (score=None, validado=False).

Uso:
    validator = get_email_validator()
    result = await validator.verify("admin@nordelta.com.ar")
    # result.valid → True/False
    # result.score → 85
"""

from dataclasses import dataclass

import httpx

from ..core.config import get_settings
from ..core.logger import get_logger

log = get_logger("hunter")

_HUNTER_API_URL = "https://api.hunter.io/v2/email-verifier"

# Estados de Hunter que consideramos "válido"
_VALID_STATUSES = {"valid", "accept_all"}


@dataclass
class EmailVerification:
    """Resultado de verificación de email."""

    email: str
    valid: bool = False
    score: int | None = None
    status: str | None = None
    error: str | None = None

    @property
    def skipped(self) -> bool:
        """True si la verificación fue saltada (sin API key o error de config)."""
        return self.error == "no_api_key"


class EmailValidator:
    """Cliente Hunter.io para verificación de emails.

    Hace una sola request por email (no hay batch en el plan gratuito).
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._api_key = self._settings.hunter_api_key

    async def verify(self, email: str) -> EmailVerification:
        """Verifica un email con Hunter.io.

        Args:
            email: Dirección de email a verificar.

        Returns:
            EmailVerification con valid, score y status.
        """
        if not email or "@" not in email:
            return EmailVerification(email=email, error="email_invalido")

        if not self._api_key:
            log.debug("hunter_skip_no_key", email=email)
            return EmailVerification(email=email, error="no_api_key")

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    _HUNTER_API_URL,
                    params={"email": email, "api_key": self._api_key},
                )

            if r.status_code == 401:
                log.error("hunter_invalid_key")
                return EmailVerification(email=email, error="api_key_invalida")

            if r.status_code == 429:
                log.warning("hunter_rate_limit")
                return EmailVerification(email=email, error="rate_limit")

            if r.status_code != 200:
                log.warning("hunter_http_error", status=r.status_code)
                return EmailVerification(email=email, error=f"http_{r.status_code}")

            data = r.json().get("data", {})
            status = data.get("status", "unknown")
            score = data.get("score")

            result = EmailVerification(
                email=email,
                valid=status in _VALID_STATUSES,
                score=int(score) if score is not None else None,
                status=status,
            )

            log.info(
                "email_verified",
                status=status,
                score=score,
                valid=result.valid,
            )
            return result

        except Exception as e:
            log.warning("hunter_error", error=str(e)[:80])
            return EmailVerification(email=email, error=str(e)[:80])

    async def verify_many(self, emails: list[str]) -> list[EmailVerification]:
        """Verifica múltiples emails de forma secuencial (Hunter no tiene batch).

        Por el límite del plan gratuito (25/mes), llamar con lista corta.
        """
        results = []
        for email in emails:
            result = await self.verify(email)
            results.append(result)
        return results

    def apply_to_lead(self, lead, result: EmailVerification) -> None:
        """Actualiza email_validado y email_score en un Lead in-place."""
        if not result.skipped and not result.error:
            lead.email_validado = result.valid
            lead.email_score = result.score


# ── Singleton ──────────────────────────────────────────────────────────────────

_validator: EmailValidator | None = None


def get_email_validator() -> EmailValidator:
    """Retorna el singleton de EmailValidator."""
    global _validator
    if _validator is None:
        _validator = EmailValidator()
    return _validator
