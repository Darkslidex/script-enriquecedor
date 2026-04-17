"""Logger estructurado con structlog + sanitización PII automática.

Sanitización (activa por default, desactivar con VERBOSE_LOGS=true):
- Emails   → SHA256[:8] del local part + dominio en claro  (ej: a3f2b1c4@dominio.com)
- Teléfonos → últimos 4 dígitos                             (ej: ****-1234)

Uso:
    from script_enriquecedor.core.logger import get_logger
    log = get_logger(__name__)
    log.info("procesando_lead", nombre="Nordelta", email="admin@nordelta.com")
    # → procesando_lead nombre=Nordelta email=a3f2b1c4@nordelta.com
"""

import hashlib
import logging
import re
import sys
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
# Teléfonos: al menos 8 dígitos, puede tener +, espacios, guiones, paréntesis
_PHONE_RE = re.compile(r"(?:\+?\d[\d\s\.\-\(\)]{6,}\d)")


def _hash_email(email: str) -> str:
    """Oculta el local part del email con SHA256[:8]."""
    local, _, domain = email.partition("@")
    hashed = hashlib.sha256(local.encode()).hexdigest()[:8]
    return f"{hashed}@{domain}"


def _mask_phone(phone: str) -> str:
    """Muestra solo los últimos 4 dígitos del teléfono."""
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 4:
        return f"****-{digits[-4:]}"
    return "****"


def _sanitize_str(value: str) -> str:
    """Aplica sanitización de PII a un string."""
    value = _EMAIL_RE.sub(lambda m: _hash_email(m.group(0)), value)
    value = _PHONE_RE.sub(lambda m: _mask_phone(m.group(0)), value)
    return value


def _sanitize_value(value: Any) -> Any:
    """Sanitiza recursivamente strings, listas y dicts."""
    if isinstance(value, str):
        return _sanitize_str(value)
    if isinstance(value, list):
        return [_sanitize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _sanitize_value(v) for k, v in value.items()}
    return value


class PIISanitizer:
    """Processor de structlog que sanitiza PII en todos los valores del evento."""

    def __init__(self) -> None:
        # Importación lazy para evitar circular import en tiempo de carga del módulo
        self._verbose: bool | None = None

    def _is_verbose(self) -> bool:
        if self._verbose is None:
            try:
                from .config import get_settings
                self._verbose = get_settings().verbose_logs
            except Exception:
                self._verbose = False
        return self._verbose

    def __call__(self, logger: WrappedLogger, method: str, event_dict: EventDict) -> EventDict:
        if self._is_verbose():
            return event_dict
        return {k: _sanitize_value(v) for k, v in event_dict.items()}


_configured = False


def configure_logging(verbose: bool = False) -> None:
    """Configura structlog globalmente. Llamar una vez al iniciar el CLI."""
    global _configured

    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            PIISanitizer(),
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    _configured = True


def get_logger(name: str = "enriquecedor") -> structlog.BoundLogger:
    """Retorna un logger structlog. Configura si no fue configurado aún."""
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)
