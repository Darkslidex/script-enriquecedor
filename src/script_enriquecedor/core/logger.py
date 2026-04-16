"""Logger estructurado con structlog.

Sanitización PII automática (default ON):
- Emails → SHA256[:8] + dominio en claro  (ej: a3f2b1c4@dominio.com)
- Teléfonos → últimos 4 dígitos           (ej: ****1234)

Desactivar con flag --verbose-logs o VERBOSE_LOGS=true en .env (solo debug local).
"""

# TODO: implementar (Fase 1 paso 2)
