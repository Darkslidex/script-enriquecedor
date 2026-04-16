"""Wrapper LiteLLM para extracción estructurada con schema Pydantic.

- Lee LITELLM_MODEL de .env (default: gpt-4o-mini)
- Usa litellm.completion() con response_format apuntando al schema del vertical
- Retries con tenacity (3 intentos, exponential backoff)
- Loggea tokens/costo por llamada
"""

# TODO: implementar (Fase 1 paso 8)
