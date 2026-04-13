# Changelog

Todos los cambios notables de este proyecto se documentarán en este archivo siguiendo la convención de [Keep a Changelog](https://keepachangelog.com/).

## [1.0.0] - 2026-04-13
### Agregado
- Versión inicial estable desacoplada como repositorio independiente.
- `llm_extractor.py`: Implementados esquemas Pydantic `AdminContact` y `ExtractionResult` para garantizar salidas JSON estrictas y tipeadas.
- Sistema de cascada en proveedores IA (soporte nativo para OpenRouter u LM Studio local).
- Tenacity configurado para lidiar internamente con `RateLimitError` y `APITimeoutError`.
- `pipeline_integrado_ia.py`: Scraper escalonado (fallback a Bing / DuckDuckGo si la BBDD madre no aporta URL válida) saltándose Firewalls (utilizando `curl_cffi`).
- Exportación estructurada continua en `.jsonl` (protegiendo el uso de memoria RAM y cuidando caídas del sistema).
- Módulo paralelo analítico en `analizar_resultados.py`.

### Adaptabilidad (Agregado)
- Código fuertemente independizado. Solo basta con modificar la constante `SYSTEM_PROMPT` dentro de `llm_extractor.py` y adaptar el CSV de origen, convirtiendo este script genérico en un recolector B2B capaz de penetrar y organizar las cúpulas directivas de cualquier rubro o industria con presencia en internet.
