# Changelog

## [2.0.0] — en desarrollo (branch v2)

### Agregado
- Pipeline multi-vertical con 15 verticales de negocio
- CLI interactivo con Typer + Rich (menús anidados, progress bars, tablas)
- Modelo `Lead` común con campo `metadata JSONB` por vertical
- 15 schemas Pydantic de metadata (uno por vertical)
- LiteLLM como abstraction layer (OpenAI / Anthropic / Gemini / Ollama)
- Playwright como fallback automático para sitios JS-heavy o con 403
- Estrategia anti-detección Google Dorks en 3 capas (googlesearch → DDG → pausa)
- Geocoding con Nominatim (gratis) y Google Maps como alternativa paga
- Validación de emails vía Hunter.io API
- Sanitización PII en logs (SHA256 de emails, últimos 4 dígitos de teléfonos)
- Estado persistente en SQLite local (`data/state.db`)
- Gestión de lotes acumulados con preview y confirmación antes de upload
- Upload al VPS con confirmación explícita ("SUBIR")
- Generación automática de prompt LLM al activar un vertical nuevo
- Respeto estricto de `robots.txt` + rate limiting por dominio (token bucket)
- Tests: modelos, dedup, fetcher, dorks fallback, prompt generator, CSV↔Prisma

### Cambiado
- Estructura monolítica → paquete `src/script_enriquecedor/` con módulos separados
- Package manager: pip → uv
- Modelo de datos: campos planos → modelo `Lead` genérico con `metadata` tipado
- LLM client: OpenRouter directo → LiteLLM (multi-provider)

### Mantenido (compatible)
- Lógica de descubrimiento Zonaprop/Argenprop para Barrios Privados (portada de v1)
- Formato CSV compatible con `prisma/seed.ts` del dashboard

---

## [1.x] — Producción (branch main)

### Estado
- 1.511 barrios privados importados, 178 emails validados
- Script monolítico: `pipeline_integrado_ia.py`, `llm_extractor.py`, `lanzar_extraccion_masiva.py`
- Solo vertical: Barrios Privados (Zonaprop + Argenprop)
- LLM via OpenRouter, modelo configurable en `.env`
