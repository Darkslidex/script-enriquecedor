# Script Enriquecedor B2B v2 — Pipeline Multi-Vertical con IA

Pipeline de prospección B2B para seguridad electrónica. Descubre organizaciones en 15 verticales, scrapea sus sitios, extrae datos de contacto con LLM, valida emails con Hunter.io y geocodifica con Nominatim. Exporta CSVs listos para importar al dashboard Prisma.

## Verticales soportados

| Vertical | Fuente de Descubrimiento |
|---|---|
| Barrios Privados | Zonaprop + Argenprop |
| Parques Industriales | CAIP (caip.org.ar) |
| Droguerías | ANMAT (registro oficial) |
| Clínicas Privadas | SSSALUD (buscador prestadores) |
| Hoteles | Google Places API |
| Logísticas | ARLOG + Google Dorks |
| Universidades, Entes Estatales, Consulados, Embajadas, Depósitos Fiscales, Empresas, Plantas Industriales, Terminales Portuarias, Aeronáuticas | Google Dorks |

## Stack

- **Python 3.11+** con `uv` como package manager
- **LiteLLM** — abstracción multi-proveedor LLM (OpenAI / Anthropic / Gemini / Ollama)
- **httpx** + **Playwright** — fetching con fallback para SPAs
- **Hunter.io** — validación de emails (25/mes gratis)
- **Nominatim** — geocodificación (OpenStreetMap, gratis, 1 req/s)
- **SQLite** — estado local (lotes, ejecuciones, verticales activos)
- **Rich** + **Typer** — CLI con menús numerados y progress bar

## Instalación

```bash
# Clonar
git clone https://github.com/Darkslidex/script-enriquecedor.git
cd script-enriquecedor
git checkout v2

# Instalar dependencias
pip install uv
uv sync

# Instalar browsers Playwright (solo primera vez)
uv run playwright install chromium

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus keys
```

## Configuración (`.env`)

```bash
# LLM — elegir proveedor
LITELLM_MODEL=gpt-4o-mini          # OpenAI (recomendado)
# LITELLM_MODEL=claude-3-haiku-20240307  # Anthropic
# LITELLM_MODEL=gemini/gemini-1.5-flash  # Google
# LITELLM_MODEL=ollama/llama3.2          # Local (gratis)

OPENAI_API_KEY=sk-...              # si usás OpenAI
ANTHROPIC_API_KEY=sk-ant-...       # si usás Anthropic
GEMINI_API_KEY=...                 # si usás Gemini

# Opcional — enriquecimiento
HUNTER_API_KEY=...                 # validación de emails
GOOGLE_PLACES_KEY=...              # discovery de hoteles

# VPS — para sincronización con dashboard
VPS_SSH_ALIAS=bunker               # alias SSH configurado en ~/.ssh/config
VPS_APP_PATH=/root/apps/barrios-dashboard
VPS_DB_URL=postgresql://...

# Comportamiento
RATE_LIMIT_SECONDS=3.0             # pausa entre requests al mismo dominio
VERBOSE_LOGS=false
```

## Uso

```bash
# Lanzar menú interactivo
uv run enriquecedor

# Ver estado actual
uv run enriquecedor status

# Modo verbose
uv run enriquecedor --verbose
```

### Flujo principal

```
Menú principal
├── 1. Trabajar con vertical    → seleccionar vertical activo
│   ├── 1. Scrapear leads       → ejecuta pipeline completo
│   ├── 2. Ver lotes            → lotes pendientes de subir
│   ├── 3. Subir al VPS         → rsync + seed.ts
│   └── 4. Ver resumen          → métricas de calidad
├── 2. Activar nuevo vertical   → configura + genera prompt LLM
├── 3. Estado del sistema       → verticales activos, ejecuciones
└── 4. Configuración            → ver .env activo
```

### Ejemplo de ejecución programática

```python
import asyncio
from script_enriquecedor.pipeline import run, PipelineConfig
from script_enriquecedor.core.models import Vertical

config = PipelineConfig(
    vertical=Vertical.BARRIOS_PRIVADOS,
    limit=50,
    concurrency=3,
    skip_geocoding=False,
)
result = asyncio.run(run(config))
print(f"Descubiertos: {result.discovered}, Enriquecidos: {result.enriched}, Guardados: {result.saved}")
```

## Arquitectura

```
src/script_enriquecedor/
├── core/
│   ├── models.py          # Lead, Vertical, EstadoComercial (Pydantic v2)
│   ├── config.py          # Settings (pydantic-settings + .env)
│   ├── state.py           # SQLite — lotes, ejecuciones, verticales activos
│   ├── dedup.py           # Exact match + fuzzy (rapidfuzz) + vs producción
│   ├── logger.py          # structlog con sanitización PII
│   └── metadata_schemas/  # 15 schemas Pydantic por vertical
├── discovery/
│   ├── zonaprop_argenprop.py  # Barrios Privados (DDG + Google fallback)
│   ├── caip.py                # Parques Industriales
│   ├── anmat.py               # Droguerías
│   ├── sssalud.py             # Clínicas
│   ├── gmaps.py               # Hoteles (Google Places)
│   ├── arlog.py               # Logísticas
│   ├── dorks.py               # Google Dorks + circuit breaker + DDG fallback
│   └── registry.py            # get_discovery_strategy(vertical)
├── scraping/
│   ├── fetcher.py         # httpx async + Playwright fallback + SPA detection
│   ├── rate_limiter.py    # Token bucket por dominio
│   └── robots.py          # robots.txt con TTL cache 24h (fail-open)
├── enrichment/
│   ├── llm_client.py      # LiteLLM async + tenacity retries + JSON mode
│   ├── hunter.py          # Hunter.io email validation
│   ├── geocoder.py        # Nominatim + Google Maps fallback
│   ├── prompt_generator.py # Auto-genera prompts al activar verticals nuevos
│   └── prompts/           # .md por vertical (15 archivos)
├── storage/
│   ├── csv_writer.py      # CSV con headers compatibles con prisma/seed.ts
│   ├── batch_manager.py   # Gestión de lotes (pendiente/consolidado/subido)
│   ├── vps_uploader.py    # rsync + ssh seed.ts remoto
│   └── quality.py         # Score, BatchQualitySummary, vs-producción
├── ui/
│   ├── menus.py           # Menús numerados Rich
│   ├── tables.py          # Tablas Rich (leads, lotes, quality)
│   ├── progress.py        # PipelineProgress con barra y resumen final
│   └── prompts.py         # select(), confirm_exact(), ask_int()
└── pipeline.py            # Orquestador: discovery→fetch→LLM→Hunter→geo→batch
```

## Runbook Operativo

### Primera ejecución

```bash
# 1. Verificar configuración
uv run enriquecedor status

# 2. Lanzar pipeline de Barrios Privados (ya activo por defecto)
uv run enriquecedor
# → Trabajar → Barrios Privados → Scrapear leads → confirmar N leads
```

### Ciclo normal de trabajo

```bash
# 1. Scrapear (genera lote en data/enriched/barrios_privados/lote_XXXX.csv)
uv run enriquecedor

# 2. Revisar calidad antes de subir
# → Trabajar → Ver resumen  (score promedio, % email, duplicados)

# 3. Subir al VPS (requiere SSH configurado)
# → Trabajar → Subir al VPS → tipear "SUBIR" para confirmar
```

### Activar un nuevo vertical

```bash
# En el menú:
# → Activar nuevo vertical → seleccionar vertical
# → El sistema genera automáticamente el prompt LLM (enrichment/prompts/<vertical>.md)
# → Revisar y ajustar el prompt si es necesario
```

### Ajustar el prompt LLM manualmente

```bash
# Editar el archivo correspondiente:
nano src/script_enriquecedor/enrichment/prompts/barrios_privados.md

# Para regenerar desde cero:
uv run python3 -c "
from script_enriquecedor.enrichment.prompt_generator import generate_prompt
from script_enriquecedor.core.models import Vertical
generate_prompt(Vertical.BARRIOS_PRIVADOS, overwrite=True)
"
```

### Comparar batch vs producción antes de subir

```python
from pathlib import Path
from script_enriquecedor.storage.quality import compare_with_production
from script_enriquecedor.storage.batch_manager import consolidate
from script_enriquecedor.core.models import Vertical

# Consolidar lotes pendientes
consolidated_path, total = consolidate(Vertical.BARRIOS_PRIVADOS)

# Cargar leads del batch (desde CSV consolidado)
from script_enriquecedor.storage.csv_writer import read_csv
# ... (ver pipeline.py para ejemplo completo)

# Comparar con producción actual
prod_csv = Path("data/production_snapshot.csv")
result = compare_with_production(new_leads, prod_csv)
print(f"{result.total_new} leads nuevos de {result.total_checked} ({result.pct_new:.0f}%)")
```

### Troubleshooting

| Síntoma | Causa probable | Solución |
|---|---|---|
| `ModuleNotFoundError` | Deps no instaladas | `uv sync` |
| `playwright._impl._errors.Error` | Browsers no instalados | `uv run playwright install chromium` |
| `HUNTER_API_KEY not configured` | Key faltante en .env | Agregar key o ignorar (validación desactivada) |
| `rsync: command not found` | rsync no instalado | `sudo apt install rsync` |
| Google retorna 0 resultados | Bloqueo / CAPTCHA | El circuit breaker activa DDG automáticamente |
| `data/state.db` corrupto | Cierre abrupto | `rm data/state.db` (se recrea con Barrios Privados activo) |

## Tests

```bash
# Suite completa (121 tests)
uv run pytest

# Con cobertura
uv run pytest --cov=src --cov-report=term-missing

# Un módulo específico
uv run pytest tests/test_fuzzy_quality.py -v
```

## Compatibilidad con Dashboard

El CSV exportado tiene headers exactos compatibles con `prisma/seed.ts` del dashboard:

```
nombre, vertical, estado_comercial, email, email_2, email_3, email_validado,
email_score, telefono, sitio_web, fuente_contacto, fecha_enriquecimiento,
direccion, localidad, partido, provincia, pais, cp, latitud, longitud, metadata
```

El campo `metadata` es un JSON string con los campos específicos del vertical.  
El seed es **idempotente** — relanzar no genera duplicados en la base.

## Licencia

Uso interno — Techcam / Félix Lezama.
