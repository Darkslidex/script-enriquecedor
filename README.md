# Script Enriquecedor de Bases de Datos B2B (con IA)

Este proyecto consta de un motor "pipeline" automatizado que toma una base de datos preexistente y la enriquece estructurando los datos de contacto oficiales utilizando un modelo de Inteligencia Artificial (LLM).

Si bien nació para recopilar información de administradores en **barrios privados**, el diseño *agnóstico* de estos scripts está pensado para ser modificado y atacar cualquier otro rubro (clubes, escuelas, hoteles, constructoras).

## ¿Cómo Funciona?
1. **Fallback a Buscadores:** Si en tu base de datos un registro no tiene página web, utiliza de manera programática buscadores (Bing/DuckDuckGo) para deducirla.
2. **Raspado (Scraping):** Extrae texto crudo (solo el contenido, descartando HTML) de subpáginas como `/contacto` o `/quienes-somos`. Para saltarse protecciones antibot utiliza `curl_cffi`.
3. **Validación Pydantic + LLM:** El texto se envía al modelo LLM usando directivas muy precisas (System Prompt). La respuesta se obliga a estructurarse bajo un esquema validado. En caso de fallar o de *rate limit*, posee sistemas de reintentos mediante `tenacity`.
4. **Guardado Lote a Lote:** Guarda resultados incrementales en un archivo `.jsonl` para posibilitar el pauseo/reanudación automática.

## Archivos Principales
* `pipeline_integrado_ia.py`: El orquestador oficial para lanzar todo el proceso.
* `llm_extractor.py`: Corazón de la inferencia. Contiene los prompts y llamadas API a la IA. **Acá debes editar los prompts si cambias de industria/rubro.**
* `lanzar_extraccion_masiva.py`: Similar al pipeline pero especializado en registros que ya sabemos que poseen web.
* `analizar_resultados.py`: Ejecútalo cuando el `.jsonl` esté listo para sacar estadísticas sobre el rendimiento de la extracción.

## Iniciar
1. Haz una copia del `.env.example`, llámalo `.env` y pega en él tu `OPENROUTER_API_KEY`.
2. Instala dependencias: `pip install pandas beautifulsoup4 curl_cffi instructor pydantic openai tenacity tqdm python-dotenv`
3. Ejecuta `python pipeline_integrado_ia.py`.
