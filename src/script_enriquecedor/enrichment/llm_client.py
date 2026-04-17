"""Wrapper LiteLLM para extracción estructurada de leads.

Flujo por página HTML:
  1. Carga el prompt del vertical desde enrichment/prompts/<vertical>.md
  2. Llama a litellm.completion() en modo JSON
  3. Parsea la respuesta con el schema Lead + metadata del vertical
  4. Retries exponenciales con tenacity (3 intentos)
  5. Loggea tokens y costo estimado por llamada

Configuración:
  LITELLM_MODEL en .env (default: gpt-4o-mini)
  Compatible con: OpenAI, Anthropic, Gemini, Ollama (sin cambiar código)

Uso:
    client = get_llm_client()
    lead = await client.extract(html="<html>...", vertical=Vertical.BARRIOS_PRIVADOS)
"""

import json
import re
from pathlib import Path

import litellm
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..core.config import get_settings
from ..core.logger import get_logger
from ..core.metadata_schemas import get_metadata_schema
from ..core.models import Lead, Vertical

log = get_logger("llm_client")

# Ruta base de prompts (relativa al repo root)
_PROMPTS_DIR = Path(__file__).parent / "prompts"

# Máximo de caracteres de HTML a enviar al LLM (portado de v1: 6000-12000)
MAX_HTML_CHARS = 8_000

# Campos comunes del Lead que el LLM debe intentar extraer
_COMMON_FIELDS = """
- nombre: nombre oficial de la organización (obligatorio)
- email: email principal de contacto
- email_2: segundo email si hay
- email_3: tercer email si hay
- telefono: teléfono principal (formato +54 11 XXXX-XXXX cuando sea posible)
- sitio_web: URL del sitio oficial
- direccion: dirección postal
- localidad: localidad/ciudad
- partido: partido/municipio (para Buenos Aires)
- provincia: provincia
- cp: código postal
- fuente_contacto: origen del dato (ej: "web_oficial", "contacto_page")
- notas: observaciones relevantes del responsable (nombre, cargo)
""".strip()


class LLMClient:
    """Cliente LiteLLM para extracción estructurada de leads.

    Thread-safe para uso con asyncio.
    """

    def __init__(self) -> None:
        self._settings = get_settings()
        self._prompt_cache: dict[Vertical, str] = {}

        # Silenciar logs verbosos de litellm
        import logging
        logging.getLogger("LiteLLM").setLevel(logging.WARNING)

        # Pasar la API key de OpenRouter a LiteLLM si está configurada
        if self._settings.openrouter_api_key:
            import os
            os.environ.setdefault("OPENROUTER_API_KEY", self._settings.openrouter_api_key)

    # ── Prompt ────────────────────────────────────────────────────────────

    def _load_prompt(self, vertical: Vertical) -> str:
        """Carga el prompt del vertical desde el archivo .md (con cache)."""
        if vertical in self._prompt_cache:
            return self._prompt_cache[vertical]

        prompt_path = _PROMPTS_DIR / f"{vertical.value}.md"
        if prompt_path.exists():
            prompt = prompt_path.read_text(encoding="utf-8")
        else:
            # Fallback: prompt genérico si no existe el específico
            prompt = self._generic_prompt(vertical)
            log.warning("prompt_not_found", vertical=vertical.value, using="generic")

        self._prompt_cache[vertical] = prompt
        return prompt

    def _generic_prompt(self, vertical: Vertical) -> str:
        from ..core.models import VERTICAL_DISPLAY_NAMES
        from ..core.metadata_schemas import get_metadata_schema

        nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
        schema = get_metadata_schema(vertical)
        fields = ", ".join(schema.model_fields.keys())

        return (
            f"Sos un extractor de datos B2B para seguridad electrónica corporativa.\n\n"
            f"Del HTML que recibís, extraé información de contacto de "
            f"**{nombre}** en Argentina.\n\n"
            f"Campos comunes a extraer:\n{_COMMON_FIELDS}\n\n"
            f"Campos metadata específicos de {nombre}:\n{fields}\n\n"
            f"Reglas:\n"
            f"- Si un dato no está en el HTML, devolvé null. No inventes.\n"
            f"- Emails genéricos (info@, contacto@) son válidos.\n"
            f"- Respondé en JSON válido con los campos comunes + metadata.\n"
        )

    # ── Extracción ─────────────────────────────────────────────────────────

    async def extract(
        self,
        html: str,
        vertical: Vertical,
        source_url: str = "",
    ) -> Lead | None:
        """Extrae un Lead de HTML usando el LLM configurado.

        Args:
            html:       Texto limpio de la página (sin tags HTML).
            vertical:   Vertical activo (determina prompt y schema).
            source_url: URL de origen (para fuente_contacto).

        Returns:
            Lead parcial con campos extraídos, o None si el LLM falló.
        """
        content = html[:MAX_HTML_CHARS]
        system_prompt = self._load_prompt(vertical)
        schema_class = get_metadata_schema(vertical)

        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"Extraé los datos de contacto del siguiente contenido:\n\n"
                    f"---\n{content}\n---\n\n"
                    f"Respondé SOLO con JSON válido."
                ),
            },
        ]

        raw_dict = await self._call_with_retry(messages)
        if raw_dict is None:
            return None

        return self._build_lead(raw_dict, vertical, schema_class, source_url)

    async def _call_with_retry(
        self,
        messages: list[dict],
    ) -> dict | None:
        """Llama al LLM con retries exponenciales.

        Returns:
            Dict con la respuesta parseada, o None si todos los intentos fallan.
        """
        settings = self._settings
        model = settings.litellm_model

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=15),
                retry=retry_if_exception_type(Exception),
                reraise=False,
            ):
                with attempt:
                    # Modelos free de OpenRouter no soportan json_object mode
                    supports_json_mode = not (
                        ":free" in model or "ollama/" in model
                    )
                    kwargs: dict = {
                        "model": model,
                        "messages": messages,
                        "temperature": 0.0,
                        "timeout": 60,
                    }
                    if supports_json_mode:
                        kwargs["response_format"] = {"type": "json_object"}

                    response = await litellm.acompletion(**kwargs)

                    content = response.choices[0].message.content or ""
                    result = self._parse_json(content)

                    # Loggear uso de tokens y costo
                    usage = getattr(response, "usage", None)
                    cost = getattr(response, "_hidden_params", {}).get("response_cost", 0)
                    if usage:
                        log.info(
                            "llm_call",
                            model=model,
                            prompt_tokens=usage.prompt_tokens,
                            completion_tokens=usage.completion_tokens,
                            cost_usd=round(cost, 6) if cost else "n/a",
                        )

                    return result

        except Exception as e:
            log.error("llm_all_retries_failed", model=model, error=str(e)[:120])
            return None

    def _parse_json(self, content: str) -> dict:
        """Parsea la respuesta JSON del LLM (maneja markdown code blocks y thinking tags)."""
        # Eliminar bloques ```json ... ``` y etiquetas de thinking (<think>...</think>)
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL)
        content = re.sub(r"```(?:json)?\s*", "", content).strip().rstrip("`").strip()
        if not content:
            return {}
        # Intentar parsear directamente
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass
        # Fallback: extraer el primer bloque JSON del texto
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError as e:
                log.warning("llm_json_parse_error", error=str(e), preview=content[:100])
        return {}

    # ── Construcción del Lead ──────────────────────────────────────────────

    def _build_lead(
        self,
        raw: dict,
        vertical: Vertical,
        schema_class,
        source_url: str,
    ) -> Lead | None:
        """Construye un Lead validado a partir del dict del LLM."""
        if not raw.get("nombre"):
            log.warning("llm_no_nombre", vertical=vertical.value)
            return None

        # Separar campos comunes de metadata
        metadata_fields = set(schema_class.model_fields.keys())
        lead_fields = {k: v for k, v in raw.items() if k not in metadata_fields and v is not None}
        metadata_raw = {k: v for k, v in raw.items() if k in metadata_fields and v is not None}

        # Validar metadata contra el schema específico del vertical
        try:
            meta_obj = schema_class.model_validate(metadata_raw)
            metadata_dict = meta_obj.model_dump(exclude_none=True)
        except Exception as e:
            log.warning("metadata_validation_error", vertical=vertical.value, error=str(e)[:80])
            metadata_dict = {}

        # Construir Lead (strict=False permite coerción de tipos)
        try:
            lead = Lead(
                vertical=vertical,
                metadata=metadata_dict,
                fuente_contacto=source_url or lead_fields.pop("fuente_contacto", None),
                **{k: v for k, v in lead_fields.items() if k in Lead.model_fields},
            )
            return lead
        except Exception as e:
            log.warning("lead_build_error", vertical=vertical.value, error=str(e)[:120])
            return None


# ── Singleton ──────────────────────────────────────────────────────────────────

_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """Retorna el singleton de LLMClient."""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client
