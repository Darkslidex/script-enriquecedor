"""Configuración global del pipeline.

Lee variables de entorno desde .env (ver .env.example).
Usar get_settings() para obtener la instancia singleton.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración cargada desde .env y variables de entorno del sistema."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Ignora vars de entorno no declaradas (evita errores en CI/CD)
        extra="ignore",
    )

    # ── LLM ────────────────────────────────────────────────────────────────
    # Formato LiteLLM: "gpt-4o-mini" | "claude-haiku-4-5" | "gemini-2.5-flash"
    #                  "openrouter/meta-llama/llama-3.3-70b-instruct:free" | "ollama/llama3.1"
    litellm_model: str = "openrouter/openai/gpt-oss-20b:free"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    gemini_api_key: str = ""
    openrouter_api_key: str = ""

    # ── APIs de enriquecimiento ────────────────────────────────────────────
    hunter_api_key: str = ""
    google_places_key: str = ""

    # Snov.io — fallback de Hunter (50 créditos/mes en free tier)
    snov_client_id: str = ""
    snov_client_secret: str = ""

    # Apollo.io — búsqueda de decisores por empresa (10.000 créditos/mes free)
    apollo_api_key: str = ""

    # LinkedIn Sales Navigator — directorio donde se depositan los CSVs exportados manualmente
    linkedin_input_dir: str = "data/input/linkedin/"

    # PhantomBuster — mantenido como referencia/fallback, no usar como fuente activa
    phantombuster_input_dir: str = "data/input/phantombuster/"

    # ── VPS / Upload ───────────────────────────────────────────────────────
    vps_ssh_alias: str = "bunker"
    vps_app_path: str = "/root/apps/barrios-dashboard"
    vps_db_url: str = ""
    vps_db_password: str = ""

    # ── Comportamiento ─────────────────────────────────────────────────────
    # Pausa entre requests al mismo dominio (segundos)
    rate_limit_seconds: float = Field(default=3.0, ge=0.1)

    # Si True, desactiva sanitización PII en logs (solo debug local)
    verbose_logs: bool = False

    # Directorio raíz de datos (relativo al cwd del script)
    data_dir: str = "data"

    @property
    def has_hunter(self) -> bool:
        return bool(self.hunter_api_key)

    @property
    def has_snov(self) -> bool:
        return bool(self.snov_client_id and self.snov_client_secret)

    @property
    def has_apollo(self) -> bool:
        return bool(self.apollo_api_key)

    @property
    def has_google_places(self) -> bool:
        return bool(self.google_places_key)

    @property
    def has_llm(self) -> bool:
        """True si al menos una API key de LLM está configurada."""
        # ollama/local no necesita API key
        if self.litellm_model.startswith("ollama/"):
            return True
        return bool(
            self.openai_api_key
            or self.anthropic_api_key
            or self.gemini_api_key
            or self.openrouter_api_key
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna la instancia singleton de Settings (cacheada con lru_cache)."""
    return Settings()
