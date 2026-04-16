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
    # Formato LiteLLM: "gpt-4o-mini" | "claude-haiku-4-5" | "gemini-2.5-flash" | "ollama/llama3.1"
    litellm_model: str = "gpt-4o-mini"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    gemini_api_key: str = ""

    # ── APIs de enriquecimiento ────────────────────────────────────────────
    hunter_api_key: str = ""
    google_places_key: str = ""

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
        """True si Hunter.io API key está configurada."""
        return bool(self.hunter_api_key)

    @property
    def has_google_places(self) -> bool:
        """True si Google Places API key está configurada."""
        return bool(self.google_places_key)

    @property
    def has_llm(self) -> bool:
        """True si al menos una API key de LLM está configurada."""
        # ollama/local no necesita API key
        if self.litellm_model.startswith("ollama/"):
            return True
        return bool(self.openai_api_key or self.anthropic_api_key or self.gemini_api_key)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna la instancia singleton de Settings (cacheada con lru_cache)."""
    return Settings()
