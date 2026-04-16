"""Modelos de datos comunes a todos los verticales.

Define:
- Vertical: enum con los 15 verticales de negocio
- EstadoComercial: ciclo de vida de un lead
- Lead: modelo Pydantic v2 que mapea 1:1 con el schema Prisma del dashboard
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl


class Vertical(str, Enum):
    """Los 15 verticales de prospección B2B."""

    BARRIOS_PRIVADOS = "barrios_privados"
    HOTELES = "hoteles"
    UNIVERSIDADES = "universidades"
    ENTES_ESTATALES = "entes_estatales"
    CONSULADOS = "consulados"
    EMBAJADAS = "embajadas"
    DROGUERIAS = "droguerias"
    CLINICAS = "clinicas"
    DEPOSITOS_FISCALES = "depositos_fiscales"
    PARQUES_INDUSTRIALES = "parques_industriales"
    LOGISTICAS = "logisticas"
    EMPRESAS = "empresas"
    PLANTAS_INDUSTRIALES = "plantas_industriales"
    TERMINALES_PORTUARIAS = "terminales_portuarias"
    AERONAUTICAS = "aeronauticas"


# Nombre legible para mostrar en la UI
VERTICAL_DISPLAY_NAMES: dict[Vertical, str] = {
    Vertical.BARRIOS_PRIVADOS: "Barrios Privados",
    Vertical.HOTELES: "Hoteles",
    Vertical.UNIVERSIDADES: "Universidades",
    Vertical.ENTES_ESTATALES: "Entes Estatales",
    Vertical.CONSULADOS: "Consulados",
    Vertical.EMBAJADAS: "Embajadas",
    Vertical.DROGUERIAS: "Droguerías",
    Vertical.CLINICAS: "Clínicas Privadas",
    Vertical.DEPOSITOS_FISCALES: "Depósitos Fiscales",
    Vertical.PARQUES_INDUSTRIALES: "Parques Industriales",
    Vertical.LOGISTICAS: "Logísticas",
    Vertical.EMPRESAS: "Empresas",
    Vertical.PLANTAS_INDUSTRIALES: "Plantas Industriales",
    Vertical.TERMINALES_PORTUARIAS: "Terminales Portuarias",
    Vertical.AERONAUTICAS: "Empresas Aeronáuticas",
}


class EstadoComercial(str, Enum):
    """Ciclo de vida de un lead en el pipeline comercial."""

    SIN_CONTACTAR = "SIN_CONTACTAR"
    CONTACTADO = "CONTACTADO"
    EN_NEGOCIACION = "EN_NEGOCIACION"
    CLIENTE = "CLIENTE"
    DESCARTADO = "DESCARTADO"


# Tipo para email_score: entero entre 0 y 100
EmailScore = Annotated[int, Field(ge=0, le=100)]


class Lead(BaseModel):
    """Lead B2B genérico — mapea 1:1 con el modelo Prisma del dashboard.

    El campo `metadata` contiene datos específicos del vertical
    y debe validarse contra el schema Pydantic correspondiente
    (ver core/metadata_schemas/).
    """

    model_config = ConfigDict(
        # strict=False para permitir coerción de tipos al leer CSV
        strict=False,
        # Serializar HttpUrl como string (necesario para CSV)
        populate_by_name=True,
    )

    # ── Campos obligatorios ────────────────────────────────────────────────
    nombre: str
    vertical: Vertical

    # ── Estado comercial ───────────────────────────────────────────────────
    estado_comercial: EstadoComercial = EstadoComercial.SIN_CONTACTAR

    # ── Contacto ───────────────────────────────────────────────────────────
    email: EmailStr | None = None
    email_2: EmailStr | None = None
    email_3: EmailStr | None = None
    email_validado: bool = False
    email_score: EmailScore | None = None
    telefono: str | None = None
    sitio_web: HttpUrl | None = None
    fuente_contacto: str | None = None

    # ── Timestamps ─────────────────────────────────────────────────────────
    fecha_enriquecimiento: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ── Notas internas ─────────────────────────────────────────────────────
    notas: str | None = None

    # ── Ubicación ──────────────────────────────────────────────────────────
    direccion: str | None = None
    localidad: str | None = None
    partido: str | None = None
    provincia: str | None = None
    pais: str = "Argentina"
    cp: str | None = None
    latitud: float | None = None
    longitud: float | None = None

    # ── Metadata específica del vertical (JSONB en Prisma) ─────────────────
    metadata: dict = Field(default_factory=dict)

    def sitio_web_str(self) -> str | None:
        """Devuelve sitio_web como string (para CSV y logs)."""
        return str(self.sitio_web) if self.sitio_web else None
