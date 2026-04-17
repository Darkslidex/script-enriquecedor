"""Schema metadata para vertical Parques Industriales."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class ParquesIndustrialesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo: Literal["publico", "privado", "mixto"] | None = None
    superficie_total_has: float | None = None
    cantidad_empresas_radicadas: int | None = None
    servicios_disponibles: list[str] = Field(default_factory=list)
    tiene_acceso_ferroviario: bool | None = None
    provincia: str | None = None
