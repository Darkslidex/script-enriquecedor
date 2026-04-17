"""Schema metadata para vertical Plantas Industriales."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class PlantasIndustrialesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    rubro: str | None = None
    superficie_m2: float | None = None
    turnos_trabajo: int | None = None
    cantidad_empleados_planta: int | None = None
    certificaciones: list[str] = Field(default_factory=list)
    capacidad_instalada: str | None = None
    tipo_proceso: Literal["continuo", "por_lotes"] | None = None
