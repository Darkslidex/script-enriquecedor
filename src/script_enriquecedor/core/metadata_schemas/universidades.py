"""Schema metadata para vertical Universidades."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class UniversidadesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo: Literal["publica", "privada", "terciario"] | None = None
    cantidad_alumnos: int | None = None
    cantidad_sedes: int | None = None
    carreras: list[str] = Field(default_factory=list)
    es_sede_principal: bool | None = None
    rector_decano: str | None = None
