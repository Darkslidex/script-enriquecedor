"""Schema metadata para vertical Clínicas Privadas."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class ClinicasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo: Literal["policlinico", "especialidad", "sanatorio", "maternidad"] | None = None
    cantidad_camas: int | None = None
    guardia_24hs: bool | None = None
    especialidades: list[str] = Field(default_factory=list)
    obras_sociales_principales: list[str] = Field(default_factory=list)
    habilitacion_ministerio: bool | None = None
