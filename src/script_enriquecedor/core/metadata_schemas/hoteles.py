"""Schema metadata para vertical Hoteles."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class HotelesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    categoria_estrellas: int | None = Field(None, ge=1, le=5)
    cantidad_habitaciones: int | None = None
    tipo: Literal["ciudad", "resort", "boutique", "apart", "hostel"] | None = None
    cadena: str | None = None
    tiene_estacionamiento: bool | None = None
    area_eventos: bool | None = None
