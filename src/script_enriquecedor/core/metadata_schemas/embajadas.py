"""Schema metadata para vertical Embajadas."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class EmbajadasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    pais_representado: str | None = None
    barrio_caba: str | None = None
    nivel_seguridad: Literal["alto", "medio", "estandar"] | None = None
    tiene_residencia_embajador: bool | None = None
