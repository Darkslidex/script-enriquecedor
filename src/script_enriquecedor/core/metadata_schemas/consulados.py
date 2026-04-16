"""Schema metadata para vertical Consulados."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class ConsuladosMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    pais_representado: str | None = None
    tipo: Literal["embajada", "consulado_general", "consulado_honorario", "representacion"] | None = None
    horario_atencion: str | None = None
    zona_ciudad: str | None = None
