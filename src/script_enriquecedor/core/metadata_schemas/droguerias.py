"""Schema metadata para vertical Droguerías."""

from pydantic import BaseModel, ConfigDict
from typing import Literal


class DrogueriasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo: Literal["cadena", "independiente"] | None = None
    cantidad_sucursales: int | None = None
    distribucion_mayorista: bool | None = None
    superficie_deposito_m2: float | None = None
    temperatura_controlada: bool | None = None
    habilitacion_anmat: bool | None = None
