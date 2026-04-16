"""Schema metadata para vertical Barrios Privados."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class BarriosPrivadosMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    zona: str | None = None                    # "GBA Norte", "GBA Sur", etc.
    distancia_km_bsas: float | None = None
    en_base_actual: bool = False
    cantidad_lotes: int | None = None
    superficie_has: float | None = None
    tipo: Literal["country", "barrio_cerrado", "chacra", "pueblo_privado"] | None = None
    administradora: str | None = None
