"""Schema metadata para vertical Logísticas."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class LogisticasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    superficie_galpon_m2: float | None = None
    altura_libre_m: float | None = None
    temperatura_controlada: bool | None = None
    acceso_ferroviario: bool | None = None
    flota_propia: bool | None = None
    cantidad_vehiculos: int | None = None
    tipo_carga: Literal["general", "refrigerada", "peligrosa", "e-commerce"] | None = None
    certificacion_iso: bool | None = None
