"""Schema metadata para vertical Depósitos Fiscales."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class DepositosFiscalesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    superficie_m2: float | None = None
    altura_libre_m: float | None = None
    tipo_habilitacion: Literal["aduana", "zona_franca", "deposito_fiscal_general"] | None = None
    acceso_camiones: bool | None = None
    conexion_ferroviaria: bool | None = None
    numero_habilitacion_aduana: str | None = None
