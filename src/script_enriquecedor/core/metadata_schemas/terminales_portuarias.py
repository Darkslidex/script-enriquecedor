"""Schema metadata para vertical Terminales Portuarias."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class TerminalesPortuariasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo_carga: Literal["contenedores", "graneles_solidos", "graneles_liquidos", "carga_general", "ro-ro"] | None = None
    calado_maximo_pies: float | None = None
    gruas_propias: bool | None = None
    conexion_ferroviaria: bool | None = None
    capacidad_anual_teu: int | None = None
    certificacion_isps: bool | None = None
    operador: str | None = None
