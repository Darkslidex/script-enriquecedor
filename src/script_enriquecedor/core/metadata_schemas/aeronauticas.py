"""Schema metadata para vertical Empresas Aeronáuticas."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class AeronauticasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    tipo: Literal["aerolinea", "mro", "handling", "catering", "cargo_aereo", "aeropuerto_privado", "fbo"] | None = None
    anac_habilitado: bool | None = None
    hangar_propio: bool | None = None
    flota_aeronaves: int | None = None
    aeropuerto_base: Literal["Ezeiza", "Aeroparque", "El Palomar", "Córdoba", "Otro"] | None = None
    certificacion_iata: bool | None = None
