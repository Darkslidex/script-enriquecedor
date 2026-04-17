"""Schema metadata para vertical Entes Estatales."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class EntesEstatalesMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    jurisdiccion: Literal["nacional", "provincial", "municipal"] | None = None
    tipo_organismo: Literal["ministerio", "secretaria", "ente", "agencia", "municipio"] | None = None
    provincia_jurisdiccion: str | None = None
    responsable_nombre: str | None = None
    responsable_cargo: str | None = None
