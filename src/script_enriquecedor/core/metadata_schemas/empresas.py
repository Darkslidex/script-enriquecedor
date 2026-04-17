"""Schema metadata para vertical Empresas (genérico)."""

from typing import Literal
from pydantic import BaseModel, ConfigDict


class EmpresasMetadata(BaseModel):
    model_config = ConfigDict(strict=True)

    rubro: str | None = None
    cuit: str | None = None
    tipo_sociedad: Literal["SA", "SRL", "SAS", "sucursal_extranjera"] | None = None
    cantidad_empleados: int | None = None
    cantidad_sucursales: int | None = None
    facturacion_anual_categoria: Literal["PyME", "Mediana", "Grande", "Multinacional"] | None = None
