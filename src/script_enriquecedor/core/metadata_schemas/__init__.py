"""Registry de schemas Pydantic v2 por vertical.

Cada schema valida el campo `metadata` (JSONB en Prisma) del modelo Lead.
Usar METADATA_SCHEMA_REGISTRY para obtener el schema de un vertical dado.

Ejemplo:
    from script_enriquecedor.core.metadata_schemas import METADATA_SCHEMA_REGISTRY
    from script_enriquecedor.core.models import Vertical

    SchemaClass = METADATA_SCHEMA_REGISTRY[Vertical.BARRIOS_PRIVADOS]
    validated = SchemaClass(**raw_dict)
"""

from pydantic import BaseModel
from typing import Type

from .aeronauticas import AeronauticasMetadata
from .barrios_privados import BarriosPrivadosMetadata
from .clinicas import ClinicasMetadata
from .consulados import ConsuladosMetadata
from .depositos_fiscales import DepositosFiscalesMetadata
from .droguerias import DrogueriasMetadata
from .embajadas import EmbajadasMetadata
from .empresas import EmpresasMetadata
from .entes_estatales import EntesEstatalesMetadata
from .hoteles import HotelesMetadata
from .logisticas import LogisticasMetadata
from .parques_industriales import ParquesIndustrialesMetadata
from .plantas_industriales import PlantasIndustrialesMetadata
from .terminales_portuarias import TerminalesPortuariasMetadata
from .universidades import UniversidadesMetadata

# Importación lazy para evitar circular import (Vertical está en models.py)
def _build_registry() -> dict:
    from script_enriquecedor.core.models import Vertical
    return {
        Vertical.BARRIOS_PRIVADOS: BarriosPrivadosMetadata,
        Vertical.HOTELES: HotelesMetadata,
        Vertical.UNIVERSIDADES: UniversidadesMetadata,
        Vertical.ENTES_ESTATALES: EntesEstatalesMetadata,
        Vertical.CONSULADOS: ConsuladosMetadata,
        Vertical.EMBAJADAS: EmbajadasMetadata,
        Vertical.DROGUERIAS: DrogueriasMetadata,
        Vertical.CLINICAS: ClinicasMetadata,
        Vertical.DEPOSITOS_FISCALES: DepositosFiscalesMetadata,
        Vertical.PARQUES_INDUSTRIALES: ParquesIndustrialesMetadata,
        Vertical.LOGISTICAS: LogisticasMetadata,
        Vertical.EMPRESAS: EmpresasMetadata,
        Vertical.PLANTAS_INDUSTRIALES: PlantasIndustrialesMetadata,
        Vertical.TERMINALES_PORTUARIAS: TerminalesPortuariasMetadata,
        Vertical.AERONAUTICAS: AeronauticasMetadata,
    }


_registry: dict | None = None


def get_metadata_registry() -> dict:
    """Retorna el registry (lazy, se construye una vez)."""
    global _registry
    if _registry is None:
        _registry = _build_registry()
    return _registry


def get_metadata_schema(vertical) -> Type[BaseModel]:
    """Devuelve la clase schema para un vertical dado.

    Args:
        vertical: instancia de Vertical o string con el valor del enum

    Raises:
        KeyError: si el vertical no tiene schema registrado
    """
    from script_enriquecedor.core.models import Vertical
    if isinstance(vertical, str):
        vertical = Vertical(vertical)
    return get_metadata_registry()[vertical]


__all__ = [
    "AeronauticasMetadata",
    "BarriosPrivadosMetadata",
    "ClinicasMetadata",
    "ConsuladosMetadata",
    "DepositosFiscalesMetadata",
    "DrogueriasMetadata",
    "EmbajadasMetadata",
    "EmpresasMetadata",
    "EntesEstatalesMetadata",
    "HotelesMetadata",
    "LogisticasMetadata",
    "ParquesIndustrialesMetadata",
    "PlantasIndustrialesMetadata",
    "TerminalesPortuariasMetadata",
    "UniversidadesMetadata",
    "get_metadata_registry",
    "get_metadata_schema",
]
