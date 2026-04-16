"""Registro de estrategias de descubrimiento por vertical.

DISCOVERY_SOURCES: descripción legible de la fuente (para UI).
get_discovery_strategy(): retorna instancia de DiscoveryStrategy para un vertical.
"""

from ..core.models import Vertical
from .base import DiscoveryStrategy

# Descripción de la fuente de descubrimiento por vertical (mostrada en menús)
DISCOVERY_SOURCES: dict[Vertical, str] = {
    Vertical.BARRIOS_PRIVADOS: "Zonaprop + Argenprop",
    Vertical.PARQUES_INDUSTRIALES: "CAIP (caip.org.ar)",
    Vertical.DROGUERIAS: "ANMAT (registro oficial)",
    Vertical.CLINICAS: "SSSALUD (buscador prestadores)",
    Vertical.HOTELES: "Google Places API",
    Vertical.LOGISTICAS: "ARLOG + Google Dorks",
    Vertical.UNIVERSIDADES: "Google Dorks",
    Vertical.ENTES_ESTATALES: "Google Dorks",
    Vertical.CONSULADOS: "Google Dorks",
    Vertical.EMBAJADAS: "Google Dorks",
    Vertical.DEPOSITOS_FISCALES: "Google Dorks",
    Vertical.EMPRESAS: "Google Dorks",
    Vertical.PLANTAS_INDUSTRIALES: "Google Dorks",
    Vertical.TERMINALES_PORTUARIAS: "Google Dorks",
    Vertical.AERONAUTICAS: "Google Dorks",
}


def get_discovery_strategy(vertical: Vertical) -> DiscoveryStrategy:
    """Retorna la estrategia de descubrimiento para un vertical.

    En Fase 1, solo Barrios Privados tiene implementación real.
    El resto usa un stub que retorna lista vacía (se implementará en Fase 2).
    """
    if vertical == Vertical.BARRIOS_PRIVADOS:
        from .zonaprop_argenprop import ZonapropArgenpropDiscovery
        return ZonapropArgenpropDiscovery()

    # Stub para verticales no implementados aún (Fase 2)
    from .base import DiscoveredLead

    class _StubDiscovery(DiscoveryStrategy):
        async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
            return []

    return _StubDiscovery()
