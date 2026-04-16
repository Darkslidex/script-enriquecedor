"""Registro de estrategias de descubrimiento por vertical.

DISCOVERY_SOURCES: descripción legible de la fuente (para UI).
DISCOVERY_REGISTRY: mapeo Vertical → clase DiscoveryStrategy (Fase 2).
"""

from ..core.models import Vertical

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

# TODO: implementar en Fase 2 paso 15
# DISCOVERY_REGISTRY: dict[Vertical, Type[DiscoveryStrategy]] = { ... }
