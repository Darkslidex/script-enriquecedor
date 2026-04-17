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
    """Retorna la estrategia de descubrimiento para un vertical."""
    from .zonaprop_argenprop import ZonapropArgenpropDiscovery
    from .caip import CAIPDiscovery
    from .anmat import ANMATDiscovery
    from .sssalud import SSSALUDDiscovery
    from .gmaps import GMapsDiscovery
    from .arlog import ARLOGDiscovery
    from .dorks import make_vertical_dorks_discovery

    _DIRECT_MAP: dict[Vertical, DiscoveryStrategy] = {
        Vertical.BARRIOS_PRIVADOS: ZonapropArgenpropDiscovery(),
        Vertical.PARQUES_INDUSTRIALES: CAIPDiscovery(),
        Vertical.DROGUERIAS: ANMATDiscovery(),
        Vertical.CLINICAS: SSSALUDDiscovery(),
        Vertical.HOTELES: GMapsDiscovery(),
        Vertical.LOGISTICAS: ARLOGDiscovery(),
    }

    if vertical in _DIRECT_MAP:
        return _DIRECT_MAP[vertical]

    # Verticales con Google Dorks
    _DORKS_CONFIG: dict[Vertical, list[str]] = {
        Vertical.UNIVERSIDADES: [
            'site:.ar "universidad privada" "seguridad" "contacto"',
            '"universidad" Argentina "sistema de seguridad" OR "CCTV" site:.ar',
        ],
        Vertical.ENTES_ESTATALES: [
            'site:gob.ar "seguridad" "contacto" "infraestructura"',
            '"organismo estatal" Argentina "seguridad electronica" "contacto"',
        ],
        Vertical.CONSULADOS: [
            '"consulado" Argentina "seguridad" "contacto" site:.ar OR site:.com',
            '"consulado general" Buenos Aires "seguridad" "contacto"',
        ],
        Vertical.EMBAJADAS: [
            '"embajada" Buenos Aires "seguridad" "contacto"',
            '"embajada" Argentina "sistema de seguridad" "contacto"',
        ],
        Vertical.DEPOSITOS_FISCALES: [
            'site:.ar "deposito fiscal" "seguridad" "contacto"',
            '"zona franca" OR "deposito fiscal" Argentina "seguridad electronica"',
        ],
        Vertical.EMPRESAS: [
            'site:.ar "empresa" "seguridad electronica" "contacto" Argentina',
            '"empresa manufacturera" Argentina "CCTV" OR "camara de seguridad" "contacto"',
        ],
        Vertical.PLANTAS_INDUSTRIALES: [
            'site:.ar "planta industrial" "seguridad" "contacto"',
            '"planta de produccion" Argentina "sistema de seguridad" "contacto"',
        ],
        Vertical.TERMINALES_PORTUARIAS: [
            'site:.ar "terminal portuaria" "seguridad" "contacto"',
            '"puerto" Argentina "seguridad" "terminal" "contacto" site:.ar',
        ],
        Vertical.AERONAUTICAS: [
            'site:.ar "empresa aeronautica" "seguridad" "contacto"',
            '"aviacion" OR "aeronautica" Argentina "seguridad" "hangar" "contacto"',
        ],
    }

    dorks = _DORKS_CONFIG.get(vertical, [
        f'site:.ar "{vertical.value.replace("_", " ")}" "seguridad" "contacto"',
    ])
    return make_vertical_dorks_discovery(vertical, dorks)
