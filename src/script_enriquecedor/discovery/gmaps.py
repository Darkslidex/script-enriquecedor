"""Descubrimiento de Hoteles via Google Places API.

Requiere GOOGLE_PLACES_KEY en .env.
Fallback a DorksDiscovery si la key no está configurada.
"""

from __future__ import annotations

import httpx
import structlog

from ..core.config import get_settings
from ..core.models import Vertical
from .base import DiscoveredLead, DiscoveryStrategy

log = structlog.get_logger(__name__)

_PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
_TIMEOUT = 15

# Ciudades argentinas a consultar (cubriendo las principales)
_AR_CITIES = [
    "Buenos Aires, Argentina",
    "Córdoba, Argentina",
    "Rosario, Argentina",
    "Mendoza, Argentina",
    "La Plata, Argentina",
    "Mar del Plata, Argentina",
    "San Miguel de Tucumán, Argentina",
    "Bariloche, Argentina",
    "Salta, Argentina",
    "Puerto Iguazú, Argentina",
]


class GMapsDiscovery(DiscoveryStrategy):
    """Descubre Hoteles via Google Places Text Search."""

    def __init__(
        self,
        query: str = "hotel",
        cities: list[str] | None = None,
    ) -> None:
        self.query = query
        self.cities = cities or _AR_CITIES

    @property
    def vertical(self) -> Vertical:
        return Vertical.HOTELES

    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        settings = get_settings()

        if not settings.google_places_key:
            log.warning("gmaps.no_api_key", msg="GOOGLE_PLACES_KEY no configurada, usando dorks fallback")
            return await _dorks_fallback(limit)

        leads: list[DiscoveredLead] = []
        seen_ids: set[str] = set()

        for city in self.cities:
            if len(leads) >= limit:
                break

            batch = await self._search_city(city, settings.google_places_key, limit - len(leads))
            for lead in batch:
                place_id = lead.raw_data.get("place_id", "")
                if place_id and place_id not in seen_ids:
                    seen_ids.add(place_id)
                    leads.append(lead)

        log.info("gmaps.done", found=len(leads))
        return leads[:limit]

    async def _search_city(
        self, city: str, api_key: str, limit: int
    ) -> list[DiscoveredLead]:
        """Busca hoteles en una ciudad via Places Text Search."""
        params = {
            "query": f"{self.query} en {city}",
            "key": api_key,
            "language": "es",
            "region": "ar",
        }

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                r = await client.get(_PLACES_NEARBY_URL, params=params)
                r.raise_for_status()
                data = r.json()

            if data.get("status") != "OK":
                log.warning("gmaps.api_error", status=data.get("status"), city=city)
                return []

            return _parse_places_response(data, city, limit)

        except Exception as exc:
            log.error("gmaps.fetch_error", city=city, error=str(exc)[:80])
            return []


def _parse_places_response(
    data: dict, city: str, limit: int
) -> list[DiscoveredLead]:
    """Parsea la respuesta de Places API a DiscoveredLead."""
    leads = []
    for place in data.get("results", [])[:limit]:
        nombre = place.get("name", "")
        address = place.get("formatted_address", "")
        website = place.get("website", "")
        place_id = place.get("place_id", "")

        # Extraer provincia de la dirección (último elemento antes de "Argentina")
        provincia = _extract_provincia(address)

        if nombre:
            leads.append(DiscoveredLead(
                nombre=nombre,
                sitio_web=website or None,
                provincia=provincia or None,
                fuente="google_places",
                raw_data={
                    "place_id": place_id,
                    "address": address,
                    "rating": place.get("rating"),
                },
            ))
    return leads


def _extract_provincia(address: str) -> str:
    """Extrae provincia de una dirección de Google Places."""
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",")]
    # Formato típico: "Calle 123, Localidad, Provincia, Argentina"
    if len(parts) >= 3 and "Argentina" in parts[-1]:
        return parts[-2]
    return ""


async def _dorks_fallback(limit: int) -> list[DiscoveredLead]:
    """Fallback Google Dorks si no hay API key de Places."""
    from .dorks import make_vertical_dorks_discovery

    dorks = [
        'site:.ar "hotel" "4 estrellas" OR "5 estrellas" "seguridad" "contacto"',
        '"hotel boutique" Argentina "sistema de seguridad" OR "CCTV" "contacto"',
        'inurl:hotel site:.ar "gerencia" "seguridad" Argentina',
        '"resort" Argentina "vigilancia" "contacto" site:.ar',
    ]
    strategy = make_vertical_dorks_discovery(Vertical.HOTELES, dorks)
    return await strategy.discover(limit=limit)
