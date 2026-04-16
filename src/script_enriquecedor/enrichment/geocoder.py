"""Geocoding de direcciones a coordenadas (latitud, longitud).

Default: Nominatim (OpenStreetMap) — gratis, sin API key.
Alternativa: Google Maps Geocoding API (requiere GOOGLE_PLACES_KEY en .env).

Rate limit Nominatim:
  - Máximo 1 request/segundo (ToS de OpenStreetMap)
  - User-Agent obligatorio identificando la aplicación

Cache en memoria:
  - Evita re-geocodificar la misma dirección en la misma ejecución
  - No es persistente entre runs (las coords van en el CSV)

Uso:
    geo = get_geocoder()
    result = await geo.geocode("Av. del Mirador 100, Tigre, Buenos Aires")
    # result.lat, result.lon → -34.4056, -58.6339
"""

import asyncio
from dataclasses import dataclass

import httpx

from ..core.config import get_settings
from ..core.logger import get_logger

log = get_logger("geocoder")

# Nominatim — no requiere API key pero sí User-Agent identificatorio
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_USER_AGENT = "script-enriquecedor/2.0 (techcam.com.ar; b2b-pipeline)"

# Google Maps Geocoding API
_GMAPS_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# Pausa entre requests a Nominatim (1 req/s según ToS)
_NOMINATIM_RATE = 1.1


@dataclass
class GeoResult:
    """Resultado de geocoding."""

    query: str
    lat: float | None = None
    lon: float | None = None
    display_name: str | None = None
    provider: str | None = None
    error: str | None = None

    @property
    def found(self) -> bool:
        return self.lat is not None and self.lon is not None


class Geocoder:
    """Geocodificador con Nominatim (default) y Google Maps como alternativa."""

    def __init__(self) -> None:
        self._settings = get_settings()
        self._cache: dict[str, GeoResult] = {}
        self._last_nominatim_call: float = 0.0

    def _build_query(
        self,
        direccion: str | None = None,
        localidad: str | None = None,
        partido: str | None = None,
        provincia: str | None = None,
        pais: str = "Argentina",
    ) -> str:
        """Construye el string de búsqueda más completo posible."""
        parts = [p for p in [direccion, localidad, partido, provincia, pais] if p]
        return ", ".join(parts)

    async def _nominatim_rate_wait(self) -> None:
        """Espera para respetar el rate limit de Nominatim (1 req/s)."""
        import time
        elapsed = time.monotonic() - self._last_nominatim_call
        if elapsed < _NOMINATIM_RATE:
            await asyncio.sleep(_NOMINATIM_RATE - elapsed)
        self._last_nominatim_call = time.monotonic() if True else 0  # update after sleep

    async def geocode(
        self,
        direccion: str | None = None,
        localidad: str | None = None,
        partido: str | None = None,
        provincia: str | None = None,
        pais: str = "Argentina",
    ) -> GeoResult:
        """Geocodifica una dirección a coordenadas.

        Intenta Nominatim primero; si falla y hay Google Places key,
        intenta Google Maps.

        Args:
            direccion: Calle y número.
            localidad: Localidad/ciudad.
            partido:   Partido/municipio.
            provincia: Provincia.
            pais:      País (default Argentina).

        Returns:
            GeoResult con lat/lon si se encontró.
        """
        query = self._build_query(direccion, localidad, partido, provincia, pais)

        if not query.strip():
            return GeoResult(query="", error="sin_datos_de_ubicacion")

        # Cache hit
        if query in self._cache:
            return self._cache[query]

        # Intentar Nominatim
        result = await self._nominatim(query)

        # Fallback a Google Maps si Nominatim no encontró y hay key
        if not result.found and self._settings.has_google_places:
            result = await self._google_maps(query)

        self._cache[query] = result
        return result

    async def _nominatim(self, query: str) -> GeoResult:
        """Geocoding con Nominatim/OpenStreetMap."""
        import time
        # Respetar rate limit
        elapsed = time.monotonic() - self._last_nominatim_call
        if elapsed < _NOMINATIM_RATE:
            await asyncio.sleep(_NOMINATIM_RATE - elapsed)

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    _NOMINATIM_URL,
                    params={
                        "q": query,
                        "format": "json",
                        "limit": 1,
                        "countrycodes": "ar",
                    },
                    headers={"User-Agent": _NOMINATIM_USER_AGENT},
                )
            self._last_nominatim_call = asyncio.get_event_loop().time() if False else __import__('time').monotonic()

            if r.status_code != 200:
                return GeoResult(query=query, error=f"nominatim_http_{r.status_code}")

            results = r.json()
            if not results:
                log.debug("nominatim_not_found", query=query)
                return GeoResult(query=query, error="not_found", provider="nominatim")

            hit = results[0]
            geo = GeoResult(
                query=query,
                lat=float(hit["lat"]),
                lon=float(hit["lon"]),
                display_name=hit.get("display_name"),
                provider="nominatim",
            )
            log.debug("nominatim_ok", query=query, lat=geo.lat, lon=geo.lon)
            return geo

        except Exception as e:
            log.warning("nominatim_error", query=query, error=str(e)[:80])
            return GeoResult(query=query, error=str(e)[:80], provider="nominatim")

    async def _google_maps(self, query: str) -> GeoResult:
        """Geocoding con Google Maps Geocoding API."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    _GMAPS_URL,
                    params={
                        "address": query,
                        "key": self._settings.google_places_key,
                        "components": "country:AR",
                    },
                )

            data = r.json()
            if data.get("status") != "OK" or not data.get("results"):
                return GeoResult(query=query, error="gmaps_not_found", provider="google_maps")

            loc = data["results"][0]["geometry"]["location"]
            geo = GeoResult(
                query=query,
                lat=float(loc["lat"]),
                lon=float(loc["lng"]),
                display_name=data["results"][0].get("formatted_address"),
                provider="google_maps",
            )
            log.debug("gmaps_ok", query=query, lat=geo.lat, lon=geo.lon)
            return geo

        except Exception as e:
            log.warning("gmaps_error", query=query, error=str(e)[:80])
            return GeoResult(query=query, error=str(e)[:80], provider="google_maps")

    def apply_to_lead(self, lead, result: GeoResult) -> None:
        """Actualiza latitud/longitud en un Lead in-place."""
        if result.found:
            lead.latitud = result.lat
            lead.longitud = result.lon


# ── Singleton ──────────────────────────────────────────────────────────────────

_geocoder: Geocoder | None = None


def get_geocoder() -> Geocoder:
    """Retorna el singleton de Geocoder."""
    global _geocoder
    if _geocoder is None:
        _geocoder = Geocoder()
    return _geocoder
