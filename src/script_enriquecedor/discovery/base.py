"""ABC para estrategias de descubrimiento de leads.

Cada vertical implementa un DiscoveryStrategy que retorna DiscoveredLead —
registros parciales con nombre + posible URL, listos para ser scrapeados
y enriquecidos en el pipeline.

El flujo del pipeline es:
    discovery → fetcher → llm_enrichment → hunter → geocoder → csv_writer
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..core.models import Vertical


@dataclass
class DiscoveredLead:
    """Lead parcial retornado por una estrategia de descubrimiento.

    Solo contiene los datos que el discovery puede obtener directamente
    (nombre, URL kandidata, ubicación). El resto se completa en enrichment.
    """

    nombre: str
    sitio_web: str | None = None
    partido: str | None = None
    provincia: str | None = None
    localidad: str | None = None
    fuente: str | None = None        # ej: "zonaprop", "argenprop", "caip"
    raw_data: dict = field(default_factory=dict)  # datos extras del scraping origen


class DiscoveryStrategy(ABC):
    """Estrategia base para descubrir leads de un vertical.

    Implementar `discover()` que retorne una lista de DiscoveredLead.
    El pipeline llama a discover() y luego scrapea/enriquece cada resultado.
    """

    @property
    @abstractmethod
    def vertical(self) -> Vertical:
        """Vertical al que corresponde esta estrategia."""
        ...

    @abstractmethod
    async def discover(self, limit: int = 100) -> list[DiscoveredLead]:
        """Retorna hasta `limit` leads candidatos para este vertical.

        Args:
            limit: Cantidad máxima de leads a retornar.

        Returns:
            Lista de DiscoveredLead (parciales, sin datos de contacto aún).
        """
        ...

    async def find_website(self, nombre: str, partido: str = "") -> str:
        """Busca el sitio web oficial de una organización usando DDG + Google.

        Override en subclases si la estrategia tiene una forma más directa
        de obtener URLs (ej: links del portal de origen).

        Returns:
            URL del sitio web o string vacío si no se encontró.
        """
        # Importación lazy para evitar circular import
        from .zonaprop_argenprop import search_official_website
        return await search_official_website(nombre, partido)
