"""Búsqueda de decisores por empresa via Apollo.io People Search API.

Usar cuando el sitio web no expone el nombre/cargo del decisor.
Free tier: 10.000 créditos/mes, 10 exportaciones.

Limitación conocida: cobertura LATAM ~20% de bounce rate.
Siempre validar emails devueltos por Apollo con Hunter/Snov antes de exportar.

Uso:
    searcher = get_apollo_searcher()
    people = await searcher.find_decision_makers("Nordelta SA")
    for p in people:
        print(p.full_name, p.title, p.email)
"""

from dataclasses import dataclass, field

import httpx

from ..core.config import get_settings
from ..core.logger import get_logger

log = get_logger("apollo")

_PEOPLE_SEARCH_URL = "https://api.apollo.io/v1/mixed_people/search"

# Cargos objetivo según sección 3.4 y 3.6 del handoff
CARGOS_OBJETIVO: list[str] = [
    "Gerente de Seguridad",
    "Jefe de Seguridad",
    "Director de Seguridad",
    "Gerente de Compras",
    "Jefe de Compras",
    "Gerente de Recursos Humanos",
    "Jefe de Recursos Humanos",
    "Director General",
    "Gerente General",
    "Dueño",
    "Socio",
    "CEO",
    "COO",
    "Director de Operaciones",
    "Gerente de Operaciones",
]


@dataclass
class ApolloContact:
    """Perfil de contacto devuelto por Apollo."""

    first_name: str = ""
    last_name: str = ""
    title: str = ""
    email: str | None = None
    linkedin_url: str | None = None
    organization_name: str = ""

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


@dataclass
class ApolloResult:
    """Resultado de búsqueda de personas en Apollo.io."""

    organization_name: str
    contacts: list[ApolloContact] = field(default_factory=list)
    error: str | None = None

    @property
    def found(self) -> bool:
        return bool(self.contacts)

    @property
    def skipped(self) -> bool:
        return self.error == "no_api_key"

    @property
    def best_contact(self) -> ApolloContact | None:
        """Retorna el primer contacto con email, o el primero de la lista."""
        with_email = [c for c in self.contacts if c.email]
        return with_email[0] if with_email else (self.contacts[0] if self.contacts else None)


class ApolloSearcher:
    """Cliente Apollo.io para búsqueda de decisores por empresa."""

    def __init__(self) -> None:
        self._settings = get_settings()

    async def find_decision_makers(
        self,
        organization_name: str,
        titles: list[str] | None = None,
        per_page: int = 5,
    ) -> ApolloResult:
        """Busca decisores en una empresa por nombre.

        Args:
            organization_name: nombre de la empresa tal como aparece públicamente.
            titles: lista de cargos a buscar. Si None, usa CARGOS_OBJETIVO global.
            per_page: máximo de resultados (Apollo cobra créditos por exportación).

        Returns:
            ApolloResult con lista de ApolloContact.
        """
        if not self._settings.has_apollo:
            log.debug("apollo_skip_no_key", org=organization_name)
            return ApolloResult(organization_name=organization_name, error="no_api_key")

        target_titles = titles or CARGOS_OBJETIVO

        payload = {
            "api_key": self._settings.apollo_api_key,
            "q_organization_name": organization_name,
            "person_titles": target_titles,
            "per_page": per_page,
            "page": 1,
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    _PEOPLE_SEARCH_URL,
                    json=payload,
                    headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
                )

            if r.status_code == 401:
                log.error("apollo_invalid_key")
                return ApolloResult(organization_name=organization_name, error="api_key_invalida")

            if r.status_code == 422:
                log.warning("apollo_unprocessable", org=organization_name)
                return ApolloResult(organization_name=organization_name, error="unprocessable")

            if r.status_code == 429:
                log.warning("apollo_rate_limit", org=organization_name)
                return ApolloResult(organization_name=organization_name, error="rate_limit")

            if r.status_code != 200:
                log.warning("apollo_http_error", org=organization_name, status=r.status_code)
                return ApolloResult(organization_name=organization_name, error=f"http_{r.status_code}")

            data = r.json()
            raw_people = data.get("people", [])

            contacts = [
                ApolloContact(
                    first_name=p.get("first_name", ""),
                    last_name=p.get("last_name", ""),
                    title=p.get("title", ""),
                    email=p.get("email"),
                    linkedin_url=p.get("linkedin_url"),
                    organization_name=p.get("organization_name", organization_name),
                )
                for p in raw_people
            ]

            result = ApolloResult(organization_name=organization_name, contacts=contacts)
            log.info("apollo_contacts_found", org=organization_name, count=len(contacts))
            return result

        except Exception as e:
            log.warning("apollo_exception", org=organization_name, error=str(e)[:80])
            return ApolloResult(organization_name=organization_name, error=str(e)[:80])

    def apply_to_lead(self, lead, result: ApolloResult) -> None:
        """Aplica el mejor contacto Apollo a un Lead in-place.

        Solo actualiza si el lead no tiene contacto_nombre ni email de Apollo aún.
        """
        if result.skipped or not result.found:
            return

        contact = result.best_contact
        if not contact:
            return

        if not lead.contacto_nombre and contact.full_name:
            lead.contacto_nombre = contact.full_name

        if not lead.contacto_cargo and contact.title:
            lead.contacto_cargo = contact.title

        # Solo usar email de Apollo como email_2 si el primario ya está ocupado
        if contact.email:
            if not lead.email:
                lead.email = contact.email
            elif not lead.email_2:
                lead.email_2 = contact.email


# ── Singleton ──────────────────────────────────────────────────────────────────

_searcher: ApolloSearcher | None = None


def get_apollo_searcher() -> ApolloSearcher:
    """Retorna el singleton de ApolloSearcher."""
    global _searcher
    if _searcher is None:
        _searcher = ApolloSearcher()
    return _searcher
