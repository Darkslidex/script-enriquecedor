"""Orquestador end-to-end del pipeline.

Conecta: discovery → fetcher → LLM enrichment → Hunter → geocoder → csv_writer → batch_manager
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import structlog

from .core.config import get_settings
from .core.models import Lead, Vertical
from .core.state import get_state
from .discovery.base import DiscoveredLead
from .discovery.registry import get_discovery_strategy
from .enrichment.geocoder import Geocoder
from .enrichment.hunter import EmailValidator
from .enrichment.llm_client import get_llm_client
from .scraping.fetcher import Fetcher
from .storage.batch_manager import save_lote
from .ui.progress import PipelineProgress

log = structlog.get_logger(__name__)


@dataclass
class PipelineConfig:
    """Parámetros de ejecución del pipeline."""
    vertical: Vertical
    limit: int = 50
    concurrency: int = 3
    skip_geocoding: bool = False
    skip_email_validation: bool = False
    dry_run: bool = False  # no guarda leads ni lotes


@dataclass
class PipelineResult:
    """Resultado de una ejecución completa."""
    vertical: Vertical
    discovered: int = 0
    fetched: int = 0
    enriched: int = 0
    validated_email: int = 0
    geocoded: int = 0
    saved: int = 0
    errors: int = 0
    lote_id: int | None = None
    leads: list[Lead] = field(default_factory=list)


async def run(config: PipelineConfig) -> PipelineResult:
    """Ejecuta el pipeline completo para un vertical.

    Fases:
    1. Discovery: obtiene lista de leads candidatos (nombre + sitio_web)
    2. Fetching: descarga HTML de cada sitio (httpx → Playwright fallback)
    3. LLM extraction: extrae campos estructurados desde HTML
    4. Email validation: Hunter.io (opcional)
    5. Geocoding: Nominatim → Google (opcional)
    6. Batch save: persiste CSV + estado

    Args:
        config: parámetros de ejecución.

    Returns: PipelineResult con métricas de la ejecución.
    """
    settings = get_settings()
    state = get_state()
    result = PipelineResult(vertical=config.vertical)

    # Registra ejecución en state
    ejecucion = state.create_ejecucion(config.vertical)
    ejecucion_id = ejecucion.id  # type: ignore[attr-defined]

    log.info(
        "pipeline.start",
        vertical=config.vertical.value,
        limit=config.limit,
        dry_run=config.dry_run,
    )

    with PipelineProgress(
        total=config.limit,
        lote_name=f"pipeline_{config.vertical.value}",
        vertical=config.vertical,
    ) as progress:

        # ── Fase 1: Discovery ──────────────────────────────────────────────
        try:
            strategy = get_discovery_strategy(config.vertical)
            discovered: list[DiscoveredLead] = await strategy.discover(limit=config.limit)
            result.discovered = len(discovered)
            progress.set_discovered(result.discovered)
            log.info("pipeline.discovery.ok", count=result.discovered)
        except Exception as exc:
            log.error("pipeline.discovery.error", exc=str(exc))
            result.errors += 1
            _finish_ejecucion(state, ejecucion_id, result)
            return result

        if not discovered:
            log.warning("pipeline.no_leads_discovered")
            _finish_ejecucion(state, ejecucion_id, result)
            return result

        # ── Fase 2: Fetching ───────────────────────────────────────────────
        urls = [d.sitio_web for d in discovered if d.sitio_web]
        result.fetched = len(urls)

        fetcher = Fetcher()
        fetch_results = await fetcher.fetch_many(urls, concurrency=config.concurrency)
        fetch_map: dict[str, str] = {}  # url → html text
        for fr in fetch_results:
            if fr.has_content:
                fetch_map[fr.url] = fr.text
                progress.advance_scraped()
            else:
                result.errors += 1
                progress.add_error()

        # ── Fase 3: LLM extraction ─────────────────────────────────────────
        llm = get_llm_client()
        leads: list[Lead] = []

        for disc in discovered:
            url = disc.sitio_web
            html = fetch_map.get(url or "", "") if url else ""

            if not html:
                # Sin HTML: crea lead mínimo con datos del discovery
                lead = Lead(
                    nombre=disc.nombre,
                    vertical=config.vertical,
                    partido=disc.partido,
                    sitio_web=url,
                    fuente_contacto=disc.fuente,
                )
                leads.append(lead)
                progress.advance_enriched()
                continue

            try:
                extracted = await llm.extract(
                    html=html,
                    vertical=config.vertical,
                    source_url=url or "",
                )
                if extracted:
                    updates: dict = {}
                    if not extracted.partido and disc.partido:
                        updates["partido"] = disc.partido
                    if not extracted.sitio_web and url:
                        updates["sitio_web"] = url
                    if updates:
                        extracted = extracted.model_copy(update=updates)
                    leads.append(extracted)
                    progress.advance_enriched()
                else:
                    lead = Lead(
                        nombre=disc.nombre,
                        vertical=config.vertical,
                        partido=disc.partido,
                        sitio_web=url,
                        fuente_contacto=disc.fuente,
                    )
                    leads.append(lead)
            except Exception as exc:
                log.error("pipeline.llm.error", url=url, exc=str(exc))
                result.errors += 1
                progress.add_error()

        result.enriched = len([l for l in leads if l.email or l.telefono or l.latitud])
        log.info("pipeline.llm.ok", leads=len(leads))

        # ── Fase 4: Email validation ───────────────────────────────────────
        if not config.skip_email_validation and settings.hunter_api_key:
            validator = EmailValidator()
            leads_with_email = [lead for lead in leads if lead.email]
            email_tasks = [_validate_email(validator, lead) for lead in leads_with_email]
            email_results = await asyncio.gather(*email_tasks, return_exceptions=True)
            for res in email_results:
                if isinstance(res, Exception):
                    result.errors += 1
                    progress.add_error()
                else:
                    result.validated_email += 1
                    progress.advance_validated()

        # ── Fase 5: Geocoding ──────────────────────────────────────────────
        if not config.skip_geocoding:
            geocoder = Geocoder()
            geo_tasks = [_geocode_lead(geocoder, lead) for lead in leads]
            geo_results = await asyncio.gather(*geo_tasks, return_exceptions=True)
            for res in geo_results:
                if isinstance(res, Exception):
                    result.errors += 1
                else:
                    result.geocoded += 1
                    progress.advance_geocoded()

        # ── Fase 6: Save batch ─────────────────────────────────────────────
        if not config.dry_run and leads:
            lote_id = save_lote(config.vertical, leads)
            result.lote_id = lote_id
            result.saved = len(leads)
            log.info("pipeline.batch_saved", lote_id=lote_id, leads=result.saved)

        result.leads = leads

    # Finaliza ejecución en state
    _finish_ejecucion(state, ejecucion_id, result)

    log.info(
        "pipeline.done",
        vertical=config.vertical.value,
        discovered=result.discovered,
        enriched=result.enriched,
        saved=result.saved,
        errors=result.errors,
    )
    return result


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _validate_email(validator: EmailValidator, lead: Lead) -> None:
    """Valida email del lead in-place."""
    if not lead.email:
        return
    result = await validator.verify(lead.email)
    validator.apply_to_lead(lead, result)


async def _geocode_lead(geocoder: Geocoder, lead: Lead) -> None:
    """Geocodifica dirección del lead in-place."""
    geo = await geocoder.geocode(
        direccion=lead.direccion or "",
        localidad=lead.localidad or "",
        partido=lead.partido or "",
        provincia=lead.provincia or "Buenos Aires",
    )
    if geo.success:
        geocoder.apply_to_lead(lead, geo)


def _finish_ejecucion(state, ejecucion_id: str, result: PipelineResult) -> None:
    """Registra el fin de la ejecución en el state."""
    try:
        state.finish_ejecucion(
            ejecucion_id=ejecucion_id,
            discovered=result.discovered,
            scraped=result.fetched,
            enriched=result.enriched,
            validated=result.validated_email,
            errors=result.errors,
        )
    except Exception as exc:
        log.error("pipeline.finish_ejecucion.error", exc=str(exc))
