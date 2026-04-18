"""Orquestador end-to-end del pipeline.

Flujo principal:
  discovery → fetcher → LLM → waterfall enriquecimiento → geocoder → batch save

Waterfall de enriquecimiento (Fase 4):
  Hunter verify → (score < 70 o sin email) → Snov domain lookup → Hunter verify
  + Apollo decisor search cuando no hay contacto_nombre

Entry points alternativos:
  run()                  → flujo normal (discovery + scraping + LLM)
  run_from_phantombuster() → parte de leads ya importados (sin scraping ni LLM)
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
from .enrichment.apollo import ApolloSearcher, get_apollo_searcher
from .enrichment.geocoder import Geocoder
from .enrichment.hunter import EmailValidator, get_email_validator
from .enrichment.llm_client import get_llm_client
from .enrichment.snov import SnovClient, get_snov_client
from .importers.phantombuster import get_phantombuster_importer
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
    snov_enriched: int = 0
    apollo_enriched: int = 0
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

        # ── Fase 4: Waterfall email enrichment (Hunter → Snov → Hunter) ───────
        if not config.skip_email_validation:
            validator = get_email_validator()
            snov = get_snov_client()
            waterfall_tasks = [
                _waterfall_email(validator, snov, lead) for lead in leads
            ]
            waterfall_results = await asyncio.gather(*waterfall_tasks, return_exceptions=True)
            for res in waterfall_results:
                if isinstance(res, Exception):
                    result.errors += 1
                    progress.add_error()
                else:
                    wresult: _WaterfallResult = res
                    if wresult.hunter_verified:
                        result.validated_email += 1
                        progress.advance_validated()
                    if wresult.snov_used:
                        result.snov_enriched += 1

        # ── Fase 4.5: Apollo decisor search ────────────────────────────────
        if not config.skip_email_validation and settings.has_apollo:
            apollo = get_apollo_searcher()
            leads_sin_decisor = [l for l in leads if not l.contacto_nombre]
            apollo_tasks = [_enrich_decisor(apollo, lead) for lead in leads_sin_decisor]
            apollo_results = await asyncio.gather(*apollo_tasks, return_exceptions=True)
            for res in apollo_results:
                if isinstance(res, Exception):
                    result.errors += 1
                elif res:
                    result.apollo_enriched += 1

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
        validated_email=result.validated_email,
        snov_enriched=result.snov_enriched,
        apollo_enriched=result.apollo_enriched,
        saved=result.saved,
        errors=result.errors,
    )
    return result


async def run_from_phantombuster(config: PipelineConfig) -> PipelineResult:
    """Ejecuta el pipeline partiendo de CSVs de PhantomBuster.

    Salta discovery, fetching y LLM. Los leads ya vienen con
    contacto_nombre y contacto_cargo; el pipeline aplica
    Apollo (para email) → Hunter/Snov (para validar) → geocoding → save.

    Args:
        config: PipelineConfig con vertical (usualmente EMPRESAS) y flags.

    Returns: PipelineResult con métricas.
    """
    settings = get_settings()
    state = get_state()
    result = PipelineResult(vertical=config.vertical)

    ejecucion = state.create_ejecucion(config.vertical)
    ejecucion_id = ejecucion.id  # type: ignore[attr-defined]

    # ── Carga desde PhantomBuster ──────────────────────────────────────────
    importer = get_phantombuster_importer()
    leads = importer.load_all()

    if not leads:
        log.warning("phantombuster.no_leads")
        _finish_ejecucion(state, ejecucion_id, result)
        return result

    # Respetar limit si está configurado
    if config.limit and len(leads) > config.limit:
        leads = leads[: config.limit]

    result.discovered = len(leads)
    log.info("phantombuster.loaded", count=result.discovered)

    with PipelineProgress(
        total=len(leads),
        lote_name=f"phantombuster_{config.vertical.value}",
        vertical=config.vertical,
    ) as progress:

        # ── Apollo primero: buscar email del decisor ya conocido ──────────
        # (PhantomBuster nos da nombre+cargo pero no email)
        if settings.has_apollo:
            apollo = get_apollo_searcher()
            apollo_tasks = [_enrich_decisor(apollo, lead) for lead in leads]
            apollo_results = await asyncio.gather(*apollo_tasks, return_exceptions=True)
            for res in apollo_results:
                if isinstance(res, Exception):
                    result.errors += 1
                elif res:
                    result.apollo_enriched += 1

        # ── Waterfall Hunter → Snov (validar/buscar emails) ───────────────
        if not config.skip_email_validation:
            validator = get_email_validator()
            snov = get_snov_client()
            waterfall_tasks = [_waterfall_email(validator, snov, lead) for lead in leads]
            waterfall_results = await asyncio.gather(*waterfall_tasks, return_exceptions=True)
            for res in waterfall_results:
                if isinstance(res, Exception):
                    result.errors += 1
                    progress.add_error()
                else:
                    wresult: _WaterfallResult = res
                    if wresult.hunter_verified:
                        result.validated_email += 1
                        progress.advance_validated()
                    if wresult.snov_used:
                        result.snov_enriched += 1

        result.enriched = len([l for l in leads if l.email or l.contacto_nombre])

        # ── Geocoding ─────────────────────────────────────────────────────
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

        # ── Save ──────────────────────────────────────────────────────────
        if not config.dry_run and leads:
            lote_id = save_lote(config.vertical, leads)
            result.lote_id = lote_id
            result.saved = len(leads)
            log.info("phantombuster.batch_saved", lote_id=lote_id, leads=result.saved)

        result.leads = leads

    _finish_ejecucion(state, ejecucion_id, result)

    log.info(
        "phantombuster.done",
        discovered=result.discovered,
        enriched=result.enriched,
        validated_email=result.validated_email,
        snov_enriched=result.snov_enriched,
        apollo_enriched=result.apollo_enriched,
        saved=result.saved,
        errors=result.errors,
    )
    return result


# ── Helpers ────────────────────────────────────────────────────────────────────

_HUNTER_SCORE_MIN = 70  # score por debajo del cual activamos Snov como fallback


@dataclass
class _WaterfallResult:
    hunter_verified: bool = False
    snov_used: bool = False


async def _waterfall_email(
    validator: EmailValidator,
    snov: SnovClient,
    lead: Lead,
) -> _WaterfallResult:
    """Waterfall Hunter → Snov → Hunter para un lead in-place.

    Lógica según sección 7 del handoff:
    - Lead tiene email → Hunter verify
        score >= 70 → ✅ done
        score < 70  → Snov por dominio → Hunter verify primer resultado
    - Lead sin email → Snov por dominio
        encontró    → Hunter verify
        no encontró → email_validado=False (no modificar)
    """
    res = _WaterfallResult()
    settings = get_settings()

    # Paso 1: si hay email, intentar Hunter primero
    if lead.email and settings.has_hunter:
        verification = await validator.verify(lead.email)
        validator.apply_to_lead(lead, verification)
        if not verification.error and verification.score is not None:
            if verification.score >= _HUNTER_SCORE_MIN:
                res.hunter_verified = True
                return res
            # Score bajo → fallback a Snov

    # Paso 2: Snov por dominio (fallback o primer intento si no había email)
    domain = _extract_domain(lead)
    if domain and settings.has_snov:
        snov_result = await snov.find_emails(domain)
        res.snov_used = True
        if snov_result.found:
            # Asignar el primer email de Snov si el lead no tenía o era de baja calidad
            candidate = snov_result.emails[0]
            if not lead.email:
                lead.email = candidate
            elif lead.email_2 is None:
                lead.email_2 = candidate

            # Re-verificar con Hunter si está disponible
            if settings.has_hunter and candidate:
                re_verify = await validator.verify(candidate)
                validator.apply_to_lead(lead, re_verify)
                if not re_verify.error:
                    res.hunter_verified = True
        else:
            # Snov no encontró nada — marcar explícitamente
            if not lead.email:
                lead.email_validado = False

    return res


def _extract_domain(lead: Lead) -> str | None:
    """Extrae el dominio del sitio_web del lead para búsqueda en Snov."""
    if not lead.sitio_web:
        return None
    url = str(lead.sitio_web)
    return url.removeprefix("https://").removeprefix("http://").split("/")[0]


async def _enrich_decisor(searcher: ApolloSearcher, lead: Lead) -> bool:
    """Busca decisores en Apollo y aplica el mejor al lead. Retorna True si encontró."""
    if not lead.nombre:
        return False
    result = await searcher.find_decision_makers(lead.nombre)
    if result.found:
        searcher.apply_to_lead(lead, result)
        return True
    return False


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
