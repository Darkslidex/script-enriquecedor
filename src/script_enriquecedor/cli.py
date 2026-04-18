"""Entrypoint CLI — Script Enriquecedor v2.

Correr con:
    python cli.py
    uv run python cli.py

Flags opcionales:
    --verbose    Activa logs sin sanitización PII (solo debug local)
    --version    Muestra la versión y sale
"""

import typer
from rich.console import Console
from rich.panel import Panel

from . import __version__
from .core.config import get_settings
from .core.logger import configure_logging, get_logger
from .core.models import Vertical, VERTICAL_DISPLAY_NAMES
from .core.state import get_state
from .ui.menus import (
    MainMenuChoice,
    UploadOp,
    VerticalOp,
    activate_menu,
    confirm_activate,
    confirm_upload,
    main_menu,
    print_banner,
    scrape_size_menu,
    select_vertical,
    show_general_status,
    upload_menu,
    vertical_ops_menu,
)
from .ui.prompts import (
    confirm,
    print_error,
    print_info,
    print_success,
    print_warning,
    select,
)
from .ui.tables import (
    active_verticals,
    lotes_pendientes as table_lotes_pendientes,
)

app = typer.Typer(
    name="enriquecedor",
    help="Pipeline B2B multi-vertical — Techcam",
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()
log = get_logger("cli")


# ── Entrypoint ─────────────────────────────────────────────────────────────────

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Logs sin sanitización PII"),
    version: bool = typer.Option(False, "--version", help="Muestra la versión y sale"),
) -> None:
    """Script Enriquecedor v2 — Pipeline B2B multi-vertical."""
    if version:
        console.print(f"script-enriquecedor [bold cyan]v{__version__}[/bold cyan]")
        raise typer.Exit()

    if ctx.invoked_subcommand is not None:
        return

    configure_logging(verbose=verbose)
    _run_interactive()


def _run_interactive() -> None:
    """Loop principal del CLI interactivo."""
    try:
        while True:
            state = get_state()
            activos = state.get_active_verticals()

            lotes_count = sum(
                len(state.get_lotes_pendientes(v.vertical)) for v in activos
            )
            print_banner(active_count=len(activos), pending_batches=lotes_count)

            choice = main_menu()

            if choice == MainMenuChoice.TRABAJAR:
                _handle_trabajar()
            elif choice == MainMenuChoice.ACTIVAR:
                _handle_activar()
            elif choice == MainMenuChoice.ESTADO:
                _handle_estado()
            elif choice == MainMenuChoice.CONFIG:
                _handle_config()
            elif choice == MainMenuChoice.SALIR:
                console.print("\n[dim]Hasta luego.[/dim]\n")
                break

    except KeyboardInterrupt:
        console.print("\n\n[dim]Interrupted — saliendo.[/dim]\n")


# ── Nivel 2a — Trabajar con vertical ──────────────────────────────────────────

def _handle_trabajar() -> None:
    """Loop: selección de vertical → operaciones → volver."""
    while True:
        state = get_state()
        activos = state.get_active_verticals()

        if not activos:
            print_warning("No hay verticales activos. Activá uno primero.")
            return

        # Estadísticas para mostrar en el selector
        lotes_por_v = {
            v.vertical.value: len(state.get_lotes_pendientes(v.vertical))
            for v in activos
        }
        leads_por_v = {
            v.vertical.value: state.count_leads_pendientes(v.vertical)
            for v in activos
        }

        vertical = select_vertical(activos, lotes_por_v, leads_por_v)
        if vertical is None:
            return  # Volver al menú principal

        _handle_vertical_ops(vertical)


def _handle_vertical_ops(vertical: Vertical) -> None:
    """Loop: operaciones sobre el vertical seleccionado."""
    while True:
        state = get_state()
        pending_leads = state.count_leads_pendientes(vertical)

        op = vertical_ops_menu(vertical, pending_leads=pending_leads)
        if op is None:
            return  # Volver

        if op == VerticalOp.SCRAPEAR:
            _handle_scrape(vertical)
        elif op == VerticalOp.PHANTOMBUSTER:
            _handle_phantombuster(vertical)
        elif op == VerticalOp.VER_LOTES:
            _handle_ver_lotes(vertical)
        elif op == VerticalOp.SUBIR_VPS:
            _handle_upload(vertical)
        elif op == VerticalOp.VER_RESUMEN:
            _handle_ver_resumen(vertical)


# ── Scraping ───────────────────────────────────────────────────────────────────

def _handle_scrape(vertical: Vertical) -> None:
    """Menú de cantidad → lanza pipeline → post-scrape."""
    state = get_state()
    available = state.count_leads_pendientes(vertical)

    size = scrape_size_menu(available=available)
    if size is None:
        return

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    console.print()

    import asyncio
    from .pipeline import run, PipelineConfig

    config = PipelineConfig(vertical=vertical, limit=size, concurrency=3)
    try:
        result = asyncio.run(run(config))
        console.print()
        if result.saved:
            print_success(
                f"Lote guardado: [bold]{result.saved}[/bold] leads "
                f"([cyan]lote #{result.lote_id}[/cyan])"
            )
        else:
            print_warning("No se guardaron leads en este lote.")
        console.print(
            f"  Descubiertos: [bold]{result.discovered}[/bold]  "
            f"Enriquecidos: [bold]{result.enriched}[/bold]  "
            f"Errores: [red]{result.errors}[/red]"
        )
    except Exception as exc:
        print_error(f"Error en el pipeline: {exc}")
        log.error("cli.pipeline_error", exc=str(exc))

    # Post-scrape: qué hacer ahora
    _handle_post_scrape(vertical)


def _handle_post_scrape(vertical: Vertical) -> None:
    """Menú de seguimiento después de un scrape."""
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    console.print()
    choices = [
        "Scrapear otro lote",
        "Ir al menú de subir al VPS",
        "Volver al menú principal",
    ]
    idx = select(f"{nombre} — ¿qué querés hacer ahora?", choices, allow_back=False)
    if idx == 0:
        _handle_scrape(vertical)
    elif idx == 1:
        _handle_upload(vertical)
    # idx == 2 o None → volver (no hace nada, sale de la función)


def _handle_phantombuster(vertical: Vertical) -> None:
    """Importa CSVs de PhantomBuster y los pasa por el pipeline de enriquecimiento."""
    import asyncio
    from .pipeline import run_from_phantombuster, PipelineConfig
    from .core.config import get_settings

    settings = get_settings()
    console.print()
    print_info(
        f"Leyendo CSVs desde: [cyan]{settings.phantombuster_input_dir}[/cyan]\n"
        "  Copiá los archivos exportados por PhantomBuster a ese directorio antes de continuar."
    )
    console.print()

    if not confirm("¿Continuar con el import?", default=True):
        return

    config = PipelineConfig(vertical=vertical, concurrency=3)
    try:
        result = asyncio.run(run_from_phantombuster(config))
        console.print()
        if result.saved:
            print_success(
                f"Lote guardado: [bold]{result.saved}[/bold] leads "
                f"([cyan]lote #{result.lote_id}[/cyan])"
            )
        else:
            print_warning("No se encontraron CSVs en el directorio o no se pudo guardar.")
        console.print(
            f"  Importados: [bold]{result.discovered}[/bold]  "
            f"Apollo: [bold]{result.apollo_enriched}[/bold]  "
            f"Hunter/Snov: [bold]{result.validated_email + result.snov_enriched}[/bold]  "
            f"Errores: [red]{result.errors}[/red]"
        )
    except Exception as exc:
        print_error(f"Error en el import PhantomBuster: {exc}")
        log.error("cli.phantombuster_error", exc=str(exc))

    _handle_post_scrape(vertical)


# ── Lotes ──────────────────────────────────────────────────────────────────────

def _handle_ver_lotes(vertical: Vertical) -> None:
    """Muestra tabla de lotes pendientes del vertical."""
    state = get_state()
    lotes = state.get_lotes_pendientes(vertical)
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

    console.print()
    if not lotes:
        print_info(f"No hay lotes pendientes para [bold]{nombre}[/bold].")
        return

    console.print(table_lotes_pendientes(lotes))
    console.print(
        f"\n  [dim]Total:[/dim] [bold]{len(lotes)}[/bold] lotes · "
        f"[bold]{sum(l.leads_count for l in lotes)}[/bold] leads\n"
    )


def _handle_ver_resumen(vertical: Vertical) -> None:
    """Muestra calidad del lote consolidado + última ejecución."""
    from .storage.batch_manager import consolidate
    from .storage.csv_writer import read_csv
    from .storage.quality import summarize_batch
    from .ui.tables import batch_quality_summary, execution_history
    from .core.models import Lead

    state = get_state()
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    console.print()

    # Quality summary del lote consolidado
    lotes = state.get_lotes_pendientes(vertical)
    if lotes:
        try:
            path, total = consolidate(vertical)
            rows = read_csv(path)
            leads = [
                Lead(
                    nombre=r.get("nombre", ""),
                    vertical=vertical,
                    email=r.get("email") or None,  # type: ignore[arg-type]
                    telefono=r.get("telefono") or None,
                    sitio_web=r.get("sitio_web") or None,
                    partido=r.get("partido") or None,
                    localidad=r.get("localidad") or None,
                    latitud=float(r["latitud"]) if r.get("latitud") else None,
                    longitud=float(r["longitud"]) if r.get("longitud") else None,
                    email_validado=(r.get("email_validado") == "true"),
                )
                for r in rows if r.get("nombre")
            ]
            summary = summarize_batch(leads, vertical)
            console.print(batch_quality_summary(summary))
            console.print()
        except Exception as exc:
            print_warning(f"No se pudo calcular calidad del lote: {exc}")

    # Última ejecución
    last = state.get_last_ejecucion(vertical)
    if last:
        console.print(execution_history([last]))
    else:
        print_info(f"No hay ejecuciones registradas para [bold]{nombre}[/bold].")
    console.print()


# ── Upload ─────────────────────────────────────────────────────────────────────

def _handle_upload(vertical: Vertical) -> None:
    """Loop: menú de upload → preview / exportar / subir / descartar."""
    while True:
        state = get_state()
        lotes = state.get_lotes_pendientes(vertical)
        nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

        if not lotes:
            print_info(f"No hay lotes pendientes para [bold]{nombre}[/bold].")
            return

        op = upload_menu(lotes)
        if op is None:
            return

        if op == UploadOp.PREVIEW:
            _upload_preview(vertical, lotes)
        elif op == UploadOp.VER_DUPLICADOS:
            _upload_ver_duplicados()
        elif op == UploadOp.EXPORTAR_CSV:
            _upload_exportar(vertical, lotes)
        elif op == UploadOp.SUBIR:
            subido = _upload_ejecutar(vertical, lotes)
            if subido:
                return  # Salir del loop de upload después de subir
        elif op == UploadOp.DESCARTAR:
            _upload_descartar(lotes)


def _upload_preview(vertical: Vertical, lotes) -> None:
    """Preview de leads del lote consolidado."""
    from .storage.batch_manager import consolidate
    from .storage.csv_writer import read_csv
    from .ui.tables import leads_preview
    from .core.models import Lead

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    try:
        consolidated_path, total = consolidate(vertical)
        rows = read_csv(consolidated_path)
        # Convertir a leads mínimos para preview
        preview_leads = [
            Lead(
                nombre=r.get("nombre", ""),
                vertical=vertical,
                email=r.get("email") or None,  # type: ignore[arg-type]
                telefono=r.get("telefono") or None,
                partido=r.get("partido") or None,
                email_validado=(r.get("email_validado") == "true"),
            )
            for r in rows[:10]
        ]
        console.print()
        console.print(leads_preview(preview_leads, max_rows=10))
        console.print(f"\n  [dim]Total en lote consolidado:[/dim] [bold]{total}[/bold] leads\n")
    except Exception as exc:
        print_error(f"No se pudo cargar el preview: {exc}")


def _upload_ver_duplicados() -> None:
    """Detección de duplicados fuzzy en el lote pendiente."""
    from .storage.csv_writer import read_csv
    from .core.dedup import find_fuzzy_matches
    from .core.models import Lead
    from .ui.tables import fuzzy_duplicates_table
    from pathlib import Path

    # Lee el CSV consolidado más reciente en data/enriched/
    csv_paths = list(Path("data/enriched").rglob("consolidated.csv"))
    if not csv_paths:
        print_info("No hay CSVs consolidados aún. Primero scrapea y revisá los lotes.")
        return

    # Usa el más reciente
    csv_path = max(csv_paths, key=lambda p: p.stat().st_mtime)
    rows = read_csv(csv_path)
    leads = [
        Lead(nombre=r.get("nombre", ""), vertical=Vertical.BARRIOS_PRIVADOS,
             partido=r.get("partido") or None)
        for r in rows if r.get("nombre")
    ]

    matches = find_fuzzy_matches(leads, threshold=85)
    console.print()
    if not matches:
        print_success("No se detectaron duplicados potenciales.")
    else:
        console.print(fuzzy_duplicates_table(matches))
        console.print(
            f"\n  [dim]Threshold:[/dim] 85%  "
            f"[dim]Revisá manualmente antes de subir.[/dim]\n"
        )


def _upload_exportar(vertical: Vertical, lotes) -> None:
    """Consolida y exporta CSV a data/enriched/<vertical>/consolidated.csv."""
    from .storage.batch_manager import consolidate

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    try:
        path, total = consolidate(vertical)
        console.print()
        print_success(
            f"CSV exportado: [bold cyan]{path}[/bold cyan]\n"
            f"  [dim]Total leads:[/dim] [bold]{total}[/bold]"
        )
    except Exception as exc:
        print_error(f"Error al exportar: {exc}")


def _upload_ejecutar(vertical: Vertical, lotes) -> bool:
    """Pide confirmación y ejecuta rsync + seed.ts remoto.

    Returns True si se subió, False si el usuario canceló.
    """
    total_leads = sum(l.leads_count for l in lotes)

    confirmado = confirm_upload(
        vertical=vertical,
        leads_count=total_leads,
        lote_names=[l.id for l in lotes],
    )

    if not confirmado:
        print_warning("Upload cancelado.")
        return False

    from .storage.batch_manager import consolidate, mark_lote_uploaded
    from .storage.vps_uploader import upload

    try:
        consolidated_path, total = consolidate(vertical)
    except Exception as exc:
        print_error(f"Error al consolidar lotes: {exc}")
        return False

    result = upload(vertical, consolidated_path)

    if result.success:
        for lote in lotes:
            mark_lote_uploaded(lote.id)
        print_success(
            f"[bold]{total}[/bold] leads subidos al VPS correctamente."
        )
        return True
    else:
        print_error(f"Error al subir: {result.error}")
        if result.rsync_output:
            console.print(f"[dim]{result.rsync_output[:300]}[/dim]")
        return False


def _upload_descartar(lotes) -> None:
    """Permite descartar lotes pendientes individualmente."""
    state = get_state()
    choices = [f"{l.id[:8]}  ({l.leads_count} leads · {l.creado_en.strftime('%Y-%m-%d %H:%M')})" for l in lotes]
    idx = select("¿Qué lote querés descartar?", choices, allow_back=True)
    if idx is None:
        return

    lote = lotes[idx]
    if confirm(f"¿Descartar lote [bold]{lote.id[:8]}[/bold] ({lote.leads_count} leads)?", default=False):
        state.update_lote(lote.id, "descartado")
        print_success(f"Lote {lote.id[:8]} marcado como descartado.")


# ── Nivel 2b — Activar vertical nuevo ─────────────────────────────────────────

def _handle_activar() -> None:
    """Flujo para activar un vertical inactivo."""
    state = get_state()
    activos_vals = {v.vertical for v in state.get_active_verticals()}
    inactivos = [v for v in Vertical if v not in activos_vals]

    if not inactivos:
        print_success("Todos los verticales ya están activos.")
        return

    vertical = activate_menu(inactivos)
    if vertical is None:
        return

    # Obtener campos de metadata del schema
    from .core.metadata_schemas import get_metadata_schema
    schema = get_metadata_schema(vertical)
    metadata_fields = list(schema.model_fields.keys())

    if not confirm_activate(vertical, metadata_fields):
        print_info("Activación cancelada.")
        return

    from .ui.progress import make_spinner
    from .enrichment.prompt_generator import ensure_prompt
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

    with make_spinner() as spinner:
        task = spinner.add_task("Generando prompt LLM...", total=None)
        prompt_file = ensure_prompt(vertical)

        spinner.update(task, description="Guardando estado...")
        schema_path = f"src/script_enriquecedor/core/metadata_schemas/{vertical.value}.py"
        state.activate_vertical(vertical, str(prompt_file), schema_path)

    console.print()
    print_success(f"[bold]{nombre}[/bold] activado.")
    console.print()

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    if confirm(f"¿Querés hacer una prueba con 5 leads ahora?", default=True):
        _handle_vertical_ops(vertical)


# ── Estado general ──────────────────────────────────────────────────────────────

def _handle_estado() -> None:
    """Muestra tabla de estado general de todos los verticales activos."""
    state = get_state()
    activos = state.get_active_verticals()

    lotes_por_v = {
        v.vertical.value: len(state.get_lotes_pendientes(v.vertical))
        for v in activos
    }
    leads_por_v = {
        v.vertical.value: state.count_leads_pendientes(v.vertical)
        for v in activos
    }

    show_general_status(activos, lotes_por_v, leads_por_v)

    settings = get_settings()
    def _api_status(ok: bool) -> str:
        return "[green]configurado[/green]" if ok else "[dim]no configurado[/dim]"

    console.print(
        f"  [dim]Modelo LLM:[/dim]      [cyan]{settings.litellm_model}[/cyan]\n"
        f"  [dim]Hunter.io:[/dim]       {_api_status(settings.has_hunter)}\n"
        f"  [dim]Snov.io:[/dim]         {_api_status(settings.has_snov)}\n"
        f"  [dim]Apollo.io:[/dim]       {_api_status(settings.has_apollo)}\n"
        f"  [dim]Google Places:[/dim]   {_api_status(settings.has_google_places)}\n"
        f"  [dim]VPS alias:[/dim]       [cyan]{settings.vps_ssh_alias}[/cyan]\n"
    )


# ── Configuración ──────────────────────────────────────────────────────────────

def _handle_config() -> None:
    """Muestra configuración actual leída del .env."""
    settings = get_settings()

    console.print()
    console.print(
        Panel(
            f"[bold]Modelo LLM:[/bold]     [cyan]{settings.litellm_model}[/cyan]\n"
            f"[bold]Rate limit:[/bold]     [cyan]{settings.rate_limit_seconds}s[/cyan] por dominio\n"
            f"[bold]Verbose logs:[/bold]   [cyan]{settings.verbose_logs}[/cyan]\n"
            f"[bold]Data dir:[/bold]       [cyan]{settings.data_dir}[/cyan]\n"
            f"[bold]VPS alias:[/bold]      [cyan]{settings.vps_ssh_alias}[/cyan]\n"
            f"[bold]VPS path:[/bold]       [cyan]{settings.vps_app_path}[/cyan]\n\n"
            f"[dim]Para cambiar configuración: editá el archivo .env[/dim]",
            title="[bold]Configuración actual[/bold]",
            border_style="blue",
        )
    )
    console.print()


# ── Subcomandos adicionales ────────────────────────────────────────────────────

@app.command("version")
def cmd_version() -> None:
    """Muestra la versión del script."""
    console.print(f"script-enriquecedor [bold cyan]v{__version__}[/bold cyan]")


@app.command("status")
def cmd_status() -> None:
    """Muestra el estado actual sin entrar al menú interactivo."""
    configure_logging()
    _handle_estado()


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()
