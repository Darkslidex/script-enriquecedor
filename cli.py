"""Entrypoint CLI — Script Enriquecedor v2.

Correr con:
    python cli.py
    uv run python cli.py

Flags opcionales:
    --verbose    Activa logs sin sanitización PII (solo debug local)
    --version    Muestra la versión y sale
"""

import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

# Asegurar que src/ esté en el path cuando se corre desde la raíz del repo
sys.path.insert(0, str(Path(__file__).parent / "src"))

from script_enriquecedor import __version__
from script_enriquecedor.core.config import get_settings
from script_enriquecedor.core.logger import configure_logging, get_logger
from script_enriquecedor.core.models import Vertical, VERTICAL_DISPLAY_NAMES
from script_enriquecedor.core.state import get_state
from script_enriquecedor.ui.menus import (
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
from script_enriquecedor.ui.prompts import (
    confirm,
    print_error,
    print_info,
    print_success,
    print_warning,
    select,
)
from script_enriquecedor.ui.tables import (
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

    # ── Placeholder hasta Fase 1 paso 10 ──────────────────────────────────
    console.print(
        Panel(
            f"[bold yellow]Pipeline no implementado aún[/bold yellow]\n\n"
            f"Vertical: [cyan]{nombre}[/cyan]\n"
            f"Leads a procesar: [bold]{size}[/bold]\n\n"
            f"[dim]Disponible en Fase 1 paso 10 (pipeline.py)[/dim]",
            title="[bold]Scrapear[/bold]",
            border_style="yellow",
        )
    )
    # ── Fin placeholder ────────────────────────────────────────────────────

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
    """Muestra la última ejecución registrada."""
    state = get_state()
    last = state.get_last_ejecucion(vertical)
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

    console.print()
    if last is None:
        print_info(f"No hay ejecuciones registradas para [bold]{nombre}[/bold].")
        return

    from script_enriquecedor.ui.tables import execution_history
    console.print(execution_history([last]))
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
    """Placeholder: preview de leads del lote consolidado."""
    console.print(
        Panel(
            "[bold yellow]Preview no implementado aún[/bold yellow]\n\n"
            "[dim]Disponible en Fase 1 paso 9 (batch_manager.py)[/dim]",
            border_style="yellow",
        )
    )


def _upload_ver_duplicados() -> None:
    """Placeholder: detección de duplicados vs producción."""
    console.print(
        Panel(
            "[bold yellow]Detección de duplicados no implementada aún[/bold yellow]\n\n"
            "[dim]Disponible en Fase 3 (dedup avanzado)[/dim]",
            border_style="yellow",
        )
    )


def _upload_exportar(vertical: Vertical, lotes) -> None:
    """Placeholder: exportar CSV consolidado sin subir."""
    console.print(
        Panel(
            "[bold yellow]Exportar CSV no implementado aún[/bold yellow]\n\n"
            "[dim]Disponible en Fase 1 paso 9 (batch_manager.py)[/dim]",
            border_style="yellow",
        )
    )


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

    # ── Placeholder hasta Fase 1 paso 9 ───────────────────────────────────
    console.print(
        Panel(
            "[bold yellow]Upload no implementado aún[/bold yellow]\n\n"
            "[dim]Disponible en Fase 1 paso 9 (vps_uploader.py)[/dim]",
            border_style="yellow",
        )
    )
    return False
    # ── Fin placeholder ────────────────────────────────────────────────────


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
    from script_enriquecedor.core.metadata_schemas import get_metadata_schema
    schema = get_metadata_schema(vertical)
    metadata_fields = list(schema.model_fields.keys())

    if not confirm_activate(vertical, metadata_fields):
        print_info("Activación cancelada.")
        return

    # ── Activar (Fase 2: prompt_generator.py generará el prompt LLM) ──────
    from script_enriquecedor.ui.progress import make_spinner
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

    with make_spinner() as spinner:
        task = spinner.add_task("Generando prompt LLM...", total=None)

        # Placeholder: en Fase 2 llamará prompt_generator.generate_prompt(vertical)
        prompt_path = f"src/script_enriquecedor/enrichment/prompts/{vertical.value}.md"
        schema_path = f"src/script_enriquecedor/core/metadata_schemas/{vertical.value}.py"

        spinner.update(task, description="Guardando estado...")
        state.activate_vertical(vertical, prompt_path, schema_path)

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
    console.print(
        f"  [dim]Modelo LLM:[/dim]      [cyan]{settings.litellm_model}[/cyan]\n"
        f"  [dim]Hunter.io:[/dim]       {'[green]configurado[/green]' if settings.has_hunter else '[dim]no configurado[/dim]'}\n"
        f"  [dim]Google Places:[/dim]   {'[green]configurado[/green]' if settings.has_google_places else '[dim]no configurado[/dim]'}\n"
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
