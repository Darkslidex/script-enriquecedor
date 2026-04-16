"""Menús Rich anidados del pipeline.

Estructura de niveles:
  1. main_menu()            → MainMenuChoice
  2. select_vertical()      → Vertical | None
  3. vertical_ops_menu()    → VerticalOp | None
  4a. scrape_size_menu()    → int | None (cantidad de leads)
  4b. upload_menu()         → UploadOp | None
  2b. activate_menu()       → Vertical | None (verticales disponibles)

Ningún menú ejecuta lógica de negocio — solo recopilan la elección del usuario.
El dispatch se hace en cli.py.
"""

from enum import Enum

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text

from ..core.models import Vertical, VERTICAL_DISPLAY_NAMES
from ..core.state import Lote, VerticalActivo
from .prompts import (
    confirm_exact,
    print_info,
    print_section,
    print_warning,
    select,
)

console = Console()

# ── Versión ──────────────────────────────────────────��───────────────────────

APP_VERSION = "2.0.0"

# Verticales recomendados (fuente oficial estructurada) vs dorks
_VERTICALES_RECOMENDADOS = {
    Vertical.PARQUES_INDUSTRIALES: "CAIP (caip.org.ar)",
    Vertical.DROGUERIAS: "ANMAT (registro oficial)",
    Vertical.HOTELES: "Google Places API",
    Vertical.CLINICAS: "SSSALUD (prestadores)",
    Vertical.LOGISTICAS: "ARLOG + Dorks",
}

_VERTICALES_DORKS = {
    Vertical.UNIVERSIDADES,
    Vertical.ENTES_ESTATALES,
    Vertical.CONSULADOS,
    Vertical.EMBAJADAS,
    Vertical.DEPOSITOS_FISCALES,
    Vertical.EMPRESAS,
    Vertical.PLANTAS_INDUSTRIALES,
    Vertical.TERMINALES_PORTUARIAS,
    Vertical.AERONAUTICAS,
}


# ── Enums de opciones ─────────────────────────────────────────────────────────

class MainMenuChoice(str, Enum):
    TRABAJAR = "trabajar"
    ACTIVAR = "activar"
    ESTADO = "estado"
    CONFIG = "config"
    SALIR = "salir"


class VerticalOp(str, Enum):
    SCRAPEAR = "scrapear"
    VER_LOTES = "ver_lotes"
    SUBIR_VPS = "subir_vps"
    VER_RESUMEN = "ver_resumen"


class UploadOp(str, Enum):
    PREVIEW = "preview"
    VER_DUPLICADOS = "ver_duplicados"
    EXPORTAR_CSV = "exportar_csv"
    SUBIR = "subir"
    DESCARTAR = "descartar"


# ── Banner ─────────��──────────────────────────────────���───────────────────────

def print_banner(active_count: int = 1, pending_batches: int = 0) -> None:
    """Imprime el banner principal del CLI."""
    title = Text()
    title.append(f"  SCRIPT ENRIQUECEDOR v{APP_VERSION}", style="bold white")
    title.append("  —  Techcam\n", style="dim")
    title.append("  Pipeline B2B multi-vertical", style="cyan")

    panel = Panel(
        title,
        border_style="bold blue",
        padding=(0, 1),
    )
    console.print()
    console.print(panel)
    console.print(
        f"  [dim]Verticales activos:[/dim] [bold]{active_count}[/bold]   "
        f"[dim]Lotes pendientes:[/dim] [bold]{pending_batches}[/bold]"
    )
    console.print()


# ── Nivel 1 — Menú principal ──────────���───────────────────────────────────────

def main_menu() -> MainMenuChoice:
    """Muestra el menú principal y retorna la opción elegida."""
    choices = [
        "Trabajar con un vertical existente",
        "Activar un vertical nuevo",
        "Ver estado general",
        "Configuración",
        "Salir",
    ]
    idx = select("¿Qué querés hacer?", choices, allow_back=False)
    mapping = {
        0: MainMenuChoice.TRABAJAR,
        1: MainMenuChoice.ACTIVAR,
        2: MainMenuChoice.ESTADO,
        3: MainMenuChoice.CONFIG,
        4: MainMenuChoice.SALIR,
    }
    # idx es None solo con Ctrl+C → salir
    return mapping.get(idx, MainMenuChoice.SALIR)  # type: ignore[arg-type]


# ── Nivel 2a — Selección de vertical activo ────────��──────────────────────────

def select_vertical(
    verticales: list[VerticalActivo],
    lotes_por_vertical: dict[str, int] | None = None,
    leads_por_vertical: dict[str, int] | None = None,
) -> Vertical | None:
    """Lista los verticales activos para trabajar con uno.

    Returns:
        Vertical — el elegido
        None     — usuario eligió Volver
    """
    lotes_por_vertical = lotes_por_vertical or {}
    leads_por_vertical = leads_por_vertical or {}

    print_section("Verticales activos")

    choices = []
    hints = []
    verticals_list = []

    for v in verticales:
        nombre = VERTICAL_DISPLAY_NAMES.get(v.vertical, v.vertical.value)
        lotes = lotes_por_vertical.get(v.vertical.value, 0)
        leads = leads_por_vertical.get(v.vertical.value, 0)

        choices.append(nombre)
        hint_parts = []
        if lotes:
            hint_parts.append(f"{lotes} lote{'s' if lotes != 1 else ''} pendiente{'s' if lotes != 1 else ''}")
        if leads:
            hint_parts.append(f"{leads} leads sin subir")
        hints.append(" · ".join(hint_parts))
        verticals_list.append(v.vertical)

    idx = select("", choices, hints=hints, allow_back=True)
    if idx is None:
        return None
    return verticals_list[idx]


# ── Nivel 3 — Operaciones sobre un vertical ───────────���───────────────────────

def vertical_ops_menu(vertical: Vertical, pending_leads: int = 0) -> VerticalOp | None:
    """Menú de operaciones para el vertical seleccionado.

    Returns:
        VerticalOp — operación elegida
        None       — usuario eligió Volver
    """
    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    print_section(f"{nombre} — ¿qué operación?")

    hints = [
        "",
        f"{pending_leads} leads acumulados" if pending_leads else "",
        "requiere confirmación",
        "",
    ]
    choices = [
        "Scrapear nuevos leads",
        "Ver lotes pendientes de upload",
        "Subir al VPS",
        "Ver resumen del último scrapeo",
    ]

    idx = select("", choices, hints=hints, allow_back=True)
    if idx is None:
        return None
    mapping = {
        0: VerticalOp.SCRAPEAR,
        1: VerticalOp.VER_LOTES,
        2: VerticalOp.SUBIR_VPS,
        3: VerticalOp.VER_RESUMEN,
    }
    return mapping[idx]


# ── Nivel 4a — Tamaño de lote de scraping ───────��────────────────────────────

# Opciones predefinidas: (label, cantidad, hint_tiempo)
_SCRAPE_PRESETS = [
    ("10   leads", 10,  "~2 min"),
    ("50   leads", 50,  "~8 min"),
    ("100  leads", 100, "~15 min"),
    ("Cantidad personalizada", -1, ""),
]


def scrape_size_menu(available: int = 0) -> int | None:
    """Menú de cantidad de leads a scrapear.

    Returns:
        int  — cantidad elegida
        None — usuario eligió Volver
    """
    print_section("¿Cuántos leads querés procesar?")

    choices = [label for label, _, _ in _SCRAPE_PRESETS]
    hints = [hint for _, _, hint in _SCRAPE_PRESETS]

    # Agregar opción "Todos" si hay disponibles
    if available > 0:
        todos_hint = f"~{_estimate_minutes(available)} min"
        choices.insert(3, f"Todos los pendientes  ({available} disponibles)")
        hints.insert(3, todos_hint)

    idx = select("", choices, hints=hints, allow_back=True)
    if idx is None:
        return None

    # Ajustar índice si se insertó "Todos"
    presets_adjusted = list(_SCRAPE_PRESETS)
    if available > 0:
        presets_adjusted.insert(3, (f"Todos ({available})", available, ""))

    _, cantidad, _ = presets_adjusted[idx]

    if cantidad == -1:
        # Personalizado
        from .prompts import ask_int
        return ask_int("¿Cuántos leads?", min_val=1, max_val=9999)

    return cantidad


def _estimate_minutes(n: int) -> int:
    """Estimación muy gruesa: ~9 segundos por lead."""
    return max(1, n * 9 // 60)


# ── Nivel 4b — Menú de upload al VPS ─────────────────────────────────────────

def upload_menu(lotes: list[Lote]) -> UploadOp | None:
    """Menú de preview y confirmación de upload.

    Returns:
        UploadOp — operación elegida
        None     — usuario eligió Volver
    """
    total_leads = sum(l.leads_count for l in lotes)
    print_section("Subir al VPS")
    print_info(f"Lotes pendientes: [bold]{len(lotes)}[/bold]  ·  Total leads: [bold]{total_leads}[/bold]")

    choices = [
        "Ver preview (primeros 10 leads)",
        "Ver potenciales duplicados",
        "Exportar CSV consolidado (sin subir)",
        "Subir todo al VPS ahora",
        "Descartar uno o más lotes",
    ]

    idx = select("", choices, allow_back=True)
    if idx is None:
        return None
    mapping = {
        0: UploadOp.PREVIEW,
        1: UploadOp.VER_DUPLICADOS,
        2: UploadOp.EXPORTAR_CSV,
        3: UploadOp.SUBIR,
        4: UploadOp.DESCARTAR,
    }
    return mapping[idx]


def confirm_upload(vertical: Vertical, leads_count: int, lote_names: list[str]) -> bool:
    """Muestra los comandos que se van a ejecutar y pide confirmación exacta 'SUBIR'.

    Returns:
        True  — usuario escribió "SUBIR"
        False — canceló
    """
    from ..core.config import get_settings
    settings = get_settings()

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    vertical_slug = vertical.value

    console.print()
    print_warning("Acción irreversible")
    console.print(
        f"\nVas a subir [bold]{leads_count}[/bold] leads de "
        f"[bold cyan]{nombre}[/bold cyan] a la base de datos del VPS.\n"
    )

    rsync_cmd = (
        f"rsync -az data/enriched/{vertical_slug}/consolidated.csv \\\n"
        f"  → {settings.vps_ssh_alias}:{settings.vps_app_path}/data/"
    )
    seed_cmd = (
        f"ssh {settings.vps_ssh_alias} \"docker run --rm --network coolify \\\n"
        f"  -e DATABASE_URL='...' \\\n"
        f"  -e CSV_PATH=/data/consolidated.csv \\\n"
        f"  -v {settings.vps_app_path}:/app \\\n"
        f"  -v {settings.vps_app_path}/data:/data \\\n"
        f"  -w /app node:20-alpine \\\n"
        f"  sh -c 'npm ci --silent && npx prisma generate && npx tsx prisma/seed.ts'\""
    )

    console.print("[dim]Comandos que se ejecutarán:[/dim]")
    console.print(Panel(f"[cyan]1.[/cyan] {rsync_cmd}\n\n[cyan]2.[/cyan] {seed_cmd}", border_style="dim"))
    console.print("[dim]Los leads se pueden editar desde el dashboard después.[/dim]\n")

    return confirm_exact(
        "Para confirmar, escribí SUBIR (en mayúsculas):",
        expected="SUBIR",
    )


# ── Nivel 2b — Activar vertical nuevo ────────────────────────────────────────

def activate_menu(inactivos: list[Vertical]) -> Vertical | None:
    """Menú para activar un vertical nuevo desde los 14 inactivos.

    Separa entre recomendados (fuente oficial) y los que requieren Dorks.

    Returns:
        Vertical — el elegido para activar
        None     — usuario eligió Volver
    """
    print_section("Activar vertical nuevo")

    recomendados = [v for v in inactivos if v in _VERTICALES_RECOMENDADOS]
    dorks = [v for v in inactivos if v in _VERTICALES_DORKS]

    choices = []
    hints = []
    verticals_list = []

    if recomendados:
        console.print("\n  [bold green]Recomendados[/bold green] [dim](fuente oficial disponible)[/dim]")
        for v in recomendados:
            nombre = VERTICAL_DISPLAY_NAMES.get(v, v.value)
            fuente = _VERTICALES_RECOMENDADOS.get(v, "")
            choices.append(nombre)
            hints.append(f"fuente: {fuente}")
            verticals_list.append(v)

    if dorks:
        console.print("\n  [bold yellow]Requieren Google Dorks[/bold yellow] [dim](más lentos, menos precisos)[/dim]")
        for v in dorks:
            nombre = VERTICAL_DISPLAY_NAMES.get(v, v.value)
            choices.append(nombre)
            hints.append("fuente: Dorks")
            verticals_list.append(v)

    if not choices:
        console.print("[green]Todos los verticales ya están activos.[/green]")
        return None

    idx = select("", choices, hints=hints, allow_back=True)
    if idx is None:
        return None
    return verticals_list[idx]


def confirm_activate(vertical: Vertical, metadata_fields: list[str]) -> bool:
    """Muestra detalles del vertical a activar y pide confirmación s/n."""
    from ..core.config import get_settings
    from .prompts import confirm

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    fuente = _VERTICALES_RECOMENDADOS.get(vertical, "Google Dorks")

    console.print()
    console.print(f"[bold]Activar:[/bold] [cyan]{nombre}[/cyan]\n")
    console.print(f"  [dim]Fuente descubrimiento:[/dim]  {fuente}")
    console.print(f"  [dim]Campos metadata:[/dim]        {', '.join(metadata_fields[:4])}")
    if len(metadata_fields) > 4:
        console.print(f"                           {', '.join(metadata_fields[4:])}")
    console.print()
    console.print("  El script va a generar automáticamente:")
    console.print("  [green]✓[/green] Prompt LLM optimizado para este vertical")
    console.print("  [green]✓[/green] Schema Pydantic de validación")
    console.print("  [green]✓[/green] Estrategia de descubrimiento configurada")
    console.print()

    return confirm("¿Activar ahora?", default=True)


# ── Estado general ───────────────────────────────────��────────────────────────

def show_general_status(
    verticales: list[VerticalActivo],
    lotes_por_vertical: dict[str, int],
    leads_por_vertical: dict[str, int],
) -> None:
    """Imprime tabla de estado general (no navega a ningún lado)."""
    from .tables import active_verticals
    console.print()
    console.print(active_verticals(verticales, lotes_por_vertical, leads_por_vertical))
    console.print()
