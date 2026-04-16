"""Tablas Rich para previews de leads y resúmenes de lotes.

Todas las funciones retornan un objeto Rich Table listo para imprimir
con Console.print() o console.print(table).
"""

from datetime import datetime
from typing import Any

from rich.console import Console
from rich.table import Table
from rich import box

from ..core.models import Lead, Vertical, VERTICAL_DISPLAY_NAMES
from ..core.state import Lote, VerticalActivo, Ejecucion

console = Console()


# ── Tablas de leads ──────────────────────────────────────────────────────────��─

def leads_preview(leads: list[Lead], max_rows: int = 10) -> Table:
    """Tabla con preview de los primeros N leads de un lote."""
    table = Table(
        title=f"Preview de leads ({min(len(leads), max_rows)} de {len(leads)})",
        box=box.ROUNDED,
        show_lines=False,
        highlight=True,
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Nombre", style="bold", min_width=20)
    table.add_column("Email", style="cyan", min_width=24)
    table.add_column("Teléfono", style="green", min_width=16)
    table.add_column("Partido", min_width=12)
    table.add_column("V.Email", justify="center", width=8)

    for i, lead in enumerate(leads[:max_rows], start=1):
        validado = "[green]✓[/green]" if lead.email_validado else "[dim]·[/dim]"
        table.add_row(
            str(i),
            lead.nombre,
            str(lead.email) if lead.email else "[dim]—[/dim]",
            lead.telefono or "[dim]—[/dim]",
            lead.partido or "[dim]—[/dim]",
            validado,
        )

    return table


def leads_quality_summary(leads: list[Lead]) -> Table:
    """Tabla resumen de calidad de un lote de leads."""
    total = len(leads)
    if total == 0:
        total = 1  # evitar división por cero

    con_email = sum(1 for l in leads if l.email)
    validados = sum(1 for l in leads if l.email_validado)
    con_tel = sum(1 for l in leads if l.telefono)
    geocoded = sum(1 for l in leads if l.latitud is not None)
    con_sitio = sum(1 for l in leads if l.sitio_web)

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Campo", style="bold", min_width=28)
    table.add_column("Valor", justify="right", min_width=8)
    table.add_column("Porcentaje", justify="right", style="dim", min_width=8)

    def pct(n: int) -> str:
        return f"({n * 100 // total}%)"

    table.add_row("Total leads procesados", str(len(leads)), "")
    table.add_row("Con email", str(con_email), pct(con_email))
    table.add_row("Emails validados (Hunter)", str(validados), pct(validados))
    table.add_row("Con teléfono", str(con_tel), pct(con_tel))
    table.add_row("Geocoded (lat/lon)", str(geocoded), pct(geocoded))
    table.add_row("Con sitio web", str(con_sitio), pct(con_sitio))

    return table


# ── Tablas de lotes ───────────────────────���───────────────────────────────────��

def lotes_pendientes(lotes: list[Lote]) -> Table:
    """Tabla de lotes pendientes de un vertical."""
    table = Table(
        title="Lotes pendientes de upload",
        box=box.ROUNDED,
        show_lines=False,
    )
    table.add_column("ID", style="dim", width=10)
    table.add_column("Fecha", min_width=18)
    table.add_column("CSV", min_width=30, style="cyan")
    table.add_column("Leads", justify="right", width=8)
    table.add_column("Estado", width=12)

    for lote in lotes:
        fecha_str = lote.creado_en.strftime("%Y-%m-%d %H:%M")
        estado_style = {
            "pendiente": "[yellow]pendiente[/yellow]",
            "subido": "[green]subido[/green]",
            "descartado": "[red]descartado[/red]",
        }.get(lote.estado, lote.estado)
        table.add_row(
            lote.id[:8],
            fecha_str,
            lote.csv_path,
            str(lote.leads_count),
            estado_style,
        )

    return table


def batch_upload_summary(lotes: list[Lote]) -> Table:
    """Resumen consolidado de lotes a subir al VPS."""
    total_leads = sum(l.leads_count for l in lotes)

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Campo", style="bold", min_width=30)
    table.add_column("Valor", justify="right", min_width=8)

    table.add_row("Total lotes a consolidar", str(len(lotes)))
    table.add_row("Total leads", str(total_leads))

    return table


# ── Tablas de estado general ───────────────────────────────��───────────────────

def active_verticals(
    verticales: list[VerticalActivo],
    lotes_por_vertical: dict[str, int] | None = None,
    leads_por_vertical: dict[str, int] | None = None,
) -> Table:
    """Tabla de verticales activos con estadísticas de lotes pendientes."""
    lotes_por_vertical = lotes_por_vertical or {}
    leads_por_vertical = leads_por_vertical or {}

    table = Table(
        title="Verticales activos",
        box=box.ROUNDED,
        show_lines=False,
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Vertical", style="bold", min_width=22)
    table.add_column("Activado", min_width=12, style="dim")
    table.add_column("Lotes pendientes", justify="right", width=16)
    table.add_column("Leads pendientes", justify="right", width=16)

    for i, v in enumerate(verticales, start=1):
        nombre = VERTICAL_DISPLAY_NAMES.get(v.vertical, v.vertical.value)
        fecha = v.activado_en.strftime("%Y-%m-%d")
        lotes = lotes_por_vertical.get(v.vertical.value, 0)
        leads = leads_por_vertical.get(v.vertical.value, 0)
        table.add_row(
            str(i),
            nombre,
            fecha,
            str(lotes) if lotes else "[dim]0[/dim]",
            str(leads) if leads else "[dim]0[/dim]",
        )

    return table


def execution_history(ejecuciones: list[Ejecucion]) -> Table:
    """Tabla de historial de ejecuciones."""
    table = Table(
        title="Historial de ejecuciones",
        box=box.ROUNDED,
        show_lines=False,
    )
    table.add_column("ID", style="dim", width=10)
    table.add_column("Inicio", min_width=18)
    table.add_column("Discovered", justify="right", width=12)
    table.add_column("Scraped", justify="right", width=10)
    table.add_column("Enriched", justify="right", width=10)
    table.add_column("Validados", justify="right", width=10)
    table.add_column("Errores", justify="right", width=10)

    for e in ejecuciones:
        errores_style = f"[red]{e.errors}[/red]" if e.errors > 0 else "[dim]0[/dim]"
        table.add_row(
            e.id[:8],
            e.inicio.strftime("%Y-%m-%d %H:%M"),
            str(e.discovered),
            str(e.scraped),
            str(e.enriched),
            str(e.validated),
            errores_style,
        )

    return table


def vertical_info(vertical: Vertical, metadata_fields: list[str]) -> Table:
    """Tabla descriptiva de un vertical al activarlo."""
    from ..discovery.registry import DISCOVERY_SOURCES
    source = DISCOVERY_SOURCES.get(vertical, "Google Dorks")

    table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    table.add_column("Campo", style="bold", min_width=26)
    table.add_column("Valor", min_width=30)

    nombre = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    table.add_row("Vertical", nombre)
    table.add_row("Fuente descubrimiento", source)
    table.add_row("Campos metadata", "\n".join(metadata_fields[:5]))

    return table
