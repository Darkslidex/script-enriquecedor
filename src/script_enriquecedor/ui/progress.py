"""Progress bars con ETA para el pipeline de scraping + enriquecimiento.

Wrapper sobre rich.progress que expone el layout exacto del spec:

  Scrapeando Barrios Privados — lote_2026-04-16_153012.csv
  [▓▓▓▓▓▓▓▓░░░░░░░░] 24/50  ETA: 4min 23s
  ✓ Descubiertos: 50
  ⋯ Scrapeados: 24
  ⋯ Enriquecidos con LLM: 18
  ⋯ Emails validados (Hunter): 12
  ⋯ Geocoded: 15
  ✗ Errores: 1

Uso:
    with PipelineProgress(total=50, lote_name="lote_2026-04-16_153012.csv") as prog:
        for item in items:
            process(item)
            prog.update(scraped=1)
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TaskID,
)
from rich.table import Table
from rich.text import Text

from ..core.models import Vertical, VERTICAL_DISPLAY_NAMES

console = Console()


@dataclass
class PipelineStats:
    """Contadores en tiempo real del pipeline."""
    total: int = 0
    discovered: int = 0
    scraped: int = 0
    enriched: int = 0
    validated: int = 0
    geocoded: int = 0
    errors: int = 0


class PipelineProgress:
    """Progress bar del pipeline con métricas en tiempo real.

    Uso como context manager:
        with PipelineProgress(total=50, lote_name="lote_xxx.csv", vertical=Vertical.BARRIOS_PRIVADOS) as p:
            p.set_discovered(50)
            p.advance_scraped()
            p.advance_enriched()
    """

    def __init__(
        self,
        total: int,
        lote_name: str,
        vertical: Vertical = Vertical.BARRIOS_PRIVADOS,
    ) -> None:
        self.stats = PipelineStats(total=total, discovered=total)
        self.lote_name = lote_name
        self.vertical = vertical
        self._vertical_name = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)

        self._progress = Progress(
            SpinnerColumn(),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TextColumn("[dim]ETA:[/dim]"),
            TimeRemainingColumn(),
            TextColumn("[dim]·[/dim]"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        )
        self._task: TaskID | None = None
        self._live: Live | None = None

    def __enter__(self) -> "PipelineProgress":
        self._progress.start()
        self._task = self._progress.add_task(
            f"[bold cyan]Scrapeando {self._vertical_name}[/bold cyan]",
            total=self.stats.total,
        )
        return self

    def __exit__(self, *_) -> None:
        self._progress.stop()
        self._print_final_summary()

    # ── Actualizadores ────────────────────────────────────────────────────

    def set_discovered(self, n: int) -> None:
        self.stats.discovered = n

    def advance_scraped(self, n: int = 1) -> None:
        self.stats.scraped += n
        if self._task is not None:
            self._progress.advance(self._task, n)

    def advance_enriched(self, n: int = 1) -> None:
        self.stats.enriched += n

    def advance_validated(self, n: int = 1) -> None:
        self.stats.validated += n

    def advance_geocoded(self, n: int = 1) -> None:
        self.stats.geocoded += n

    def add_error(self, n: int = 1) -> None:
        self.stats.errors += n

    # ── Resumen final ───────────────────────────────────────────────���─────

    def _print_final_summary(self) -> None:
        s = self.stats
        console.print()
        console.print(f"[bold green]✓[/bold green] Lote completado: [cyan]{self.lote_name}[/cyan]")
        console.print()

        stats_table = Table(box=None, show_header=False, padding=(0, 2))
        stats_table.add_column("", style="bold", min_width=30)
        stats_table.add_column("", justify="right", min_width=10)

        total = max(s.total, 1)

        def pct(n: int) -> str:
            return f"[dim]({n * 100 // total}%)[/dim]"

        stats_table.add_row("Leads procesados:", str(s.total))
        stats_table.add_row("Con email:", f"{s.enriched} {pct(s.enriched)}")
        stats_table.add_row("Emails validados:", f"{s.validated} {pct(s.validated)}")
        stats_table.add_row("Geocoded:", f"{s.geocoded} {pct(s.geocoded)}")

        if s.errors:
            stats_table.add_row(
                "[red]Errores:[/red]",
                f"[red]{s.errors}[/red]",
            )
        else:
            stats_table.add_row("[dim]Errores:[/dim]", "[green]0[/green]")

        console.print(stats_table)
        console.print()


def make_spinner(label: str = "Procesando...") -> Progress:
    """Spinner simple para operaciones cortas (activar vertical, upload, etc.)."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}[/bold cyan]"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )
