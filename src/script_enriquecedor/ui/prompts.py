"""Prompts interactivos reutilizables.

Wrappers sobre rich.prompt que estandarizan el estilo visual del CLI.
Todas las funciones imprimen en el console global y retornan el valor elegido.
"""

from enum import Enum
from typing import TypeVar

from rich.console import Console
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.text import Text

console = Console()

T = TypeVar("T")


# ── Helpers internos ───────────────────────────────────────────────────────────

def _print_choice(n: int, label: str, hint: str = "", selected: bool = False) -> None:
    """Imprime una opción numerada con formato consistente."""
    prefix = "[bold cyan]›[/bold cyan]" if selected else " "
    line = f"  {prefix} [bold]{n}[/bold]. {label}"
    if hint:
        line += f"  [dim]{hint}[/dim]"
    console.print(line)


# ── API pública ──────────────────────────────────��───────────────────────────��─

def select(
    title: str,
    choices: list[str],
    hints: list[str] | None = None,
    allow_back: bool = True,
) -> int | None:
    """Muestra un menú numerado y retorna el índice elegido (0-based).

    Returns:
        int  — índice en `choices` (0-based) si el usuario eligió una opción
        None — si el usuario eligió "Volver" o presionó Ctrl+C
    """
    console.print()
    hints = hints or [""] * len(choices)
    for i, (choice, hint) in enumerate(zip(choices, hints), start=1):
        _print_choice(i, choice, hint)
    if allow_back:
        back_n = len(choices) + 1
        console.print(f"  [dim]{back_n}. ← Volver[/dim]")

    console.print()
    while True:
        try:
            raw = Prompt.ask("[bold]›[/bold] Elegí una opción")
            n = int(raw.strip())
            if 1 <= n <= len(choices):
                return n - 1
            if allow_back and n == len(choices) + 1:
                return None
            console.print(f"  [red]Opción inválida. Ingresá entre 1 y {len(choices) + (1 if allow_back else 0)}.[/red]")
        except (ValueError, TypeError):
            console.print("  [red]Ingresá un número.[/red]")
        except KeyboardInterrupt:
            console.print()
            return None


def confirm(prompt: str, default: bool = False) -> bool:
    """Retorna True/False. Usa (s/n) en el prompt."""
    try:
        return Confirm.ask(f"[bold]{prompt}[/bold]", default=default)
    except KeyboardInterrupt:
        console.print()
        return False


def confirm_exact(prompt: str, expected: str) -> bool:
    """Retorna True solo si el usuario escribe exactamente `expected`.

    Usado para confirmar acciones destructivas (ej: escribir "SUBIR").
    """
    console.print(f"\n[bold yellow]{prompt}[/bold yellow]")
    try:
        answer = Prompt.ask("[bold]›[/bold]")
        return answer.strip() == expected
    except KeyboardInterrupt:
        console.print()
        return False


def ask_int(
    prompt: str,
    min_val: int = 1,
    max_val: int = 9999,
    default: int | None = None,
) -> int | None:
    """Solicita un entero en el rango [min_val, max_val].

    Returns:
        int  — valor ingresado
        None — si el usuario presionó Ctrl+C
    """
    hint = f"[dim]({min_val}–{max_val})[/dim]"
    while True:
        try:
            if default is not None:
                val = IntPrompt.ask(f"[bold]{prompt}[/bold] {hint}", default=default)
            else:
                val = IntPrompt.ask(f"[bold]{prompt}[/bold] {hint}")
            if min_val <= val <= max_val:
                return val
            console.print(f"  [red]Debe estar entre {min_val} y {max_val}.[/red]")
        except KeyboardInterrupt:
            console.print()
            return None


def ask_text(prompt: str, default: str | None = None) -> str | None:
    """Solicita texto libre.

    Returns:
        str  — texto ingresado
        None — si el usuario presionó Ctrl+C
    """
    try:
        if default:
            return Prompt.ask(f"[bold]{prompt}[/bold]", default=default)
        return Prompt.ask(f"[bold]{prompt}[/bold]")
    except KeyboardInterrupt:
        console.print()
        return None


def print_section(title: str, color: str = "bold cyan") -> None:
    """Imprime un separador de sección."""
    console.print(f"\n[{color}]── {title} ──────────────────────────────[/{color}]")


def print_success(msg: str) -> None:
    console.print(f"[bold green]✓[/bold green] {msg}")


def print_warning(msg: str) -> None:
    console.print(f"[bold yellow]⚠[/bold yellow]  {msg}")


def print_error(msg: str) -> None:
    console.print(f"[bold red]✗[/bold red] {msg}")


def print_info(msg: str) -> None:
    console.print(f"[bold blue]·[/bold blue] {msg}")
