"""Entrypoint CLI — Script Enriquecedor v2.

Correr con:
    python cli.py
    uv run python cli.py
"""

import typer

app = typer.Typer(invoke_without_command=True)


def main() -> None:
    """Lanza el CLI interactivo."""
    # TODO: importar y ejecutar ui.menus.main_menu() (Fase 1 paso 5)
    typer.echo("Script Enriquecedor v2 — en construcción")


if __name__ == "__main__":
    main()
