"""Gestión de lotes: crea, lista y consolida CSVs en data/enriched/<vertical>/."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from ..core.models import Lead, Vertical
from ..core.state import get_state
from .csv_writer import append_csv, read_csv, write_csv

# Directorio base para archivos enriched
DATA_DIR = Path("data/enriched")


def _vertical_dir(vertical: Vertical) -> Path:
    return DATA_DIR / vertical.value


def _lote_path(vertical: Vertical, lote_id) -> Path:
    return _vertical_dir(vertical) / f"lote_{lote_id}.csv"


def _consolidated_path(vertical: Vertical) -> Path:
    return _vertical_dir(vertical) / "consolidated.csv"


def save_lote(vertical: Vertical, leads: list[Lead]) -> int:
    """Guarda un nuevo lote de leads en CSV y lo registra en el StateManager.

    Returns: lote_id creado.
    """
    state = get_state()
    lote = state.create_lote(
        vertical=vertical,
        csv_path="",  # placeholder, se actualiza abajo
        leads_count=len(leads),
    )
    lote_id: int = lote.id  # type: ignore[attr-defined]

    path = _lote_path(vertical, lote_id)
    written = write_csv(leads, path)

    state.update_lote(lote_id, estado="pendiente", leads_count=written)
    # Actualiza csv_path con la ruta real (state no guarda path en update, se pone en create)
    # El path ya quedó en la creación del registro; solo actualizamos si hace falta.

    return lote_id


def list_lotes(vertical: Vertical) -> list[dict]:
    """Lista los lotes pendientes del StateManager para una vertical."""
    state = get_state()
    lotes_obj = state.get_lotes(vertical=vertical, estado="pendiente")
    return [{"id": l.id, "leads_count": l.leads_count, "creado_en": str(l.creado_en)} for l in lotes_obj]


def consolidate(vertical: Vertical) -> tuple[Path, int]:
    """Combina todos los lotes pendientes en consolidated.csv.

    Returns: (path consolidado, total_leads).
    Marca los lotes consolidados como 'consolidado'.
    """
    state = get_state()
    lotes_obj = state.get_lotes(vertical=vertical, estado="pendiente")
    lotes = [{"id": l.id, "leads_count": l.leads_count} for l in lotes_obj]

    if not lotes:
        path = _consolidated_path(vertical)
        existing = read_csv(path)
        return path, len(existing)

    all_rows: list[dict[str, str]] = []
    lote_ids: list[int] = []

    for lote in lotes:
        lote_id = lote["id"]
        path = _lote_path(vertical, lote_id)
        rows = read_csv(path)
        all_rows.extend(rows)
        lote_ids.append(lote_id)

    # Construye leads vacíos no: directamente escribimos rows al consolidado
    # Para evitar re-parseo complejo, escribimos rows directamente
    consolidated = _consolidated_path(vertical)
    consolidated.parent.mkdir(parents=True, exist_ok=True)

    import csv
    from .csv_writer import CSV_HEADERS

    with consolidated.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in CSV_HEADERS})

    # Marca lotes como consolidados
    for lote_id in lote_ids:
        state.update_lote(lote_id, estado="consolidado")

    return consolidated, len(all_rows)


def mark_lote_uploaded(lote_id: int) -> None:
    """Marca un lote como subido con timestamp actual."""
    state = get_state()
    state.update_lote(
        lote_id,
        estado="subido",
        subido_en=datetime.now(timezone.utc),
    )


def get_lotes_summary(vertical: Vertical) -> dict:
    """Retorna resumen de lotes por estado para una vertical."""
    state = get_state()
    pendientes = state.get_lotes(vertical=vertical, estado="pendiente")
    consolidados = state.get_lotes(vertical=vertical, estado="consolidado")
    subidos = state.get_lotes(vertical=vertical, estado="subido")

    return {
        "pendientes": len(pendientes),
        "consolidados": len(consolidados),
        "subidos": len(subidos),
        "total": len(pendientes) + len(consolidados) + len(subidos),
    }
