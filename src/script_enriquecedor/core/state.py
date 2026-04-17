"""Persistencia de estado local en SQLite (data/state.db).

Tablas:
- verticales_activos: qué verticales están habilitados y sus paths de configuración
- lotes: CSVs generados, estado (pendiente / subido / descartado)
- ejecuciones: historial de runs con métricas por vertical

Al inicializarse por primera vez, pre-activa Barrios Privados automáticamente.

Uso:
    from script_enriquecedor.core.state import get_state
    state = get_state()
    activos = state.get_active_verticals()
"""

import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Literal

from .config import get_settings
from .logger import get_logger
from .models import Vertical

log = get_logger("state")

EstadoLote = Literal["pendiente", "subido", "descartado"]

# Ruta al prompt de barrios privados (relativa al package)
_BARRIOS_PROMPT_PATH = "src/script_enriquecedor/enrichment/prompts/barrios_privados.md"
_BARRIOS_SCHEMA_PATH = "src/script_enriquecedor/core/metadata_schemas/barrios_privados.py"


# ── Dataclasses de resultado ───────────────────────────────────────────────────

@dataclass
class VerticalActivo:
    vertical: Vertical
    activado_en: datetime
    prompt_path: str
    schema_path: str


@dataclass
class Lote:
    id: str
    vertical: Vertical
    csv_path: str
    creado_en: datetime
    leads_count: int
    estado: EstadoLote
    subido_en: datetime | None = None


@dataclass
class Ejecucion:
    id: str
    vertical: Vertical
    inicio: datetime
    fin: datetime | None = None
    discovered: int = 0
    scraped: int = 0
    enriched: int = 0
    validated: int = 0
    errors: int = 0


# ── DDL ────────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS verticales_activos (
    vertical    TEXT PRIMARY KEY,
    activado_en TEXT NOT NULL,
    prompt_path TEXT NOT NULL,
    schema_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lotes (
    id          TEXT PRIMARY KEY,
    vertical    TEXT NOT NULL,
    csv_path    TEXT NOT NULL,
    creado_en   TEXT NOT NULL,
    subido_en   TEXT,
    leads_count INTEGER NOT NULL DEFAULT 0,
    estado      TEXT NOT NULL DEFAULT 'pendiente'
);

CREATE TABLE IF NOT EXISTS ejecuciones (
    id          TEXT PRIMARY KEY,
    vertical    TEXT NOT NULL,
    inicio      TEXT NOT NULL,
    fin         TEXT,
    discovered  INTEGER NOT NULL DEFAULT 0,
    scraped     INTEGER NOT NULL DEFAULT 0,
    enriched    INTEGER NOT NULL DEFAULT 0,
    validated   INTEGER NOT NULL DEFAULT 0,
    errors      INTEGER NOT NULL DEFAULT 0
);
"""


# ── StateManager ───────────────────────────────────────────────────────────────

class StateManager:
    """Gestión del estado local del pipeline en SQLite."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── Infraestructura ────────────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager para conexiones SQLite con autocommit en éxito."""
        conn = sqlite3.connect(self._db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Crea tablas si no existen y pre-activa Barrios Privados."""
        with self._conn() as conn:
            conn.executescript(_DDL)

        # Pre-activar Barrios Privados si no está ya activo
        if not self.is_active(Vertical.BARRIOS_PRIVADOS):
            self.activate_vertical(
                vertical=Vertical.BARRIOS_PRIVADOS,
                prompt_path=_BARRIOS_PROMPT_PATH,
                schema_path=_BARRIOS_SCHEMA_PATH,
            )
            log.info("vertical_activado_por_defecto", vertical=Vertical.BARRIOS_PRIVADOS.value)

    # ── Verticales activos ────────────────────────────────────────────────

    def get_active_verticals(self) -> list[VerticalActivo]:
        """Retorna todos los verticales activos, ordenados por fecha de activación."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM verticales_activos ORDER BY activado_en"
            ).fetchall()
        return [
            VerticalActivo(
                vertical=Vertical(r["vertical"]),
                activado_en=datetime.fromisoformat(r["activado_en"]),
                prompt_path=r["prompt_path"],
                schema_path=r["schema_path"],
            )
            for r in rows
        ]

    def is_active(self, vertical: Vertical) -> bool:
        """True si el vertical está activo."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM verticales_activos WHERE vertical = ?",
                (vertical.value,),
            ).fetchone()
        return row is not None

    def activate_vertical(
        self,
        vertical: Vertical,
        prompt_path: str,
        schema_path: str,
    ) -> VerticalActivo:
        """Activa un vertical. Si ya estaba activo, actualiza los paths."""
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO verticales_activos (vertical, activado_en, prompt_path, schema_path)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(vertical) DO UPDATE SET
                    prompt_path = excluded.prompt_path,
                    schema_path = excluded.schema_path
                """,
                (vertical.value, now, prompt_path, schema_path),
            )
        log.info("vertical_activado", vertical=vertical.value)
        return VerticalActivo(
            vertical=vertical,
            activado_en=datetime.fromisoformat(now),
            prompt_path=prompt_path,
            schema_path=schema_path,
        )

    def deactivate_vertical(self, vertical: Vertical) -> None:
        """Desactiva un vertical (no borra sus lotes históricos)."""
        if vertical == Vertical.BARRIOS_PRIVADOS:
            raise ValueError("No se puede desactivar Barrios Privados — es el vertical base.")
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM verticales_activos WHERE vertical = ?",
                (vertical.value,),
            )
        log.info("vertical_desactivado", vertical=vertical.value)

    # ── Lotes ─────────────────────────────────────────────────────────────

    def create_lote(self, vertical: Vertical, csv_path: str, leads_count: int = 0) -> Lote:
        """Registra un nuevo lote CSV como 'pendiente'."""
        lote = Lote(
            id=str(uuid.uuid4()),
            vertical=vertical,
            csv_path=csv_path,
            creado_en=datetime.now(timezone.utc),
            leads_count=leads_count,
            estado="pendiente",
        )
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO lotes (id, vertical, csv_path, creado_en, leads_count, estado)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lote.id,
                    lote.vertical.value,
                    lote.csv_path,
                    lote.creado_en.isoformat(),
                    lote.leads_count,
                    lote.estado,
                ),
            )
        log.info("lote_creado", id=lote.id, vertical=vertical.value, leads=leads_count)
        return lote

    def update_lote(
        self,
        lote_id: str,
        estado: EstadoLote,
        leads_count: int | None = None,
        subido_en: datetime | None = None,
    ) -> None:
        """Actualiza estado de un lote (y opcionalmente lead count y fecha de subida)."""
        with self._conn() as conn:
            if leads_count is not None and subido_en is not None:
                conn.execute(
                    "UPDATE lotes SET estado=?, leads_count=?, subido_en=? WHERE id=?",
                    (estado, leads_count, subido_en.isoformat(), lote_id),
                )
            elif leads_count is not None:
                conn.execute(
                    "UPDATE lotes SET estado=?, leads_count=? WHERE id=?",
                    (estado, leads_count, lote_id),
                )
            elif subido_en is not None:
                conn.execute(
                    "UPDATE lotes SET estado=?, subido_en=? WHERE id=?",
                    (estado, subido_en.isoformat(), lote_id),
                )
            else:
                conn.execute(
                    "UPDATE lotes SET estado=? WHERE id=?",
                    (estado, lote_id),
                )
        log.info("lote_actualizado", id=lote_id, estado=estado)

    def get_lotes(
        self,
        vertical: Vertical | None = None,
        estado: EstadoLote | None = None,
    ) -> list[Lote]:
        """Lista lotes con filtros opcionales por vertical y estado."""
        query = "SELECT * FROM lotes WHERE 1=1"
        params: list = []
        if vertical:
            query += " AND vertical = ?"
            params.append(vertical.value)
        if estado:
            query += " AND estado = ?"
            params.append(estado)
        query += " ORDER BY creado_en DESC"

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_row_to_lote(r) for r in rows]

    def get_lotes_pendientes(self, vertical: Vertical) -> list[Lote]:
        """Shortcut: lotes pendientes de upload para un vertical."""
        return self.get_lotes(vertical=vertical, estado="pendiente")

    def count_leads_pendientes(self, vertical: Vertical) -> int:
        """Total de leads en lotes pendientes para un vertical."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(leads_count), 0) FROM lotes WHERE vertical=? AND estado='pendiente'",
                (vertical.value,),
            ).fetchone()
        return row[0] if row else 0

    # ── Ejecuciones ────────────────────────────────────────────────────────

    def create_ejecucion(self, vertical: Vertical) -> Ejecucion:
        """Registra el inicio de una ejecución del pipeline."""
        exec_ = Ejecucion(
            id=str(uuid.uuid4()),
            vertical=vertical,
            inicio=datetime.now(timezone.utc),
        )
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO ejecuciones (id, vertical, inicio) VALUES (?, ?, ?)",
                (exec_.id, exec_.vertical.value, exec_.inicio.isoformat()),
            )
        return exec_

    def finish_ejecucion(
        self,
        ejecucion_id: str,
        discovered: int = 0,
        scraped: int = 0,
        enriched: int = 0,
        validated: int = 0,
        errors: int = 0,
    ) -> None:
        """Registra el fin de una ejecución con sus métricas."""
        fin = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE ejecuciones
                SET fin=?, discovered=?, scraped=?, enriched=?, validated=?, errors=?
                WHERE id=?
                """,
                (fin, discovered, scraped, enriched, validated, errors, ejecucion_id),
            )
        log.info(
            "ejecucion_finalizada",
            id=ejecucion_id,
            discovered=discovered,
            scraped=scraped,
            enriched=enriched,
            validated=validated,
            errors=errors,
        )

    def get_last_ejecucion(self, vertical: Vertical) -> Ejecucion | None:
        """Retorna la última ejecución registrada para un vertical."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM ejecuciones WHERE vertical=? ORDER BY inicio DESC LIMIT 1",
                (vertical.value,),
            ).fetchone()
        return _row_to_ejecucion(row) if row else None


# ── Helpers de conversión ──────────────────────────────────────────────────────

def _row_to_lote(row: sqlite3.Row) -> Lote:
    return Lote(
        id=row["id"],
        vertical=Vertical(row["vertical"]),
        csv_path=row["csv_path"],
        creado_en=datetime.fromisoformat(row["creado_en"]),
        leads_count=row["leads_count"],
        estado=row["estado"],
        subido_en=datetime.fromisoformat(row["subido_en"]) if row["subido_en"] else None,
    )


def _row_to_ejecucion(row: sqlite3.Row) -> Ejecucion:
    return Ejecucion(
        id=row["id"],
        vertical=Vertical(row["vertical"]),
        inicio=datetime.fromisoformat(row["inicio"]),
        fin=datetime.fromisoformat(row["fin"]) if row["fin"] else None,
        discovered=row["discovered"],
        scraped=row["scraped"],
        enriched=row["enriched"],
        validated=row["validated"],
        errors=row["errors"],
    )


# ── Singleton ──────────────────────────────────────────────────────────────────

_state: StateManager | None = None


def get_state() -> StateManager:
    """Retorna el StateManager singleton (crea la DB si no existe)."""
    global _state
    if _state is None:
        settings = get_settings()
        db_path = Path(settings.data_dir) / "state.db"
        _state = StateManager(db_path)
    return _state
