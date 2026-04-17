"""Upload al VPS via rsync + seed.ts remoto.

Paso 1: rsync data/enriched/<vertical>/consolidated.csv → bunker:/root/apps/barrios-dashboard/data/
Paso 2: ssh bunker "docker run ... npx tsx prisma/seed.ts"

El seed es idempotente. Esta clase solo ejecuta los comandos, no duplica la lógica de dedup.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import structlog

from ..core.config import get_settings
from ..core.models import Vertical

log = structlog.get_logger(__name__)


@dataclass
class UploadResult:
    success: bool
    vertical: Vertical
    csv_path: Path
    rsync_returncode: int = 0
    seed_returncode: int = 0
    rsync_output: str = ""
    seed_output: str = ""
    error: str = ""


def _build_rsync_cmd(csv_path: Path, vertical: Vertical) -> list[str]:
    settings = get_settings()
    ssh_alias = settings.vps_ssh_alias
    remote_path = settings.vps_app_path.rstrip("/")
    # remote dir: <vps_app_path>/data/<vertical>/
    remote_dir = f"{ssh_alias}:{remote_path}/data/{vertical.value}/"
    return [
        "rsync",
        "-avz",
        "--mkpath",
        str(csv_path),
        remote_dir,
    ]


def _build_seed_cmd(vertical: Vertical) -> list[str]:
    settings = get_settings()
    ssh_alias = settings.vps_ssh_alias
    remote_path = settings.vps_app_path.rstrip("/")
    db_url = settings.vps_db_url
    db_password = settings.vps_db_password or ""

    # Comando que se ejecuta en el VPS
    seed_remote = (
        f"cd {remote_path} && "
        f"DATABASE_URL='{db_url}' DB_PASSWORD='{db_password}' "
        f"npx tsx prisma/seed.ts --vertical {vertical.value}"
    )
    return ["ssh", ssh_alias, seed_remote]


def upload(vertical: Vertical, csv_path: Path, dry_run: bool = False) -> UploadResult:
    """Ejecuta rsync + seed remoto.

    Args:
        vertical: vertical a subir.
        csv_path: ruta al CSV consolidado.
        dry_run: si True, imprime los comandos sin ejecutarlos.

    Returns: UploadResult con resultados de ambos pasos.
    """
    result = UploadResult(success=False, vertical=vertical, csv_path=csv_path)

    if not csv_path.exists():
        result.error = f"CSV no encontrado: {csv_path}"
        log.error("upload.csv_missing", path=str(csv_path))
        return result

    rsync_cmd = _build_rsync_cmd(csv_path, vertical)
    seed_cmd = _build_seed_cmd(vertical)

    log.info("upload.rsync.start", cmd=" ".join(rsync_cmd), dry_run=dry_run)

    if dry_run:
        result.rsync_output = "DRY RUN: " + " ".join(rsync_cmd)
        result.seed_output = "DRY RUN: " + " ".join(seed_cmd)
        result.rsync_returncode = 0
        result.seed_returncode = 0
        result.success = True
        log.info("upload.dry_run.ok", vertical=vertical.value)
        return result

    # Paso 1: rsync
    try:
        proc = subprocess.run(
            rsync_cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        result.rsync_returncode = proc.returncode
        result.rsync_output = (proc.stdout + proc.stderr).strip()

        if proc.returncode != 0:
            result.error = f"rsync falló (código {proc.returncode})"
            log.error("upload.rsync.failed", returncode=proc.returncode, output=result.rsync_output)
            return result

        log.info("upload.rsync.ok", vertical=vertical.value)

    except subprocess.TimeoutExpired:
        result.error = "rsync timeout (120s)"
        log.error("upload.rsync.timeout")
        return result
    except FileNotFoundError:
        result.error = "rsync no está instalado"
        log.error("upload.rsync.not_found")
        return result

    # Paso 2: seed remoto
    log.info("upload.seed.start", vertical=vertical.value)
    try:
        proc = subprocess.run(
            seed_cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        result.seed_returncode = proc.returncode
        result.seed_output = (proc.stdout + proc.stderr).strip()

        if proc.returncode != 0:
            result.error = f"seed.ts falló (código {proc.returncode})"
            log.error("upload.seed.failed", returncode=proc.returncode, output=result.seed_output[-500:])
            return result

        log.info("upload.seed.ok", vertical=vertical.value)

    except subprocess.TimeoutExpired:
        result.error = "seed timeout (300s)"
        log.error("upload.seed.timeout")
        return result
    except FileNotFoundError:
        result.error = "ssh no está disponible"
        log.error("upload.seed.ssh_not_found")
        return result

    result.success = True
    return result


def build_preview_commands(vertical: Vertical, csv_path: Path) -> tuple[str, str]:
    """Retorna los comandos rsync y seed como strings (para mostrar en UI)."""
    rsync_cmd = _build_rsync_cmd(csv_path, vertical)
    seed_cmd = _build_seed_cmd(vertical)
    return " ".join(rsync_cmd), " ".join(seed_cmd)
