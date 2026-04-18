"""Escritura de CSVs compatibles con prisma/seed.ts del dashboard.

Headers exactos (orden fijo):
nombre,vertical,estado_comercial,email,email_2,email_3,email_validado,
email_score,telefono,sitio_web,fuente_contacto,fecha_enriquecimiento,
direccion,localidad,partido,provincia,pais,cp,latitud,longitud,metadata

El campo metadata va serializado como JSON string escapado.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from ..core.models import Lead

# Orden fijo de columnas — debe coincidir con prisma/seed.ts
CSV_HEADERS: list[str] = [
    "nombre",
    "vertical",
    "estado_comercial",
    "email",
    "email_2",
    "email_3",
    "email_validado",
    "email_score",
    "telefono",
    "sitio_web",
    "fuente_contacto",
    "fuente_descubrimiento",
    "contacto_nombre",
    "contacto_cargo",
    "fecha_enriquecimiento",
    "direccion",
    "localidad",
    "partido",
    "provincia",
    "pais",
    "cp",
    "latitud",
    "longitud",
    "metadata",
]


def _lead_to_row(lead: Lead) -> dict[str, str]:
    """Convierte un Lead a dict con los headers del CSV."""
    # Fecha de enriquecimiento: usa la del lead o now() como fallback
    fecha = lead.fecha_enriquecimiento or datetime.now(timezone.utc)
    if isinstance(fecha, datetime):
        fecha_str = fecha.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        fecha_str = str(fecha)

    # email_score: int o vacío
    score_str = ""
    if lead.email_score is not None:
        score_str = str(lead.email_score.value) if hasattr(lead.email_score, "value") else str(lead.email_score)

    # latitud/longitud
    lat_str = f"{lead.latitud:.6f}" if lead.latitud is not None else ""
    lon_str = f"{lead.longitud:.6f}" if lead.longitud is not None else ""

    # metadata → JSON string
    meta_str = json.dumps(lead.metadata, ensure_ascii=False) if lead.metadata else ""

    return {
        "nombre": lead.nombre or "",
        "vertical": lead.vertical.value if lead.vertical else "",
        "estado_comercial": lead.estado_comercial.value if lead.estado_comercial else "",
        "email": lead.email or "",
        "email_2": lead.email_2 or "",
        "email_3": lead.email_3 or "",
        "email_validado": "true" if lead.email_validado else "false",
        "email_score": score_str,
        "telefono": lead.telefono or "",
        "sitio_web": lead.sitio_web or "",
        "fuente_contacto": lead.fuente_contacto or "",
        "fuente_descubrimiento": lead.fuente_descubrimiento or "",
        "contacto_nombre": lead.contacto_nombre or "",
        "contacto_cargo": lead.contacto_cargo or "",
        "fecha_enriquecimiento": fecha_str,
        "direccion": lead.direccion or "",
        "localidad": lead.localidad or "",
        "partido": lead.partido or "",
        "provincia": lead.provincia or "",
        "pais": lead.pais or "Argentina",
        "cp": lead.cp or "",
        "latitud": lat_str,
        "longitud": lon_str,
        "metadata": meta_str,
    }


def write_csv(leads: list[Lead], path: Path) -> int:
    """Escribe leads a un CSV. Crea directorios si no existen.

    Returns: cantidad de filas escritas.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            writer.writerow(_lead_to_row(lead))
            count += 1
    return count


def append_csv(leads: list[Lead], path: Path) -> int:
    """Agrega leads a un CSV existente (sin reescribir headers).

    Si el archivo no existe, lo crea con headers.
    Returns: cantidad de filas agregadas.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    count = 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for lead in leads:
            writer.writerow(_lead_to_row(lead))
            count += 1
    return count


def read_csv(path: Path) -> list[dict[str, str]]:
    """Lee un CSV y retorna lista de dicts. No parsea a Lead (sin schema)."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))
