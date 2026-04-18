"""Normalización de filas CSV al schema base del unificador.

Cada función toma una fila (dict str→str) y retorna un MergeRow —
un dict con las claves del schema Prisma + campos internos de dedup.

MergeRow keys:
  _key           → clave de deduplicación (nombre normalizado + dominio)
  _source        → "scraper" | "linkedin"
  + todos los campos del CSV_HEADERS (storage/csv_writer.py)
"""

from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlparse


# ── Normalización de texto para dedup ─────────────────────────────────────────

def _normalize_text(text: str) -> str:
    """Lowercase, sin acentos, sin puntuación, espacios colapsados."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_domain(url: str) -> str:
    """Extrae solo el hostname de una URL para usar como clave."""
    if not url:
        return ""
    try:
        parsed = urlparse(url if "://" in url else "https://" + url)
        host = parsed.netloc or parsed.path
        # Quitar www.
        return re.sub(r"^www\.", "", host).lower().strip()
    except Exception:
        return ""


def make_key(nombre: str, sitio_web: str = "", partido: str = "") -> str:
    """Clave de deduplicación: nombre_normalizado + dominio (o partido si no hay dominio).

    Dos registros con la misma key representan la misma organización.
    """
    nombre_norm = _normalize_text(nombre)
    domain = _extract_domain(sitio_web)
    if domain:
        return f"{nombre_norm}|{domain}"
    if partido:
        return f"{nombre_norm}|{_normalize_text(partido)}"
    return nombre_norm


# ── Normalizadores por fuente ──────────────────────────────────────────────────

def normalize_scraper_row(row: dict[str, str]) -> dict[str, str]:
    """Normaliza una fila del CSV del scraper al schema base del unificador.

    El scraper ya usa CSV_HEADERS — solo agregamos _key y _source.
    """
    nombre   = row.get("nombre", "")
    web      = row.get("sitio_web", "")
    partido  = row.get("partido", "")

    return {
        **row,
        "_key":    make_key(nombre, web, partido),
        "_source": "scraper",
    }


def normalize_linkedin_row(row: dict[str, str]) -> dict[str, str]:
    """Normaliza una fila del CSV de LinkedIn al schema base del unificador.

    Mapeo desde columnas de Sales Navigator al schema Prisma.
    """
    first   = row.get("First Name",          row.get("firstName",  "")).strip()
    last    = row.get("Last Name",           row.get("lastName",   "")).strip()
    title   = row.get("Title",               row.get("title",      "")).strip()
    company = row.get("Company",             row.get("company",    "")).strip()
    ln_url  = row.get("LinkedIn Profile URL",row.get("linkedInUrl","")).strip()
    email   = row.get("Email",               row.get("email",      "")).strip()
    phone   = row.get("Phone",               row.get("phone",      "")).strip()
    website = row.get("Website",             row.get("website",    "")).strip()
    geo     = row.get("Geography",           row.get("geography",  "")).strip()

    # Parsear geografía → provincia
    provincia = ""
    if geo:
        parts = [p.strip() for p in geo.split(",")]
        if len(parts) >= 2:
            provincia = parts[0]

    full_name = f"{first} {last}".strip()
    fuente = f"linkedin:{ln_url}" if ln_url else "linkedin_csv"

    # Normalizar URL
    if website and not website.startswith(("http://", "https://")):
        website = "https://" + website

    return {
        "nombre":               company,
        "vertical":             "empresas",
        "estado_comercial":     "SIN_CONTACTAR",
        "email":                email,
        "email_2":              "",
        "email_3":              "",
        "email_validado":       "false",
        "email_score":          "",
        "telefono":             phone,
        "sitio_web":            website,
        "fuente_contacto":      "",
        "fuente_descubrimiento": fuente,
        "contacto_nombre":      full_name,
        "contacto_cargo":       title,
        "fecha_enriquecimiento": "",
        "direccion":            "",
        "localidad":            "",
        "partido":              "",
        "provincia":            provincia,
        "pais":                 "Argentina",
        "cp":                   "",
        "latitud":              "",
        "longitud":             "",
        "metadata":             f'{{"linkedin_url": "{ln_url}"}}' if ln_url else "",
        "_key":                 make_key(company, website),
        "_source":              "linkedin",
    }
