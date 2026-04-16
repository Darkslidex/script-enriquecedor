"""Deduplicación de leads.

- Fase 1: exact match por nombre + partido (configurable por vertical)
- Fase 3: fuzzy matching con rapidfuzz (Levenshtein)
"""

from __future__ import annotations

import unicodedata

from .models import Lead


def _normalize(text: str) -> str:
    """Normaliza string: minúsculas, sin acentos, sin puntuación extra."""
    text = text.lower().strip()
    # Elimina acentos
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    # Colapsa espacios múltiples
    return " ".join(text.split())


def _dedup_key(lead: Lead) -> str:
    """Clave de deduplicación: nombre_normalizado|partido_normalizado."""
    nombre = _normalize(lead.nombre or "")
    partido = _normalize(lead.partido or "")
    return f"{nombre}|{partido}"


def dedup_exact(leads: list[Lead]) -> list[Lead]:
    """Elimina duplicados exactos (nombre + partido normalizados).

    Conserva el primer elemento de cada grupo duplicado.
    Complejidad: O(n).

    Args:
        leads: lista de leads (puede tener duplicados).

    Returns: lista sin duplicados exactos, orden preservado.
    """
    seen: set[str] = set()
    result: list[Lead] = []
    for lead in leads:
        key = _dedup_key(lead)
        if key not in seen:
            seen.add(key)
            result.append(lead)
    return result


def count_duplicates(leads: list[Lead]) -> int:
    """Retorna cuántos duplicados exactos existen en la lista."""
    return len(leads) - len(dedup_exact(leads))


def find_duplicate_groups(leads: list[Lead]) -> list[list[Lead]]:
    """Agrupa leads por clave de dedup. Retorna solo grupos con >1 elemento."""
    from collections import defaultdict

    groups: dict[str, list[Lead]] = defaultdict(list)
    for lead in leads:
        groups[_dedup_key(lead)].append(lead)
    return [g for g in groups.values() if len(g) > 1]
