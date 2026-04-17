"""Deduplicación de leads.

- Fase 1: exact match por nombre + partido (configurable por vertical)
- Fase 3: fuzzy matching con rapidfuzz (WRatio / token_sort_ratio)
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from .models import Lead

# Umbral de similitud para considerar dos leads como duplicados fuzzy (0–100)
DEFAULT_FUZZY_THRESHOLD = 85


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


# ── Fuzzy dedup (Fase 3) ───────────────────────────────────────────────────────

@dataclass
class FuzzyMatch:
    """Par de leads similares detectados por fuzzy matching."""
    lead_a: Lead
    lead_b: Lead
    score: float          # 0–100
    match_type: str       # "nombre" | "nombre+partido"


def dedup_fuzzy(
    leads: list[Lead],
    threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> list[Lead]:
    """Elimina duplicados fuzzy (similitud de nombre ≥ threshold).

    Algoritmo: token_sort_ratio sobre nombre normalizado.
    Complejidad: O(n²). Para lotes grandes (>500) considera usar dedup_exact primero.

    Args:
        leads: lista de leads (ya sin duplicados exactos idealmente).
        threshold: similitud mínima 0–100 (default 85).

    Returns: lista deduplicada, conservando el primero de cada grupo.
    """
    from rapidfuzz import fuzz

    if len(leads) <= 1:
        return list(leads)

    names = [_normalize(lead.nombre or "") for lead in leads]
    keep = [True] * len(leads)

    for i in range(len(leads)):
        if not keep[i]:
            continue
        for j in range(i + 1, len(leads)):
            if not keep[j]:
                continue
            score = fuzz.token_sort_ratio(names[i], names[j])
            if score >= threshold:
                # Mismo partido o partido vacío → duplicado
                partido_i = _normalize(leads[i].partido or "")
                partido_j = _normalize(leads[j].partido or "")
                if not partido_i or not partido_j or partido_i == partido_j:
                    keep[j] = False

    return [lead for lead, k in zip(leads, keep) if k]


def find_fuzzy_matches(
    leads: list[Lead],
    threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> list[FuzzyMatch]:
    """Detecta todos los pares de leads con similitud ≥ threshold.

    Útil para review manual antes de descartar duplicados.

    Args:
        leads: lista de leads a comparar.
        threshold: similitud mínima.

    Returns: lista de FuzzyMatch ordenada por score descendente.
    """
    from rapidfuzz import fuzz

    names = [_normalize(lead.nombre or "") for lead in leads]
    matches: list[FuzzyMatch] = []

    for i in range(len(leads)):
        for j in range(i + 1, len(leads)):
            score = fuzz.token_sort_ratio(names[i], names[j])
            if score >= threshold:
                partido_i = _normalize(leads[i].partido or "")
                partido_j = _normalize(leads[j].partido or "")
                match_type = "nombre"
                if partido_i and partido_j and partido_i == partido_j:
                    match_type = "nombre+partido"
                matches.append(FuzzyMatch(
                    lead_a=leads[i],
                    lead_b=leads[j],
                    score=float(score),
                    match_type=match_type,
                ))

    matches.sort(key=lambda m: m.score, reverse=True)
    return matches


def dedup_vs_production(
    new_leads: list[Lead],
    production_leads: list[Lead],
    threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> tuple[list[Lead], list[FuzzyMatch]]:
    """Filtra leads nuevos que ya existen en producción.

    Compara exact match primero (O(n)), luego fuzzy para los que pasan.

    Args:
        new_leads: leads del lote nuevo.
        production_leads: leads de producción (del VPS o CSV consolidado).
        threshold: umbral fuzzy.

    Returns:
        (leads_nuevos_unicos, matches_encontrados)
        leads_nuevos_unicos: los que NO están en producción.
        matches_encontrados: pares (nuevo, producción) similares.
    """
    from rapidfuzz import fuzz

    # Paso 1: exact match rápido por clave
    prod_keys = {_dedup_key(lead) for lead in production_leads}
    candidates = [l for l in new_leads if _dedup_key(l) not in prod_keys]

    if not candidates or not production_leads:
        return candidates, []

    # Paso 2: fuzzy match entre candidatos y producción
    prod_names = [_normalize(l.nombre or "") for l in production_leads]
    unique: list[Lead] = []
    found_matches: list[FuzzyMatch] = []

    for new_lead in candidates:
        new_name = _normalize(new_lead.nombre or "")
        best_score = 0.0
        best_prod: Lead | None = None

        for prod_lead, prod_name in zip(production_leads, prod_names):
            score = fuzz.token_sort_ratio(new_name, prod_name)
            if score > best_score:
                best_score = score
                best_prod = prod_lead

        if best_score >= threshold and best_prod is not None:
            partido_new = _normalize(new_lead.partido or "")
            partido_prod = _normalize(best_prod.partido or "")
            if not partido_new or not partido_prod or partido_new == partido_prod:
                found_matches.append(FuzzyMatch(
                    lead_a=new_lead,
                    lead_b=best_prod,
                    score=best_score,
                    match_type="vs_production",
                ))
                continue  # no es nuevo

        unique.append(new_lead)

    found_matches.sort(key=lambda m: m.score, reverse=True)
    return unique, found_matches
