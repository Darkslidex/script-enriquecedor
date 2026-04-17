"""Resumen de calidad de un batch de leads y detección de duplicados vs producción.

Fase 3: métricas de completitud, scoring por campo, y comparación con CSV de producción.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import structlog

from ..core.dedup import (
    FuzzyMatch,
    count_duplicates,
    dedup_exact,
    dedup_fuzzy,
    dedup_vs_production,
    find_fuzzy_matches,
    DEFAULT_FUZZY_THRESHOLD,
)
from ..core.models import Lead, Vertical
from .csv_writer import read_csv

log = structlog.get_logger(__name__)

# Campos "críticos" cuya presencia define si un lead es de calidad alta
_CRITICAL_FIELDS = ["email", "telefono", "sitio_web"]
# Campos de enriquecimiento
_ENRICHMENT_FIELDS = ["latitud", "longitud", "email_validado", "partido", "localidad"]


@dataclass
class LeadQuality:
    """Métricas de calidad de un lead individual."""
    lead: Lead
    score: float          # 0–100
    has_critical: bool    # tiene al menos un campo crítico
    missing_fields: list[str]
    completeness: float   # % de campos rellenos (excluyendo metadata)


@dataclass
class BatchQualitySummary:
    """Resumen de calidad de un batch de leads."""
    vertical: Vertical
    total: int
    avg_score: float
    pct_with_email: float
    pct_with_phone: float
    pct_with_website: float
    pct_with_coords: float
    pct_email_validated: float
    exact_duplicates: int
    fuzzy_duplicates: int
    high_quality: int          # score ≥ 70
    medium_quality: int        # score 40–69
    low_quality: int           # score < 40
    quality_by_lead: list[LeadQuality] = field(default_factory=list)

    @property
    def pct_high_quality(self) -> float:
        return (self.high_quality / self.total * 100) if self.total else 0.0

    @property
    def upload_ready(self) -> bool:
        """True si el batch tiene calidad suficiente para subir al VPS."""
        return self.avg_score >= 40 and self.total >= 5


@dataclass
class ProductionComparisonResult:
    """Resultado de comparar un batch contra producción."""
    new_unique: list[Lead]
    already_in_production: list[FuzzyMatch]
    total_new: int
    total_checked: int

    @property
    def pct_new(self) -> float:
        return (self.total_new / self.total_checked * 100) if self.total_checked else 0.0


def score_lead(lead: Lead) -> LeadQuality:
    """Calcula el score de calidad de un lead (0–100).

    Pesos:
    - email presente: +25
    - telefono presente: +20
    - sitio_web presente: +15
    - partido/localidad: +10 cada uno
    - latitud+longitud: +10
    - email_validado=True: +10
    """
    score = 0.0
    missing: list[str] = []

    # Campos críticos
    if lead.email:
        score += 25
    else:
        missing.append("email")

    if lead.telefono:
        score += 20
    else:
        missing.append("telefono")

    if lead.sitio_web:
        score += 15
    else:
        missing.append("sitio_web")

    # Ubicación
    if lead.partido:
        score += 10
    else:
        missing.append("partido")

    if lead.localidad:
        score += 10
    else:
        missing.append("localidad")

    # Geocodificación
    if lead.latitud is not None and lead.longitud is not None:
        score += 10
    else:
        missing.append("coordenadas")

    # Validación de email
    if lead.email_validado:
        score += 10

    has_critical = bool(lead.email or lead.telefono or lead.sitio_web)

    # Completeness: campos de Lead no None (excluyendo metadata y campos internos)
    _TRACKED = ["nombre", "email", "telefono", "sitio_web", "partido",
                "localidad", "provincia", "direccion", "latitud", "longitud"]
    filled = sum(1 for f in _TRACKED if getattr(lead, f, None) is not None)
    completeness = filled / len(_TRACKED) * 100

    return LeadQuality(
        lead=lead,
        score=min(score, 100.0),
        has_critical=has_critical,
        missing_fields=missing,
        completeness=completeness,
    )


def summarize_batch(
    leads: list[Lead],
    vertical: Vertical,
    fuzzy_threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> BatchQualitySummary:
    """Genera resumen de calidad de un batch de leads.

    Args:
        leads: lista de leads del batch.
        vertical: vertical al que pertenecen.
        fuzzy_threshold: umbral para considerar duplicados fuzzy.

    Returns: BatchQualitySummary con métricas agregadas.
    """
    if not leads:
        return BatchQualitySummary(
            vertical=vertical,
            total=0,
            avg_score=0.0,
            pct_with_email=0.0,
            pct_with_phone=0.0,
            pct_with_website=0.0,
            pct_with_coords=0.0,
            pct_email_validated=0.0,
            exact_duplicates=0,
            fuzzy_duplicates=0,
            high_quality=0,
            medium_quality=0,
            low_quality=0,
        )

    n = len(leads)
    qualities = [score_lead(lead) for lead in leads]
    scores = [q.score for q in qualities]

    exact_dups = count_duplicates(leads)
    deduped_exact = dedup_exact(leads)
    fuzzy_dups = len(deduped_exact) - len(dedup_fuzzy(deduped_exact, threshold=fuzzy_threshold))

    high = sum(1 for s in scores if s >= 70)
    medium = sum(1 for s in scores if 40 <= s < 70)
    low = sum(1 for s in scores if s < 40)

    return BatchQualitySummary(
        vertical=vertical,
        total=n,
        avg_score=sum(scores) / n,
        pct_with_email=sum(1 for l in leads if l.email) / n * 100,
        pct_with_phone=sum(1 for l in leads if l.telefono) / n * 100,
        pct_with_website=sum(1 for l in leads if l.sitio_web) / n * 100,
        pct_with_coords=sum(1 for l in leads if l.latitud is not None) / n * 100,
        pct_email_validated=sum(1 for l in leads if l.email_validado) / n * 100,
        exact_duplicates=exact_dups,
        fuzzy_duplicates=fuzzy_dups,
        high_quality=high,
        medium_quality=medium,
        low_quality=low,
        quality_by_lead=qualities,
    )


def compare_with_production(
    new_leads: list[Lead],
    production_csv: Path,
    threshold: int = DEFAULT_FUZZY_THRESHOLD,
) -> ProductionComparisonResult:
    """Compara leads nuevos contra el CSV consolidado de producción.

    Lee el CSV de producción, lo convierte en leads mínimos y busca duplicados.

    Args:
        new_leads: leads del nuevo batch.
        production_csv: path al CSV consolidado actual (del VPS o local).
        threshold: umbral fuzzy.

    Returns: ProductionComparisonResult con únicos y matches encontrados.
    """
    if not production_csv.exists():
        log.info("quality.no_production_csv", path=str(production_csv))
        return ProductionComparisonResult(
            new_unique=list(new_leads),
            already_in_production=[],
            total_new=len(new_leads),
            total_checked=len(new_leads),
        )

    # Leer CSV de producción y convertir a leads mínimos
    prod_rows = read_csv(production_csv)
    from ..core.models import Vertical as V
    prod_leads: list[Lead] = []
    for row in prod_rows:
        nombre = row.get("nombre", "").strip()
        partido = row.get("partido", "").strip()
        if nombre:
            try:
                vertical_val = row.get("vertical", "barrios_privados")
                vert = V(vertical_val)
            except ValueError:
                vert = V.BARRIOS_PRIVADOS
            prod_leads.append(Lead(
                nombre=nombre,
                vertical=vert,
                partido=partido or None,
            ))

    if not prod_leads:
        return ProductionComparisonResult(
            new_unique=list(new_leads),
            already_in_production=[],
            total_new=len(new_leads),
            total_checked=len(new_leads),
        )

    unique, matches = dedup_vs_production(new_leads, prod_leads, threshold=threshold)

    log.info(
        "quality.vs_production",
        new=len(new_leads),
        unique=len(unique),
        duplicates=len(matches),
    )

    return ProductionComparisonResult(
        new_unique=unique,
        already_in_production=matches,
        total_new=len(unique),
        total_checked=len(new_leads),
    )
