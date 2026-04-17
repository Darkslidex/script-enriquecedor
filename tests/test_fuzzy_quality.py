"""Tests para fuzzy dedup y quality summary (Fase 3)."""

import pytest
from pathlib import Path

from src.script_enriquecedor.core.dedup import (
    DEFAULT_FUZZY_THRESHOLD,
    FuzzyMatch,
    dedup_fuzzy,
    dedup_vs_production,
    find_fuzzy_matches,
)
from src.script_enriquecedor.core.models import Lead, Vertical
from src.script_enriquecedor.storage.quality import (
    BatchQualitySummary,
    ProductionComparisonResult,
    compare_with_production,
    score_lead,
    summarize_batch,
)


def _lead(nombre: str, partido: str = "", email: str = "", telefono: str = "") -> Lead:
    return Lead(
        nombre=nombre,
        vertical=Vertical.BARRIOS_PRIVADOS,
        partido=partido or None,
        email=email or None,  # type: ignore[arg-type]
        telefono=telefono or None,
    )


# ── FuzzyDedup ─────────────────────────────────────────────────────────────────

class TestDedupFuzzy:
    def test_clear_match_removed(self):
        """token_sort_ratio("hotel intercontinental", "intercontinental hotel") = 100"""
        leads = [
            _lead("Hotel Intercontinental", "Retiro"),
            _lead("Intercontinental Hotel", "Retiro"),
        ]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 1
        assert result[0].nombre == "Hotel Intercontinental"

    def test_partial_match_kept_below_threshold(self):
        """token_sort_ratio("nordelta", "nordelta country") = 67 → distintos"""
        leads = [_lead("Nordelta", "Tigre"), _lead("Nordelta Country", "Tigre")]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 2

    def test_high_similarity_match_removed(self):
        """clinica bazterrica vs clinica bazterrica sa → 92 → duplicado"""
        leads = [
            _lead("Clinica Bazterrica", "Retiro"),
            _lead("Clinica Bazterrica SA", "Retiro"),
        ]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 1

    def test_different_partido_kept(self):
        """Misma empresa, distintos partidos → no son duplicados."""
        leads = [
            _lead("Parque Industrial Pilar", "Pilar"),
            _lead("Parque Industrial de Pilar", "Lujan"),
        ]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 2

    def test_same_partido_match_removed(self):
        """token_sort_ratio("parque industrial pilar", "parque industrial de pilar") = 94"""
        leads = [
            _lead("Parque Industrial Pilar", "Pilar"),
            _lead("Parque Industrial de Pilar", "Pilar"),
        ]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 1

    def test_empty_partido_treated_as_wildcard(self):
        """Si uno no tiene partido, se considera posible duplicado."""
        leads = [
            _lead("Clinica Bazterrica SA", ""),
            _lead("Clinica Bazterrica", "Retiro"),
        ]
        result = dedup_fuzzy(leads, threshold=85)
        assert len(result) == 1

    def test_empty_list(self):
        assert dedup_fuzzy([]) == []

    def test_single_lead(self):
        leads = [_lead("Nordelta")]
        assert len(dedup_fuzzy(leads)) == 1

    def test_preserves_first_occurrence(self):
        a = _lead("Hotel Intercontinental", "Retiro")
        b = _lead("Intercontinental Hotel", "Retiro")
        result = dedup_fuzzy([a, b], threshold=85)
        assert result[0] is a

    def test_custom_threshold_permissive(self):
        """Con threshold bajo, más cosas se deducan."""
        leads = [
            _lead("Hotel Alvear", "Retiro"),
            _lead("Hotel Alvear Palace", "Retiro"),  # score 77
        ]
        strict = dedup_fuzzy(leads, threshold=85)
        permissive = dedup_fuzzy(leads, threshold=70)
        assert len(strict) == 2
        assert len(permissive) == 1


class TestFindFuzzyMatches:
    def test_finds_high_similarity_pair(self):
        leads = [
            _lead("Hotel Intercontinental", "Retiro"),
            _lead("Intercontinental Hotel", "Retiro"),
            _lead("Nordelta", "Tigre"),
        ]
        matches = find_fuzzy_matches(leads, threshold=85)
        assert len(matches) == 1
        assert matches[0].score == 100.0

    def test_sorted_by_score_desc(self):
        leads = [
            _lead("Clinica Bazterrica", "Retiro"),     # con Bazterrica SA → 92
            _lead("Clinica Bazterrica SA", "Retiro"),
            _lead("Hotel Intercontinental", ""),        # con Intercontinental → 100
            _lead("Intercontinental Hotel", ""),
        ]
        matches = find_fuzzy_matches(leads, threshold=85)
        assert matches[0].score >= matches[-1].score

    def test_empty_list(self):
        assert find_fuzzy_matches([]) == []

    def test_match_type_nombre_plus_partido(self):
        leads = [
            _lead("Clinica Bazterrica", "Retiro"),
            _lead("Clinica Bazterrica SA", "Retiro"),
        ]
        matches = find_fuzzy_matches(leads, threshold=85)
        assert len(matches) == 1
        assert "partido" in matches[0].match_type


class TestDedupVsProduction:
    def test_unique_lead_passes_through(self):
        new = [_lead("Hotel Nuevo", "Palermo")]
        prod = [_lead("Hotel Viejo", "Tigre")]
        unique, matches = dedup_vs_production(new, prod, threshold=85)
        assert len(unique) == 1
        assert len(matches) == 0

    def test_exact_match_filtered(self):
        """Exact matches se eliminan en el paso 1 (key set).
        No aparecen en matches (esos son para revisión manual de fuzzy)."""
        lead = _lead("Nordelta", "Tigre")
        new = [lead]
        prod = [_lead("Nordelta", "Tigre")]
        unique, matches = dedup_vs_production(new, prod, threshold=85)
        assert len(unique) == 0  # filtrado como duplicado
        # matches puede ser 0 (filtrado antes del fuzzy pass) — comportamiento esperado

    def test_fuzzy_match_vs_production(self):
        new = [_lead("Hotel Intercontinental", "Retiro")]
        prod = [_lead("Intercontinental Hotel", "Retiro")]
        unique, matches = dedup_vs_production(new, prod, threshold=85)
        assert len(unique) == 0
        assert len(matches) == 1
        assert matches[0].match_type == "vs_production"

    def test_empty_production(self):
        new = [_lead("Hotel Nuevo")]
        unique, matches = dedup_vs_production(new, [], threshold=85)
        assert len(unique) == 1
        assert len(matches) == 0

    def test_empty_new_leads(self):
        prod = [_lead("Nordelta", "Tigre")]
        unique, matches = dedup_vs_production([], prod, threshold=85)
        assert unique == []
        assert matches == []


# ── Quality ────────────────────────────────────────────────────────────────────

class TestScoreLead:
    def test_full_lead_scores_high(self):
        lead = Lead(
            nombre="Torres del Lago",
            vertical=Vertical.BARRIOS_PRIVADOS,
            email="info@torresdellago.com.ar",
            telefono="011-4567-8901",
            sitio_web="https://torresdellago.com.ar",
            partido="Pilar",
            localidad="Pilar",
            latitud=-34.45,
            longitud=-58.91,
            email_validado=True,
        )
        q = score_lead(lead)
        assert q.score == 100.0
        assert q.has_critical
        assert q.missing_fields == []

    def test_minimal_lead_scores_low(self):
        lead = _lead("Sin Datos")
        q = score_lead(lead)
        assert q.score < 30
        assert not q.has_critical
        assert "email" in q.missing_fields

    def test_email_only_scores_25(self):
        lead = Lead(
            nombre="X",
            vertical=Vertical.BARRIOS_PRIVADOS,
            email="x@x.com",
        )
        q = score_lead(lead)
        assert q.score == pytest.approx(25.0, abs=1)
        assert q.has_critical

    def test_missing_fields_reported(self):
        lead = Lead(nombre="Test", vertical=Vertical.BARRIOS_PRIVADOS)
        q = score_lead(lead)
        assert "email" in q.missing_fields
        assert "telefono" in q.missing_fields
        assert "sitio_web" in q.missing_fields


class TestSummarizeBatch:
    def _batch(self) -> list[Lead]:
        return [
            Lead(nombre=f"Lead {i}", vertical=Vertical.BARRIOS_PRIVADOS,
                 email=f"l{i}@x.com" if i % 2 == 0 else None,
                 telefono="011-1234" if i % 3 == 0 else None,
                 sitio_web="https://x.com" if i % 4 == 0 else None)
            for i in range(10)
        ]

    def test_total_count(self):
        s = summarize_batch(self._batch(), Vertical.BARRIOS_PRIVADOS)
        assert s.total == 10

    def test_email_pct_correct(self):
        s = summarize_batch(self._batch(), Vertical.BARRIOS_PRIVADOS)
        # 0,2,4,6,8 tienen email → 50%
        assert s.pct_with_email == pytest.approx(50.0)

    def test_empty_batch(self):
        s = summarize_batch([], Vertical.BARRIOS_PRIVADOS)
        assert s.total == 0
        assert s.avg_score == 0.0

    def test_upload_ready_requires_min_leads(self):
        few = [Lead(nombre=f"L{i}", vertical=Vertical.BARRIOS_PRIVADOS,
                    email=f"l{i}@x.com") for i in range(3)]
        s = summarize_batch(few, Vertical.BARRIOS_PRIVADOS)
        assert not s.upload_ready  # < 5 leads

    def test_counts_exact_duplicates(self):
        leads = [_lead("Nordelta", "Tigre"), _lead("Nordelta", "Tigre"), _lead("El Canton")]
        s = summarize_batch(leads, Vertical.BARRIOS_PRIVADOS)
        assert s.exact_duplicates == 1


class TestCompareWithProduction:
    def test_no_production_csv_returns_all_new(self, tmp_path):
        new = [_lead("Hotel Nuevo")]
        result = compare_with_production(new, tmp_path / "missing.csv")
        assert len(result.new_unique) == 1
        assert result.pct_new == pytest.approx(100.0)

    def test_finds_duplicate_in_production_csv(self, tmp_path):
        from src.script_enriquecedor.storage.csv_writer import write_csv

        prod_csv = tmp_path / "consolidated.csv"
        prod_leads = [
            Lead(nombre="Hotel Intercontinental", vertical=Vertical.HOTELES,
                 partido="Retiro"),
        ]
        write_csv(prod_leads, prod_csv)

        new_leads = [
            Lead(nombre="Intercontinental Hotel", vertical=Vertical.HOTELES,
                 partido="Retiro"),
            Lead(nombre="Hotel Nuevo", vertical=Vertical.HOTELES),
        ]
        result = compare_with_production(new_leads, prod_csv, threshold=85)
        assert len(result.new_unique) == 1
        assert result.new_unique[0].nombre == "Hotel Nuevo"
        assert len(result.already_in_production) == 1
