"""Tests para lógica de deduplicación."""

import pytest

from src.script_enriquecedor.core.dedup import (
    count_duplicates,
    dedup_exact,
    find_duplicate_groups,
)
from src.script_enriquecedor.core.models import Lead, Vertical


def _lead(nombre: str, partido: str = "") -> Lead:
    return Lead(nombre=nombre, vertical=Vertical.BARRIOS_PRIVADOS, partido=partido or None)


class TestDedupExact:
    def test_no_duplicates(self):
        leads = [_lead("El Cantón", "Escobar"), _lead("Nordelta", "Tigre")]
        result = dedup_exact(leads)
        assert len(result) == 2

    def test_exact_duplicate_removed(self):
        leads = [_lead("Nordelta", "Tigre"), _lead("Nordelta", "Tigre")]
        result = dedup_exact(leads)
        assert len(result) == 1
        assert result[0].nombre == "Nordelta"

    def test_case_insensitive(self):
        leads = [_lead("Nordelta", "Tigre"), _lead("NORDELTA", "TIGRE")]
        result = dedup_exact(leads)
        assert len(result) == 1

    def test_accent_insensitive(self):
        leads = [_lead("El Cantón", "Escobar"), _lead("El Canton", "Escobar")]
        result = dedup_exact(leads)
        assert len(result) == 1

    def test_different_partido_not_deduped(self):
        leads = [_lead("Los Eucaliptos", "Pilar"), _lead("Los Eucaliptos", "Tigre")]
        result = dedup_exact(leads)
        assert len(result) == 2

    def test_empty_list(self):
        assert dedup_exact([]) == []

    def test_single_lead(self):
        leads = [_lead("Puertos del Lago")]
        assert len(dedup_exact(leads)) == 1

    def test_preserves_first_occurrence(self):
        a = _lead("Nordelta", "Tigre")
        b = _lead("nordelta", "tigre")
        result = dedup_exact([a, b])
        assert result[0] is a

    def test_preserves_order(self):
        leads = [
            _lead("C", "Tigre"),
            _lead("A", "Pilar"),
            _lead("B", "Escobar"),
        ]
        result = dedup_exact(leads)
        assert [l.nombre for l in result] == ["C", "A", "B"]

    def test_whitespace_normalized(self):
        leads = [_lead("El  Cantón", "Escobar"), _lead("El Cantón", "Escobar")]
        result = dedup_exact(leads)
        assert len(result) == 1


class TestCountDuplicates:
    def test_no_duplicates(self):
        leads = [_lead("A"), _lead("B"), _lead("C")]
        assert count_duplicates(leads) == 0

    def test_one_pair(self):
        leads = [_lead("A"), _lead("A"), _lead("B")]
        assert count_duplicates(leads) == 1

    def test_triple(self):
        leads = [_lead("A"), _lead("A"), _lead("A")]
        assert count_duplicates(leads) == 2


class TestFindDuplicateGroups:
    def test_finds_groups(self):
        leads = [
            _lead("Nordelta", "Tigre"),
            _lead("nordelta", "tigre"),
            _lead("El Cantón", "Escobar"),
        ]
        groups = find_duplicate_groups(leads)
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_no_duplicates_empty_result(self):
        leads = [_lead("A"), _lead("B")]
        assert find_duplicate_groups(leads) == []
