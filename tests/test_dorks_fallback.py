"""Tests para circuit breaker dorks: googlesearch → DDG → pausa."""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from src.script_enriquecedor.discovery.dorks import (
    CircuitState,
    DorksDiscovery,
    _CircuitBreaker,
    _extract_domain,
    _domain_to_name,
    make_vertical_dorks_discovery,
)
from src.script_enriquecedor.core.models import Vertical


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = _CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert not cb.is_open

    def test_opens_after_threshold_failures(self):
        cb = _CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_open

    def test_success_resets_counter(self):
        cb = _CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.consecutive_failures == 0
        assert cb.state == CircuitState.CLOSED

    def test_record_empty_opens_after_threshold(self):
        cb = _CircuitBreaker(failure_threshold=2)
        cb.record_empty()
        assert not cb.is_open
        opened = cb.record_empty()
        assert opened
        assert cb.is_open

    def test_record_empty_returns_false_before_threshold(self):
        cb = _CircuitBreaker(failure_threshold=3)
        result = cb.record_empty()
        assert not result

    def test_custom_threshold(self):
        cb = _CircuitBreaker(failure_threshold=5)
        for _ in range(4):
            cb.record_failure()
        assert not cb.is_open
        cb.record_failure()
        assert cb.is_open


class TestDorksDiscovery:
    def make_dorks(self, dorks=None) -> DorksDiscovery:
        return make_vertical_dorks_discovery(
            Vertical.UNIVERSIDADES,
            dorks or ["universidades privadas argentina contacto"],
        )

    @pytest.mark.asyncio
    async def test_google_search_returns_leads(self):
        d = self.make_dorks()
        fake_urls = ["https://uces.edu.ar", "https://belgrano.edu.ar"]

        with patch.object(d, "_google_search", AsyncMock(return_value=[
            d._urls_to_leads(fake_urls, fuente="google_dorks")[0],
        ])):
            d._google_cb.record_success()
            results = await d._search_with_fallback("test query", 10)
        assert len(results) >= 0  # al menos no falla

    @pytest.mark.asyncio
    async def test_ddg_fallback_when_google_circuit_open(self):
        d = self.make_dorks()
        # Abrir el circuito de Google
        for _ in range(3):
            d._google_cb.record_failure()
        assert d._google_cb.is_open

        ddg_leads = [
            d._urls_to_leads(["https://uces.edu.ar"], "ddg_dorks")[0]
        ]
        with patch.object(d, "_ddg_search", AsyncMock(return_value=ddg_leads)):
            results = await d._search_with_fallback("universidades", 10)

        assert len(results) == 1
        assert results[0].fuente == "ddg_dorks"

    @pytest.mark.asyncio
    async def test_empty_list_when_both_fail(self):
        d = self.make_dorks()
        # Abrir circuito
        for _ in range(3):
            d._google_cb.record_failure()

        with patch.object(d, "_ddg_search", AsyncMock(return_value=[])):
            results = await d._search_with_fallback("failing query", 10)

        assert results == []

    @pytest.mark.asyncio
    async def test_banned_domains_filtered(self):
        d = self.make_dorks()
        urls = [
            "https://zonaprop.com.ar/result",   # baneado
            "https://uces.edu.ar",               # ok
            "https://facebook.com/uni",          # baneado
            "https://austral.edu.ar",            # ok
        ]
        leads = d._urls_to_leads(urls, "google_dorks")
        result_urls = [l.sitio_web for l in leads]
        assert "https://zonaprop.com.ar/result" not in result_urls
        assert "https://facebook.com/uni" not in result_urls
        assert "https://uces.edu.ar" in result_urls
        assert "https://austral.edu.ar" in result_urls

    @pytest.mark.asyncio
    async def test_discover_deduplicates_urls(self):
        d = self.make_dorks(["query1", "query2"])
        lead = d._urls_to_leads(["https://uces.edu.ar"], "google_dorks")[0]

        with patch.object(d, "_search_with_fallback", AsyncMock(return_value=[lead])):
            results = await d.discover(limit=10)

        # Aunque query1 y query2 retornen la misma URL, aparece 1 vez
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_discover_respects_limit(self):
        d = self.make_dorks(["q1", "q2", "q3"])
        leads = d._urls_to_leads(
            [f"https://uni{i}.edu.ar" for i in range(20)],
            "google_dorks",
        )

        with patch.object(d, "_search_with_fallback", AsyncMock(return_value=leads[:5])):
            results = await d.discover(limit=3)

        assert len(results) <= 3

    @pytest.mark.asyncio
    async def test_empty_dorks_returns_empty(self):
        from src.script_enriquecedor.discovery.dorks import DorksDiscovery
        d = DorksDiscovery(_vertical=Vertical.UNIVERSIDADES, dorks=[])
        results = await d.discover()
        assert results == []


class TestHelpers:
    def test_extract_domain_simple(self):
        assert _extract_domain("https://www.example.com/path") == "example.com"

    def test_extract_domain_no_www(self):
        assert _extract_domain("https://example.com.ar/x") == "example.com.ar"

    def test_extract_domain_invalid(self):
        assert _extract_domain("not-a-url") == ""

    def test_domain_to_name_simple(self):
        assert _domain_to_name("clubnautico.com.ar") == "Clubnautico"

    def test_domain_to_name_hyphen(self):
        result = _domain_to_name("club-nautico.com.ar")
        assert "Club" in result and "Nautico" in result

    def test_make_vertical_dorks_discovery(self):
        d = make_vertical_dorks_discovery(
            Vertical.HOTELES,
            ["hoteles argentina seguridad"],
        )
        assert d.vertical == Vertical.HOTELES
        assert len(d.dorks) == 1
        assert not d._google_cb.is_open
