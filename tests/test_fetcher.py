"""Tests para fetcher HTTP (httpx) con mocks respx.

Estrategia de mocking:
- Se parchea RobotsChecker.can_fetch para siempre retornar True (fail-open).
- Se parchea Fetcher._fetch_httpx con monkeypatch para retornar HTML controlado.
- Se evita Playwright completamente en tests unitarios.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.script_enriquecedor.scraping.fetcher import Fetcher, FetchResult

SAMPLE_HTML = """
<html><body>
<h1>Club Náutico</h1>
<p>Somos un club privado con más de 500 socios activos. Contáctenos en info@club.com.ar
o llamando al 011-4567-8901. Ubicados en Av. del Lago 1234, Tigre, Buenos Aires.</p>
<p>Actividades: vela, remo, natación, paddleboard y kayak. Instalaciones premium con
quincho, piscina olímpica, canchas de tenis y fútbol. Personal de seguridad las 24 hs.</p>
<p>Historia: fundado en 1985, el club cuenta con moderna infraestructura y más de tres
décadas de trayectoria en el deporte náutico del Río de la Plata.</p>
<p>Membresías disponibles: familiar, individual y corporativa. Consulte aranceles.</p>
</body></html>
"""

SPA_HTML = """<html><body><div id="root"></div><script>window.__NEXT_DATA__={}</script></body></html>"""


@pytest.fixture
def fetcher():
    f = Fetcher()
    return f


@pytest.fixture(autouse=True)
def mock_robots():
    """Robots.txt siempre permite (fail-open) en tests."""
    from src.script_enriquecedor.scraping.robots import RobotsChecker
    with patch.object(RobotsChecker, "can_fetch", AsyncMock(return_value=True)):
        yield


@pytest.fixture(autouse=True)
def mock_rate_limiter():
    """Rate limiter no-op en tests."""
    from src.script_enriquecedor.scraping.rate_limiter import DomainRateLimiter
    with patch.object(DomainRateLimiter, "acquire", AsyncMock(return_value=None)):
        yield


class TestFetcherBasic:
    @pytest.mark.asyncio
    async def test_successful_fetch(self, fetcher):
        from src.script_enriquecedor.scraping.fetcher import Fetcher as F
        with patch.object(F, "_fetch_httpx", AsyncMock(return_value=(SAMPLE_HTML, 200))):
            result = await fetcher.fetch("https://example.com/", try_contact_subpages=False)
        assert result.status_code == 200
        assert result.has_content
        assert "Club Náutico" in result.text

    @pytest.mark.asyncio
    async def test_404_returns_result(self, fetcher):
        from src.script_enriquecedor.scraping.fetcher import Fetcher as F
        with patch.object(F, "_fetch_httpx", AsyncMock(return_value=("", 404))):
            with patch(
                "src.script_enriquecedor.scraping.fetcher._fetch_with_playwright",
                AsyncMock(return_value=("", 404)),
            ):
                result = await fetcher.fetch("https://example.com/", try_contact_subpages=False)
        assert result.status_code == 404
        assert not result.has_content

    @pytest.mark.asyncio
    async def test_spa_detected_no_content(self, fetcher):
        from src.script_enriquecedor.scraping.fetcher import Fetcher as F
        with patch.object(F, "_fetch_httpx", AsyncMock(return_value=(SPA_HTML, 200))):
            with patch(
                "src.script_enriquecedor.scraping.fetcher._fetch_with_playwright",
                AsyncMock(return_value=("", 0)),
            ):
                result = await fetcher.fetch("https://example.com/", try_contact_subpages=False)
        assert not result.has_content

    @pytest.mark.asyncio
    async def test_fetch_many_concurrency(self, fetcher):
        from src.script_enriquecedor.scraping.fetcher import Fetcher as F
        with patch.object(F, "_fetch_httpx", AsyncMock(return_value=(SAMPLE_HTML, 200))):
            urls = [f"https://ex{i}.com/" for i in range(3)]
            results = await fetcher.fetch_many(urls, concurrency=2, try_contact_subpages=False)
        assert len(results) == 3
        assert all(r.has_content for r in results)

    @pytest.mark.asyncio
    async def test_fetch_many_empty_list(self, fetcher):
        results = await fetcher.fetch_many([])
        assert results == []

    @pytest.mark.asyncio
    async def test_contact_subpage_fallback(self, fetcher):
        call_count = [0]

        async def _fake_httpx(self_inner, url: str) -> tuple[str, int]:
            call_count[0] += 1
            if "contacto" in url:
                return SAMPLE_HTML, 200
            return SPA_HTML, 200  # SPA en main page

        from src.script_enriquecedor.scraping.fetcher import Fetcher as F
        with patch.object(F, "_fetch_httpx", _fake_httpx):
            with patch(
                "src.script_enriquecedor.scraping.fetcher._fetch_with_playwright",
                AsyncMock(return_value=("", 0)),
            ):
                result = await fetcher.fetch("https://example.com/", try_contact_subpages=True)
        assert result is not None
        assert isinstance(result, FetchResult)
        assert call_count[0] >= 1

    @pytest.mark.asyncio
    async def test_invalid_url(self, fetcher):
        result = await fetcher.fetch("not-a-url")
        assert not result.has_content
        assert result.error is not None


class TestFetchResult:
    def test_has_content_true_for_long_text(self):
        result = FetchResult(url="https://x.com", status_code=200, text="A" * 400)
        assert result.has_content

    def test_has_content_false_for_short_text(self):
        result = FetchResult(url="https://x.com", status_code=200, text="short")
        assert not result.has_content

    def test_has_content_false_for_empty(self):
        result = FetchResult(url="https://x.com", status_code=200, text="")
        assert not result.has_content
