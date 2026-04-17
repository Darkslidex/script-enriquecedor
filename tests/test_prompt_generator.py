"""Tests para generación automática de prompts al activar verticales."""

import pytest
from pathlib import Path

from src.script_enriquecedor.core.models import Vertical
from src.script_enriquecedor.enrichment.prompt_generator import (
    generate_prompt,
    generate_all_missing,
    prompt_exists,
    ensure_prompt,
    PROMPTS_DIR,
)


@pytest.fixture
def tmp_prompts_dir(tmp_path, monkeypatch):
    """Redirige el directorio de prompts a un tmpdir para tests aislados."""
    import src.script_enriquecedor.enrichment.prompt_generator as pg
    monkeypatch.setattr(pg, "PROMPTS_DIR", tmp_path / "prompts")
    return tmp_path / "prompts"


class TestGeneratePrompt:
    def test_creates_file(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.HOTELES)
        assert path.exists()
        assert path.name == "hoteles.md"

    def test_content_has_vertical_name(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.HOTELES)
        content = path.read_text()
        assert "Hoteles" in content

    def test_content_has_metadata_fields(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.HOTELES)
        content = path.read_text()
        # BarriosPrivadosMetadata tiene campos zona, cantidad_lotes, etc.
        assert "metadata" in content.lower()

    def test_content_has_json_template(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.UNIVERSIDADES)
        content = path.read_text()
        assert "```json" in content
        assert '"nombre"' in content
        assert '"email"' in content

    def test_raises_if_exists_no_overwrite(self, tmp_prompts_dir):
        generate_prompt(Vertical.CLINICAS)
        with pytest.raises(FileExistsError):
            generate_prompt(Vertical.CLINICAS, overwrite=False)

    def test_overwrite_replaces_file(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.CLINICAS)
        original_mtime = path.stat().st_mtime
        import time; time.sleep(0.01)
        generate_prompt(Vertical.CLINICAS, overwrite=True)
        assert path.stat().st_mtime >= original_mtime

    def test_all_verticals_generate_without_error(self, tmp_prompts_dir):
        for v in Vertical:
            path = generate_prompt(v)
            assert path.exists()
            assert path.stat().st_size > 100  # contenido mínimo

    def test_barrios_privados_metadata_fields_present(self, tmp_prompts_dir):
        path = generate_prompt(Vertical.BARRIOS_PRIVADOS)
        content = path.read_text()
        # Campos del schema BarriosPrivadosMetadata
        assert "zona" in content
        assert "cantidad_lotes" in content


class TestPromptExists:
    def test_false_when_not_exists(self, tmp_prompts_dir):
        assert not prompt_exists(Vertical.HOTELES)

    def test_true_after_generate(self, tmp_prompts_dir):
        generate_prompt(Vertical.HOTELES)
        assert prompt_exists(Vertical.HOTELES)


class TestEnsurePrompt:
    def test_generates_if_missing(self, tmp_prompts_dir):
        path = ensure_prompt(Vertical.UNIVERSIDADES)
        assert path.exists()

    def test_returns_existing_without_regenerating(self, tmp_prompts_dir):
        path1 = ensure_prompt(Vertical.UNIVERSIDADES)
        mtime1 = path1.stat().st_mtime
        import time; time.sleep(0.01)
        path2 = ensure_prompt(Vertical.UNIVERSIDADES)
        assert path1 == path2
        assert path2.stat().st_mtime == mtime1  # no se regeneró


class TestGenerateAllMissing:
    def test_generates_all_when_none_exist(self, tmp_prompts_dir):
        generated = generate_all_missing()
        assert len(generated) == len(Vertical)

    def test_skips_existing(self, tmp_prompts_dir):
        generate_prompt(Vertical.BARRIOS_PRIVADOS)
        generate_prompt(Vertical.HOTELES)
        generated = generate_all_missing()
        assert len(generated) == len(Vertical) - 2

    def test_empty_when_all_exist(self, tmp_prompts_dir):
        for v in Vertical:
            generate_prompt(v)
        generated = generate_all_missing()
        assert generated == []
