"""Tests críticos: verifica que el CSV generado sea compatible con prisma/seed.ts.

Mockea el parser de seed.ts con un validador Python equivalente.
Valida headers exactos, tipos de datos, formato de metadata JSON string.
"""

import csv
import io
import json
from pathlib import Path
from datetime import datetime, timezone

import pytest

from src.script_enriquecedor.core.models import Lead, Vertical, EstadoComercial
from src.script_enriquecedor.storage.csv_writer import (
    CSV_HEADERS,
    write_csv,
    append_csv,
    read_csv,
)

# ── Headers exactos esperados por prisma/seed.ts ────────────────────────────
EXPECTED_HEADERS = [
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


def _sample_lead(**kwargs) -> Lead:
    defaults = dict(
        nombre="Torres del Lago",
        vertical=Vertical.BARRIOS_PRIVADOS,
        email="info@torresdellago.com.ar",
        telefono="011-4567-8901",
        sitio_web="https://torresdellago.com.ar",
        localidad="Pilar",
        partido="Pilar",
        provincia="Buenos Aires",
        pais="Argentina",
        latitud=-34.4587,
        longitud=-58.9148,
        metadata={"tipo_barrio": "cerrado", "lotes": 450},
    )
    defaults.update(kwargs)
    return Lead(**defaults)


def _write_to_string(leads: list[Lead]) -> str:
    """Escribe CSV a string en memoria para inspección."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_HEADERS)
    writer.writeheader()
    from src.script_enriquecedor.storage.csv_writer import _lead_to_row
    for lead in leads:
        writer.writerow(_lead_to_row(lead))
    return buf.getvalue()


class TestHeadersExact:
    def test_csv_headers_match_expected(self):
        assert CSV_HEADERS == EXPECTED_HEADERS

    def test_written_csv_has_exact_headers(self, tmp_path):
        path = tmp_path / "test.csv"
        write_csv([_sample_lead()], path)
        with path.open() as f:
            reader = csv.DictReader(f)
            assert list(reader.fieldnames) == EXPECTED_HEADERS


class TestDataTypes:
    def test_email_validado_is_lowercase_bool(self):
        content = _write_to_string([_sample_lead(email_validado=True)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["email_validado"] in ("true", "false")
        assert row["email_validado"] == "true"

    def test_email_validado_false(self):
        content = _write_to_string([_sample_lead(email_validado=False)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["email_validado"] == "false"

    def test_latitud_longitud_format(self):
        content = _write_to_string([_sample_lead(latitud=-34.458712, longitud=-58.914801)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        # Debe tener 6 decimales
        assert "." in row["latitud"]
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        assert lat == pytest.approx(-34.458712, abs=1e-5)
        assert lon == pytest.approx(-58.914801, abs=1e-5)

    def test_null_latlong_is_empty_string(self):
        content = _write_to_string([_sample_lead(latitud=None, longitud=None)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["latitud"] == ""
        assert row["longitud"] == ""

    def test_fecha_enriquecimiento_iso_format(self):
        dt = datetime(2025, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
        content = _write_to_string([_sample_lead(fecha_enriquecimiento=dt)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["fecha_enriquecimiento"] == "2025-06-15T12:30:00Z"

    def test_vertical_is_snake_case_value(self):
        content = _write_to_string([_sample_lead(vertical=Vertical.BARRIOS_PRIVADOS)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["vertical"] == "barrios_privados"

    def test_estado_comercial_is_uppercase(self):
        content = _write_to_string([_sample_lead(estado_comercial=EstadoComercial.SIN_CONTACTAR)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["estado_comercial"] == "SIN_CONTACTAR"


class TestMetadataField:
    def test_metadata_is_valid_json_string(self):
        meta = {"tipo_barrio": "cerrado", "lotes": 450, "amenities": ["piscina"]}
        content = _write_to_string([_sample_lead(metadata=meta)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        parsed = json.loads(row["metadata"])
        assert parsed == meta

    def test_empty_metadata_is_empty_string(self):
        content = _write_to_string([_sample_lead(metadata={})])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        assert row["metadata"] == ""

    def test_metadata_unicode_preserved(self):
        meta = {"nombre_completo": "Barrio Náutico del Río"}
        content = _write_to_string([_sample_lead(metadata=meta)])
        reader = csv.DictReader(io.StringIO(content))
        row = next(reader)
        parsed = json.loads(row["metadata"])
        assert parsed["nombre_completo"] == "Barrio Náutico del Río"


class TestFileOperations:
    def test_write_and_read_roundtrip(self, tmp_path):
        path = tmp_path / "leads.csv"
        leads = [_sample_lead(nombre=f"Lead {i}") for i in range(5)]
        written = write_csv(leads, path)
        assert written == 5

        rows = read_csv(path)
        assert len(rows) == 5
        assert rows[0]["nombre"] == "Lead 0"

    def test_append_adds_rows(self, tmp_path):
        path = tmp_path / "leads.csv"
        write_csv([_sample_lead(nombre="A")], path)
        append_csv([_sample_lead(nombre="B")], path)

        rows = read_csv(path)
        assert len(rows) == 2
        assert rows[1]["nombre"] == "B"

    def test_append_to_nonexistent_creates_with_headers(self, tmp_path):
        path = tmp_path / "new.csv"
        count = append_csv([_sample_lead()], path)
        assert count == 1
        assert path.exists()
        with path.open() as f:
            reader = csv.DictReader(f)
            assert list(reader.fieldnames) == EXPECTED_HEADERS

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "nested" / "deep" / "leads.csv"
        write_csv([_sample_lead()], path)
        assert path.exists()

    def test_read_nonexistent_returns_empty(self, tmp_path):
        path = tmp_path / "missing.csv"
        assert read_csv(path) == []

    def test_optional_fields_empty_string(self, tmp_path):
        path = tmp_path / "min.csv"
        lead = Lead(nombre="Minimal", vertical=Vertical.BARRIOS_PRIVADOS)
        write_csv([lead], path)
        rows = read_csv(path)
        row = rows[0]
        assert row["email"] == ""
        assert row["telefono"] == ""
        assert row["latitud"] == ""
        assert row["metadata"] == ""
