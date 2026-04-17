"""Tests para modelos core (Vertical, EstadoComercial, Lead)."""

import pytest
from pydantic import ValidationError

from src.script_enriquecedor.core.models import (
    EstadoComercial,
    Lead,
    Vertical,
)


class TestVertical:
    def test_all_15_verticals_exist(self):
        assert len(Vertical) == 15

    def test_barrios_privados_value(self):
        assert Vertical.BARRIOS_PRIVADOS.value == "barrios_privados"

    def test_vertical_from_string(self):
        v = Vertical("barrios_privados")
        assert v is Vertical.BARRIOS_PRIVADOS


class TestEstadoComercial:
    def test_default_is_sin_contactar(self):
        lead = Lead(nombre="Test", vertical=Vertical.BARRIOS_PRIVADOS)
        assert lead.estado_comercial == EstadoComercial.SIN_CONTACTAR

    def test_all_states_accessible(self):
        assert EstadoComercial.SIN_CONTACTAR
        assert EstadoComercial.CONTACTADO
        assert EstadoComercial.EN_NEGOCIACION
        assert EstadoComercial.CLIENTE
        assert EstadoComercial.DESCARTADO


class TestLead:
    def test_minimal_lead(self):
        lead = Lead(nombre="Club Náutico San Isidro", vertical=Vertical.BARRIOS_PRIVADOS)
        assert lead.nombre == "Club Náutico San Isidro"
        assert lead.vertical == Vertical.BARRIOS_PRIVADOS
        assert lead.email is None
        assert lead.email_validado is False
        assert lead.metadata == {}

    def test_full_lead(self):
        lead = Lead(
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
            metadata={"tipo_barrio": "cerrado", "cantidad_lotes": 450},
        )
        assert lead.email == "info@torresdellago.com.ar"
        assert lead.latitud == pytest.approx(-34.4587)
        assert lead.metadata["tipo_barrio"] == "cerrado"

    def test_invalid_email_raises(self):
        with pytest.raises(ValidationError):
            Lead(
                nombre="Test",
                vertical=Vertical.BARRIOS_PRIVADOS,
                email="not-an-email",
            )

    def test_model_copy_update(self):
        lead = Lead(nombre="Original", vertical=Vertical.BARRIOS_PRIVADOS)
        updated = lead.model_copy(update={"partido": "Tigre"})
        assert updated.partido == "Tigre"
        assert lead.partido is None  # original not mutated

    def test_estado_comercial_assignment(self):
        lead = Lead(
            nombre="Test",
            vertical=Vertical.BARRIOS_PRIVADOS,
            estado_comercial=EstadoComercial.EN_NEGOCIACION,
        )
        assert lead.estado_comercial == EstadoComercial.EN_NEGOCIACION

    def test_metadata_default_factory(self):
        a = Lead(nombre="A", vertical=Vertical.BARRIOS_PRIVADOS)
        b = Lead(nombre="B", vertical=Vertical.BARRIOS_PRIVADOS)
        a.metadata["key"] = "val"
        assert "key" not in b.metadata

    def test_email_2_email_3_optional(self):
        lead = Lead(
            nombre="Test",
            vertical=Vertical.BARRIOS_PRIVADOS,
            email_2="sec@test.com",
            email_3="ter@test.com",
        )
        assert lead.email_2 == "sec@test.com"
        assert lead.email_3 == "ter@test.com"


class TestMetadataSchemas:
    def test_barrios_privados_schema(self):
        from src.script_enriquecedor.core.metadata_schemas import get_metadata_schema

        schema = get_metadata_schema(Vertical.BARRIOS_PRIVADOS)
        instance = schema(
            zona="Norte GBA",
            cantidad_lotes=300,
        )
        assert instance.zona == "Norte GBA"
        assert instance.cantidad_lotes == 300

    def test_all_verticals_have_schema(self):
        from src.script_enriquecedor.core.metadata_schemas import get_metadata_schema

        for v in Vertical:
            schema = get_metadata_schema(v)
            assert schema is not None
