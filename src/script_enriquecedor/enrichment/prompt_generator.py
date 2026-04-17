"""Generación automática de prompts LLM al activar un vertical nuevo.

Crea enrichment/prompts/<vertical>.md a partir del template base
usando los campos del schema metadata del vertical activado.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Type

from pydantic import BaseModel

from ..core.metadata_schemas import get_metadata_schema
from ..core.models import Vertical, VERTICAL_DISPLAY_NAMES

PROMPTS_DIR = Path(__file__).parent / "prompts"

# Template base para generar prompts de nuevos verticales
_TEMPLATE = """\
# Prompt de extracción — {display_name}

Sos un extractor de datos B2B para seguridad electrónica corporativa.

## Tu tarea
Del contenido HTML que te paso, extraé información de contacto de **{display_name}**
en Argentina.

## Cargos objetivo
Priorizá encontrar datos de estos cargos (en orden de prioridad):
1. Gerente de Seguridad / Director de Seguridad
2. Responsable de Facilities / Infraestructura
3. Gerente General / Director Ejecutivo
4. Responsable de Compras

## Datos comunes a extraer
- nombre: nombre del establecimiento (obligatorio)
- email: email principal de contacto
- email_2: email secundario (si hay)
- email_3: email terciario (si hay)
- telefono: teléfono principal con código de área
- sitio_web: URL del sitio oficial
- direccion: calle y número
- localidad: ciudad o localidad
- partido: partido o municipio (para Buenos Aires)
- provincia: provincia argentina
- nombre_responsable: nombre del contacto clave (si aparece)
- cargo_responsable: cargo del contacto (si aparece)

## Datos específicos de {display_name} (metadata)
{metadata_fields}

## Formato de respuesta
Respondé SOLO con un JSON válido con exactamente estas claves (null si no encontrás el dato):
```json
{{
  "nombre": "...",
  "email": "...",
  "email_2": null,
  "email_3": null,
  "telefono": "...",
  "sitio_web": "...",
  "direccion": "...",
  "localidad": "...",
  "partido": "...",
  "provincia": "...",
  "nombre_responsable": null,
  "cargo_responsable": null,
  "metadata": {{
{metadata_json_example}
  }}
}}
```

## Reglas
- Solo datos que aparezcan explícitamente en el HTML. NO inventes información.
- Emails: formato estándar usuario@dominio.com
- Teléfonos: incluir código de área (ej: 011-4567-8901 o +54 11 4567-8901)
- Si el HTML no contiene suficiente información, retorná null en los campos faltantes.
- metadata debe contener solo los campos definidos arriba (null si no encontrás el dato).
"""


def _get_field_docs(schema_class: Type[BaseModel]) -> tuple[str, str]:
    """Extrae documentación de campos del schema.

    Returns:
        (metadata_fields str, metadata_json_example str)
    """
    fields = schema_class.model_fields
    field_lines = []
    json_lines = []

    for name, field_info in fields.items():
        annotation = field_info.annotation
        description = field_info.description or ""

        # Tipo legible
        type_str = _readable_type(annotation)

        # Línea de documentación
        line = f"- {name}: {type_str}"
        if description:
            line += f" — {description}"
        field_lines.append(line)

        # Línea de ejemplo JSON
        default = field_info.default
        if default is None:
            json_lines.append(f'    "{name}": null')
        elif isinstance(default, bool):
            json_lines.append(f'    "{name}": {str(default).lower()}')
        elif isinstance(default, (int, float)):
            json_lines.append(f'    "{name}": {default}')
        else:
            json_lines.append(f'    "{name}": "..."')

    return "\n".join(field_lines), ",\n".join(json_lines)


def _readable_type(annotation) -> str:
    """Convierte anotación de tipo en string legible."""
    if annotation is None:
        return "string | null"

    type_str = str(annotation)

    # Simplificar tipos complejos
    replacements = [
        ("typing.Optional[", ""),
        ("<class '", ""),
        ("'>", ""),
        ("int | None", "número entero | null"),
        ("str | None", "string | null"),
        ("float | None", "número decimal | null"),
        ("bool | None", "true/false | null"),
        ("list[str] | None", "lista de strings | null"),
        ("list[str]", "lista de strings"),
        ("int", "número entero"),
        ("str", "string"),
        ("float", "número decimal"),
        ("bool", "true/false"),
    ]
    for old, new in replacements:
        type_str = type_str.replace(old, new)

    # Limpiar Literal[...]
    if "Literal[" in type_str:
        # Extraer opciones: Literal["a", "b"] → "a" | "b"
        inner = type_str.split("Literal[")[1].rstrip("] | None")
        type_str = inner + (" | null" if "None" in str(annotation) else "")

    return type_str.strip()


def generate_prompt(vertical: Vertical, overwrite: bool = False) -> Path:
    """Genera (o regenera) el archivo de prompt para un vertical.

    Args:
        vertical: vertical para el que generar el prompt.
        overwrite: si True, sobreescribe el prompt existente.

    Returns:
        Path al archivo generado.

    Raises:
        FileExistsError: si el prompt ya existe y overwrite=False.
    """
    PROMPTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROMPTS_DIR / f"{vertical.value}.md"

    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Prompt ya existe: {output_path}. Usá overwrite=True para regenerar."
        )

    display_name = VERTICAL_DISPLAY_NAMES.get(vertical, vertical.value)
    schema_class = get_metadata_schema(vertical)
    metadata_fields, metadata_json_example = _get_field_docs(schema_class)

    content = _TEMPLATE.format(
        display_name=display_name,
        metadata_fields=metadata_fields or "_(sin campos específicos)_",
        metadata_json_example=metadata_json_example or '    "_": null',
    )

    output_path.write_text(content, encoding="utf-8")
    return output_path


def prompt_exists(vertical: Vertical) -> bool:
    """Retorna True si ya existe un prompt para el vertical."""
    return (PROMPTS_DIR / f"{vertical.value}.md").exists()


def ensure_prompt(vertical: Vertical) -> Path:
    """Genera el prompt si no existe. Retorna la ruta (nueva o existente)."""
    path = PROMPTS_DIR / f"{vertical.value}.md"
    if path.exists():
        return path
    return generate_prompt(vertical)


def generate_all_missing() -> list[Path]:
    """Genera prompts para todos los verticales que no tengan uno."""
    generated = []
    for v in Vertical:
        if not prompt_exists(v):
            path = generate_prompt(v)
            generated.append(path)
    return generated
