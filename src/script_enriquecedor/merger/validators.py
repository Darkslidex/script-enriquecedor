"""Validación del schema Prisma antes de exportar el CSV unificado.

Reglas:
  - nombre es obligatorio (sin él no se puede insertar en la DB)
  - vertical debe ser uno de los valores del enum Prisma
  - email debe tener @ si está presente
  - estado_comercial debe ser un valor válido

Uso:
    issues = validate_row(row)
    if issues:
        # row tiene problemas pero se exporta igual con flag _invalid=True
"""

from __future__ import annotations

# Valores válidos del enum Prisma (deben coincidir con schema.prisma)
_VALID_VERTICALS = {
    "barrios_privados", "hoteles", "universidades", "entes_estatales",
    "consulados", "embajadas", "droguerias", "clinicas", "depositos_fiscales",
    "parques_industriales", "logisticas", "empresas", "plantas_industriales",
    "terminales_portuarias", "aeronauticas",
}

_VALID_ESTADOS = {
    "SIN_CONTACTAR", "CONTACTADO", "EN_NEGOCIACION", "CLIENTE", "DESCARTADO",
}


def validate_row(row: dict[str, str]) -> list[str]:
    """Retorna lista de problemas encontrados. Lista vacía = row válida."""
    issues: list[str] = []

    nombre = row.get("nombre", "").strip()
    if not nombre:
        issues.append("nombre_vacio")

    vertical = row.get("vertical", "").strip().lower()
    if vertical and vertical not in _VALID_VERTICALS:
        issues.append(f"vertical_invalido:{vertical}")

    email = row.get("email", "").strip()
    if email and "@" not in email:
        issues.append(f"email_malformado:{email[:30]}")

    estado = row.get("estado_comercial", "").strip()
    if estado and estado not in _VALID_ESTADOS:
        issues.append(f"estado_invalido:{estado}")

    return issues


def validate_batch(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Divide filas en válidas e inválidas.

    Returns:
        (valid_rows, invalid_rows) — los inválidos llevan campo _issues con los problemas.
    """
    valid: list[dict[str, str]] = []
    invalid: list[dict[str, str]] = []

    for row in rows:
        issues = validate_row(row)
        if not issues:
            valid.append(row)
        else:
            invalid.append({**row, "_issues": "; ".join(issues)})

    return valid, invalid


def summarize_validation(valid: list, invalid: list) -> str:
    """Retorna un resumen legible de la validación."""
    total = len(valid) + len(invalid)
    if not total:
        return "Sin registros para validar."
    pct = 100 * len(valid) // total if total else 0
    lines = [
        f"Válidos: {len(valid)}/{total} ({pct}%)",
    ]
    if invalid:
        from collections import Counter
        issue_counts: Counter = Counter()
        for row in invalid:
            for issue in row.get("_issues", "").split("; "):
                if issue:
                    issue_counts[issue.split(":")[0]] += 1
        lines.append("Problemas encontrados:")
        for issue_type, count in issue_counts.most_common():
            lines.append(f"  · {issue_type}: {count}")
    return "\n".join(lines)
