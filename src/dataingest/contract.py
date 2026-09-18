"""Contrato de dados.

Um contrato declara o que a origem promete entregar: quais colunas existem, de
que tipo, quais podem ser nulas e qual e a chave de negocio. Registros que
violam o contrato sao separados em vez de derrubar a carga, porque numa
ingestao de dez mil linhas, tres linhas ruins nao podem impedir as outras
9.997 de chegar.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import Any

Record = dict[str, Any]


class FieldType(StrEnum):
    """Tipos aceitos no contrato."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


_PARSERS: dict[FieldType, Any] = {
    FieldType.STRING: lambda v: v if isinstance(v, str) else str(v),
    FieldType.INTEGER: lambda v: int(str(v).strip()),
    FieldType.FLOAT: lambda v: float(str(v).strip().replace(",", ".")),
    FieldType.DATE: lambda v: v if isinstance(v, date) else date.fromisoformat(str(v).strip()),
    FieldType.TIMESTAMP: lambda v: (
        v if isinstance(v, datetime) else datetime.fromisoformat(str(v).strip())
    ),
}


def _parse_boolean(value: Any) -> bool:
    """Converte representacoes textuais comuns de booleano."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "sim", "s", "yes", "y", "t"}:
        return True
    if text in {"false", "0", "nao", "n", "no", "f"}:
        return False
    raise ValueError(f"valor booleano nao reconhecido: {value!r}")


_PARSERS[FieldType.BOOLEAN] = _parse_boolean


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Especificacao de uma coluna."""

    name: str
    type: FieldType
    nullable: bool = True
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("nome de campo vazio")


@dataclass(frozen=True, slots=True)
class Violation:
    """Motivo pelo qual um registro foi rejeitado."""

    row: int
    field: str
    reason: str

    def __str__(self) -> str:
        return f"linha {self.row}, campo '{self.field}': {self.reason}"


@dataclass(slots=True)
class ValidationResult:
    """Separacao entre o que passou e o que foi rejeitado."""

    valid: list[Record] = field(default_factory=list)
    violations: list[Violation] = field(default_factory=list)

    @property
    def rejected_rows(self) -> set[int]:
        """Linhas que tiveram ao menos uma violacao."""
        return {v.row for v in self.violations}

    @property
    def reject_ratio(self) -> float:
        """Fracao de linhas rejeitadas sobre o total processado."""
        total = len(self.valid) + len(self.rejected_rows)
        return len(self.rejected_rows) / total if total else 0.0


@dataclass(frozen=True, slots=True)
class DataContract:
    """Contrato de um conjunto de dados."""

    name: str
    fields: tuple[FieldSpec, ...]
    business_key: tuple[str, ...]
    """Colunas que identificam o registro na origem, usadas na deduplicacao."""

    watermark_field: str | None = None
    """Coluna monotonica usada para carga incremental, por exemplo 'atualizado_em'."""

    def __post_init__(self) -> None:
        if not self.fields:
            raise ValueError("contrato sem campos")

        names = [f.name for f in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("ha campo duplicado no contrato")
        if not self.business_key:
            raise ValueError("contrato sem chave de negocio")

        unknown = set(self.business_key) - set(names)
        if unknown:
            raise ValueError(f"chave de negocio cita campo inexistente: {sorted(unknown)}")
        if self.watermark_field and self.watermark_field not in names:
            raise ValueError(f"watermark cita campo inexistente: {self.watermark_field}")

        for key in self.business_key:
            spec = next(f for f in self.fields if f.name == key)
            if spec.nullable:
                raise ValueError(f"campo de chave de negocio nao pode ser nulavel: {key}")

    @property
    def field_names(self) -> tuple[str, ...]:
        """Nomes das colunas na ordem do contrato."""
        return tuple(f.name for f in self.fields)

    def validate(self, records: Iterable[Record]) -> ValidationResult:
        """Valida e converte os registros, separando os que violam o contrato."""
        result = ValidationResult()

        for row, record in enumerate(records, start=1):
            parsed: Record = {}
            problems: list[Violation] = []

            for spec in self.fields:
                raw = record.get(spec.name)

                if raw is None or (isinstance(raw, str) and not raw.strip()):
                    if not spec.nullable:
                        problems.append(Violation(row, spec.name, "campo obrigatorio ausente"))
                    parsed[spec.name] = None
                    continue

                try:
                    parsed[spec.name] = _PARSERS[spec.type](raw)
                except (ValueError, TypeError) as exc:
                    problems.append(
                        Violation(row, spec.name, f"nao convertivel para {spec.type}: {exc}")
                    )

            if problems:
                result.violations.extend(problems)
            else:
                result.valid.append(parsed)

        return result
