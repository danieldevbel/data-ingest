"""Checagens de qualidade sobre o lote ja validado.

O contrato garante forma. As checagens garantem sentido: valor dentro da faixa
esperada, chave sem duplicata, lote nao vazio, volume compativel com o
historico. Cada checagem tem severidade: `ERROR` interrompe a carga, `WARNING`
e registrada e segue.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from dataingest.contract import Record


class Severity(StrEnum):
    """Gravidade de uma checagem."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Resultado de uma checagem."""

    name: str
    passed: bool
    severity: Severity
    detail: str = ""

    @property
    def blocking(self) -> bool:
        """Indica se esta falha deve interromper a carga."""
        return not self.passed and self.severity is Severity.ERROR


@dataclass(frozen=True, slots=True)
class Check:
    """Uma checagem nomeada, aplicada ao lote inteiro."""

    name: str
    fn: Callable[[Sequence[Record]], tuple[bool, str]]
    severity: Severity = Severity.ERROR

    def run(self, records: Sequence[Record]) -> CheckResult:
        """Executa a checagem e devolve o resultado."""
        passed, detail = self.fn(records)
        return CheckResult(self.name, passed, self.severity, detail)


def not_empty() -> Check:
    """Falha quando o lote chega vazio, que costuma indicar origem quebrada."""

    def check(records: Sequence[Record]) -> tuple[bool, str]:
        return bool(records), f"{len(records)} registros"

    return Check("lote_nao_vazio", check, Severity.ERROR)


def unique_key(fields: Sequence[str]) -> Check:
    """Falha quando a chave de negocio se repete dentro do lote."""

    def check(records: Sequence[Record]) -> tuple[bool, str]:
        seen: set[tuple[Any, ...]] = set()
        duplicates: set[tuple[Any, ...]] = set()

        for record in records:
            key = tuple(record.get(f) for f in fields)
            if key in seen:
                duplicates.add(key)
            seen.add(key)

        if duplicates:
            amostra = sorted(str(d) for d in duplicates)[:3]
            return False, f"{len(duplicates)} chaves repetidas, ex.: {amostra}"
        return True, f"{len(seen)} chaves unicas"

    return Check(f"chave_unica[{','.join(fields)}]", check, Severity.ERROR)


def not_null(field_name: str) -> Check:
    """Falha quando um campo que deveria vir preenchido vem nulo."""

    def check(records: Sequence[Record]) -> tuple[bool, str]:
        nulls = sum(1 for r in records if r.get(field_name) is None)
        return nulls == 0, f"{nulls} nulos em '{field_name}'"

    return Check(f"nao_nulo[{field_name}]", check, Severity.ERROR)


def in_range(field_name: str, minimum: float, maximum: float) -> Check:
    """Falha quando um valor numerico sai da faixa plausivel."""
    if minimum > maximum:
        raise ValueError("minimo maior que maximo")

    def check(records: Sequence[Record]) -> tuple[bool, str]:
        fora = [
            r[field_name]
            for r in records
            if isinstance(r.get(field_name), int | float)
            and not minimum <= float(r[field_name]) <= maximum
        ]
        if fora:
            return False, f"{len(fora)} fora de [{minimum}, {maximum}], ex.: {fora[:3]}"
        return True, f"todos dentro de [{minimum}, {maximum}]"

    return Check(f"faixa[{field_name}]", check, Severity.ERROR)


def row_count_within(expected: int, tolerance: float = 0.5) -> Check:
    """Avisa quando o volume do lote destoa do historico.

    Uma carga que sempre traz mil linhas e hoje trouxe dez provavelmente teve a
    origem truncada, mesmo que as dez linhas sejam validas. E aviso, nao erro,
    porque variacao legitima de volume existe.
    """
    if expected < 0:
        raise ValueError("expected nao pode ser negativo")
    if not 0 < tolerance <= 1:
        raise ValueError("tolerance deve estar em (0, 1]")

    def check(records: Sequence[Record]) -> tuple[bool, str]:
        lower, upper = expected * (1 - tolerance), expected * (1 + tolerance)
        n = len(records)
        ok = lower <= n <= upper
        return ok, f"{n} registros, esperado entre {lower:.0f} e {upper:.0f}"

    return Check("volume_esperado", check, Severity.WARNING)


def run_checks(records: Sequence[Record], checks: Sequence[Check]) -> list[CheckResult]:
    """Roda todas as checagens e devolve os resultados na ordem declarada."""
    return [check.run(records) for check in checks]


def has_blocking_failure(results: Sequence[CheckResult]) -> bool:
    """Indica se alguma checagem de severidade ERROR falhou."""
    return any(r.blocking for r in results)
