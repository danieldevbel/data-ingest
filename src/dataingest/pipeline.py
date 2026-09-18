"""Orquestracao da ingestao.

    origem ──► contrato ──► deduplicacao ──► checagens ──► destino ──► estado
               (forma)      (idempotencia)   (sentido)                (commit)

A ordem nao e arbitraria. A validacao vem antes da deduplicacao porque a chave
de negocio precisa estar convertida para comparar. As checagens vem antes da
escrita porque uma checagem bloqueante precisa impedir a carga, nao apenas
reclamar depois. E o estado so avança se a escrita deu certo, de modo que uma
falha no meio deixa o pipeline pronto para reexecutar sem duplicar.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from dataingest.contract import DataContract, Violation
from dataingest.quality import Check, CheckResult, Severity, has_blocking_failure, run_checks
from dataingest.sinks import Sink
from dataingest.sources import Source
from dataingest.state import PipelineState


class IngestionBlocked(RuntimeError):  # noqa: N818 - o nome descreve o efeito, nao o tipo
    """A carga foi interrompida por falha de checagem bloqueante."""

    def __init__(self, failures: list[CheckResult]) -> None:
        nomes = ", ".join(f.name for f in failures)
        super().__init__(f"carga bloqueada por checagem: {nomes}")
        self.failures = failures


@dataclass(slots=True)
class RunManifest:
    """Registro auditavel de uma execucao.

    E o artefato que responde 'de onde veio este numero': quantos registros a
    origem devolveu, quantos o contrato rejeitou e por que, quantos ja existiam,
    quais checagens rodaram e ate onde a marca d'agua avancou.
    """

    contract: str
    started_at: str
    finished_at: str | None = None
    fetched: int = 0
    valid: int = 0
    rejected: int = 0
    duplicates: int = 0
    written: int = 0
    watermark_before: str | None = None
    watermark_after: str | None = None
    checks: list[CheckResult] = field(default_factory=list)
    violations: list[Violation] = field(default_factory=list)
    blocked: bool = False

    @property
    def reject_ratio(self) -> float:
        """Fracao de registros rejeitados sobre o total buscado."""
        return self.rejected / self.fetched if self.fetched else 0.0

    def as_dict(self) -> dict[str, Any]:
        """Serializa o manifesto para gravacao ao lado dos dados."""
        return {
            "contract": self.contract,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "counts": {
                "fetched": self.fetched,
                "valid": self.valid,
                "rejected": self.rejected,
                "duplicates": self.duplicates,
                "written": self.written,
            },
            "reject_ratio": round(self.reject_ratio, 4),
            "watermark": {"before": self.watermark_before, "after": self.watermark_after},
            "blocked": self.blocked,
            "checks": [
                {
                    "name": c.name,
                    "passed": c.passed,
                    "severity": c.severity.value,
                    "detail": c.detail,
                }
                for c in self.checks
            ],
            "violations": [str(v) for v in self.violations[:50]],
        }


@dataclass(slots=True)
class IngestionPipeline:
    """Pipeline completo de uma ingestao."""

    contract: DataContract
    source: Source
    sink: Sink
    checks: Sequence[Check] = field(default_factory=tuple)
    max_reject_ratio: float = 0.1
    """Acima desta fracao de rejeicao, a carga e bloqueada: a origem mudou."""

    def __post_init__(self) -> None:
        if not 0.0 <= self.max_reject_ratio <= 1.0:
            raise ValueError("max_reject_ratio deve estar em [0, 1]")

    def run(self, state: PipelineState, full_refresh: bool = False) -> RunManifest:
        """Executa uma carga e devolve o manifesto.

        Raises:
            IngestionBlocked: se alguma checagem bloqueante falhar, ou se a
                taxa de rejeicao ultrapassar o limite. Nesse caso nada e
                escrito e o estado nao avanca.
        """
        started = datetime.now(UTC)
        manifest = RunManifest(
            contract=self.contract.name,
            started_at=started.isoformat(),
            watermark_before=state.watermark,
        )

        since = None if full_refresh else state.watermark
        raw = self.source.fetch(since=since)
        manifest.fetched = len(raw)

        validation = self.contract.validate(raw)
        manifest.valid = len(validation.valid)
        manifest.rejected = len(validation.rejected_rows)
        manifest.violations = validation.violations

        new_records = state.filter_new(validation.valid, self.contract.business_key)
        manifest.duplicates = len(validation.valid) - len(new_records)

        # A taxa de rejeicao e avaliada antes de qualquer outra coisa: ela
        # indica que a origem mudou de forma, e isso vale mesmo quando o lote
        # acaba sem nada novo para escrever.
        if manifest.fetched and manifest.reject_ratio > self.max_reject_ratio:
            manifest.checks.append(
                CheckResult(
                    name="taxa_de_rejeicao",
                    passed=False,
                    severity=Severity.ERROR,
                    detail=(
                        f"{manifest.reject_ratio:.1%} rejeitados, "
                        f"limite {self.max_reject_ratio:.1%}"
                    ),
                )
            )
            manifest.blocked = True
            manifest.finished_at = datetime.now(UTC).isoformat()
            raise IngestionBlocked(blocking_results(manifest))

        # Nada novo para escrever e um no-op legitimo, nao uma falha: ou a
        # origem nao teve movimento desde a ultima marca d'agua, ou devolveu
        # registros que ja estao carregados. Rodar as checagens de conteudo
        # sobre um lote vazio faria 'lote_nao_vazio' falhar todo dia parado.
        #
        # A excecao e a origem que devolveu zero linhas quando foi consultada
        # sem filtro: ai o silencio e sintoma, e as checagens devem rodar.
        source_silent = manifest.fetched == 0 and (full_refresh or state.watermark is None)

        if not new_records and not source_silent:
            manifest.watermark_after = state.watermark
            manifest.finished_at = datetime.now(UTC).isoformat()
            return manifest

        manifest.checks.extend(run_checks(new_records, self.checks))

        if has_blocking_failure(manifest.checks):
            manifest.blocked = True
            manifest.finished_at = datetime.now(UTC).isoformat()
            raise IngestionBlocked(blocking_results(manifest))

        manifest.written = self.sink.write(new_records)

        state.commit(
            new_records,
            self.contract.business_key,
            self.contract.watermark_field,
            run_at=started.isoformat(),
        )

        manifest.watermark_after = state.watermark
        manifest.finished_at = datetime.now(UTC).isoformat()
        return manifest


def blocking_results(manifest: RunManifest) -> list[CheckResult]:
    """Checagens bloqueantes que falharam em uma execucao."""
    return [c for c in manifest.checks if c.blocking]


__all__ = [
    "IngestionBlocked",
    "IngestionPipeline",
    "RunManifest",
    "blocking_results",
    "has_blocking_failure",
]
