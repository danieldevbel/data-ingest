"""Estado de execucao: marca d'agua e chaves ja carregadas.

Idempotencia e o requisito que separa um script de um pipeline. Rodar a mesma
carga duas vezes precisa produzir o mesmo resultado, seja porque a primeira
falhou no meio, seja porque alguem reexecutou por engano.

Duas defesas combinadas:

1. **Marca d'agua**: so busca na origem o que mudou depois do ultimo sucesso.
2. **Chaves vistas**: descarta registro cuja chave de negocio ja entrou, mesmo
   que a origem devolva de novo, o que acontece sempre que a marca d'agua tem
   granularidade de dia e o registro muda duas vezes no mesmo dia.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dataingest.contract import Record


@dataclass(slots=True)
class PipelineState:
    """Estado persistido entre execucoes."""

    watermark: str | None = None
    seen_keys: set[str] = field(default_factory=set)
    last_run_at: str | None = None

    @staticmethod
    def key_of(record: Record, business_key: tuple[str, ...]) -> str:
        """Serializa a chave de negocio de um registro de forma estavel."""
        return "|".join(str(record.get(f)) for f in business_key)

    def filter_new(self, records: list[Record], business_key: tuple[str, ...]) -> list[Record]:
        """Descarta registros cuja chave ja foi carregada.

        Dentro do proprio lote, mantem a ultima ocorrencia de cada chave, que e
        a leitura mais recente da origem.
        """
        deduped: dict[str, Record] = {}
        for record in records:
            deduped[self.key_of(record, business_key)] = record

        return [r for k, r in deduped.items() if k not in self.seen_keys]

    def commit(
        self,
        records: list[Record],
        business_key: tuple[str, ...],
        watermark_field: str | None,
        run_at: str,
    ) -> None:
        """Registra o sucesso de uma carga, avancando a marca d'agua."""
        for record in records:
            self.seen_keys.add(self.key_of(record, business_key))

        if watermark_field:
            values = [
                _as_watermark(r[watermark_field])
                for r in records
                if r.get(watermark_field) is not None
            ]
            if values:
                highest = max(values)
                if self.watermark is None or highest > self.watermark:
                    self.watermark = highest

        self.last_run_at = run_at

    def to_dict(self) -> dict[str, Any]:
        """Serializa o estado, com as chaves ordenadas para diff estavel."""
        return {
            "watermark": self.watermark,
            "last_run_at": self.last_run_at,
            "seen_keys": sorted(self.seen_keys),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PipelineState:
        """Reconstroi o estado a partir da forma serializada."""
        return cls(
            watermark=data.get("watermark"),
            seen_keys=set(data.get("seen_keys", [])),
            last_run_at=data.get("last_run_at"),
        )

    def save(self, path: Path) -> None:
        """Grava o estado de forma atomica, para nao corromper em falha."""
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        temp.replace(path)

    @classmethod
    def load(cls, path: Path) -> PipelineState:
        """Le o estado, devolvendo um estado vazio se o arquivo nao existir."""
        if not path.exists():
            return cls()
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))


def _as_watermark(value: Any) -> str:
    """Serializa um valor de marca d'agua em ISO, para comparacao textual estavel.

    Comparar `str(datetime)` com o texto ISO que a origem entrega falha, porque
    um usa espaco e o outro usa 'T' entre data e hora. Normalizar os dois lados
    para ISO resolve, e mantem a ordem lexicografica igual a ordem cronologica.
    """
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)
