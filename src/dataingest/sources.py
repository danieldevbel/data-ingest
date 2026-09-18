"""Origens de dados.

O pipeline depende do protocolo `Source`, nunca de um sistema especifico.
Trocar CSV por API REST ou por consulta em ERP nao altera nenhuma outra parte.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Protocol

from dataingest.contract import Record


class Source(Protocol):
    """Contrato de uma origem de dados."""

    def fetch(self, since: str | None = None) -> list[Record]:
        """Devolve os registros disponiveis, opcionalmente a partir da marca d'agua."""
        ...


class MemorySource:
    """Origem em memoria, para teste e demonstracao."""

    def __init__(self, records: list[Record], watermark_field: str | None = None) -> None:
        self._records = records
        self._watermark_field = watermark_field
        self.calls: list[str | None] = []

    def fetch(self, since: str | None = None) -> list[Record]:
        """Devolve os registros, filtrando pela marca d'agua quando houver."""
        self.calls.append(since)

        if since is None or self._watermark_field is None:
            return list(self._records)

        return [
            r
            for r in self._records
            if r.get(self._watermark_field) is not None and str(r[self._watermark_field]) > since
        ]


class CsvSource:
    """Origem em arquivo CSV delimitado."""

    def __init__(
        self,
        path: Path,
        watermark_field: str | None = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> None:
        if not path.exists():
            raise FileNotFoundError(f"arquivo de origem nao encontrado: {path}")
        self._path = path
        self._watermark_field = watermark_field
        self._delimiter = delimiter
        self._encoding = encoding

    def fetch(self, since: str | None = None) -> list[Record]:
        """Le o arquivo inteiro e filtra pela marca d'agua quando houver."""
        with self._path.open(encoding=self._encoding, newline="") as handle:
            rows: list[Record] = list(csv.DictReader(handle, delimiter=self._delimiter))

        if since is None or self._watermark_field is None:
            return rows

        return [
            r
            for r in rows
            if r.get(self._watermark_field) and str(r[self._watermark_field]) > since
        ]
