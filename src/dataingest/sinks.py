"""Destinos de carga."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Protocol

from dataingest.contract import Record


class Sink(Protocol):
    """Contrato de um destino de carga."""

    def write(self, records: list[Record]) -> int:
        """Grava os registros e devolve quantos foram efetivamente gravados."""
        ...


class MemorySink:
    """Destino em memoria, para teste."""

    def __init__(self) -> None:
        self.records: list[Record] = []

    def write(self, records: list[Record]) -> int:
        """Acumula os registros recebidos."""
        self.records.extend(records)
        return len(records)


class JsonlSink:
    """Destino em arquivo JSON Lines, em modo append."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def write(self, records: list[Record]) -> int:
        """Acrescenta uma linha JSON por registro."""
        if not records:
            return 0

        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        return len(records)


class CsvSink:
    """Destino em arquivo CSV, escrevendo o cabecalho apenas na criacao."""

    def __init__(self, path: Path, fieldnames: tuple[str, ...]) -> None:
        self._path = path
        self._fieldnames = fieldnames

    def write(self, records: list[Record]) -> int:
        """Acrescenta as linhas ao arquivo, criando-o se necessario."""
        if not records:
            return 0

        self._path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self._path.exists()

        with self._path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self._fieldnames))
            if new_file:
                writer.writeheader()
            for record in records:
                writer.writerow({k: record.get(k) for k in self._fieldnames})

        return len(records)
