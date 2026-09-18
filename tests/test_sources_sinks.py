"""Testes das origens e destinos."""

from __future__ import annotations

import json

import pytest

from dataingest.sinks import CsvSink, JsonlSink, MemorySink
from dataingest.sources import CsvSource, MemorySource

CSV = "id,valor,quando\nA,1,2026-01-01\nB,2,2026-02-01\nC,3,2026-03-01\n"


def test_memory_source_sem_marca_dagua_devolve_tudo():
    source = MemorySource([{"a": 1}, {"a": 2}])
    assert len(source.fetch()) == 2


def test_memory_source_filtra_pela_marca_dagua():
    registros = [{"q": "2026-01-01"}, {"q": "2026-05-01"}]
    source = MemorySource(registros, watermark_field="q")
    assert source.fetch(since="2026-03-01") == [{"q": "2026-05-01"}]


def test_csv_source_le_o_arquivo(tmp_path):
    caminho = tmp_path / "e.csv"
    caminho.write_text(CSV, encoding="utf-8")
    assert len(CsvSource(caminho).fetch()) == 3


def test_csv_source_filtra_pela_marca_dagua(tmp_path):
    caminho = tmp_path / "e.csv"
    caminho.write_text(CSV, encoding="utf-8")
    linhas = CsvSource(caminho, watermark_field="quando").fetch(since="2026-01-15")
    assert [r["id"] for r in linhas] == ["B", "C"]


def test_csv_source_exige_arquivo_existente(tmp_path):
    with pytest.raises(FileNotFoundError, match="origem"):
        CsvSource(tmp_path / "nao-existe.csv")


def test_memory_sink_acumula():
    sink = MemorySink()
    assert sink.write([{"a": 1}]) == 1
    sink.write([{"a": 2}])
    assert len(sink.records) == 2


def test_jsonl_sink_grava_uma_linha_por_registro(tmp_path):
    caminho = tmp_path / "sub" / "out.jsonl"
    JsonlSink(caminho).write([{"a": 1}, {"a": 2}])
    linhas = caminho.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(linha)["a"] for linha in linhas] == [1, 2]


def test_jsonl_sink_acrescenta_sem_apagar(tmp_path):
    caminho = tmp_path / "out.jsonl"
    sink = JsonlSink(caminho)
    sink.write([{"a": 1}])
    sink.write([{"a": 2}])
    assert len(caminho.read_text(encoding="utf-8").strip().splitlines()) == 2


def test_sink_vazio_nao_cria_arquivo(tmp_path):
    caminho = tmp_path / "out.jsonl"
    assert JsonlSink(caminho).write([]) == 0
    assert not caminho.exists()


def test_csv_sink_escreve_cabecalho_uma_vez(tmp_path):
    caminho = tmp_path / "out.csv"
    sink = CsvSink(caminho, ("id", "valor"))
    sink.write([{"id": "A", "valor": 1}])
    sink.write([{"id": "B", "valor": 2}])

    linhas = caminho.read_text(encoding="utf-8").strip().splitlines()
    assert linhas[0] == "id,valor"
    assert len(linhas) == 3
