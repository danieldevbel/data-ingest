"""Testes de integracao do pipeline de ingestao."""

from __future__ import annotations

import pytest

from dataingest.example import INSCRICOES, default_checks, dirty_records, sample_records
from dataingest.pipeline import IngestionBlocked, IngestionPipeline
from dataingest.quality import not_empty, unique_key
from dataingest.sinks import MemorySink
from dataingest.sources import MemorySource
from dataingest.state import PipelineState


def montar(records: list[dict[str, object]], **kwargs: object) -> tuple:
    sink = MemorySink()
    pipeline = IngestionPipeline(
        contract=INSCRICOES,
        source=MemorySource(records, watermark_field=INSCRICOES.watermark_field),
        sink=sink,
        checks=kwargs.pop("checks", default_checks(expected_rows=len(records))),
        **kwargs,
    )
    return pipeline, sink


def test_carga_limpa_escreve_tudo():
    pipeline, sink = montar(sample_records())
    manifest = pipeline.run(PipelineState())

    assert manifest.fetched == 6
    assert manifest.valid == 6
    assert manifest.written == 6
    assert len(sink.records) == 6
    assert manifest.blocked is False


def test_reexecucao_nao_duplica():
    """A propriedade que separa um pipeline de um script."""
    pipeline, sink = montar(sample_records())
    state = PipelineState()

    pipeline.run(state)
    segunda = pipeline.run(state)

    assert segunda.written == 0
    assert len(sink.records) == 6


def test_reexecucao_com_full_refresh_tambem_nao_duplica():
    pipeline, sink = montar(sample_records())
    state = PipelineState()

    pipeline.run(state)
    pipeline.run(state, full_refresh=True)

    assert len(sink.records) == 6


def test_marca_dagua_avanca_apos_sucesso():
    pipeline, _ = montar(sample_records())
    state = PipelineState()

    manifest = pipeline.run(state)

    assert manifest.watermark_before is None
    assert manifest.watermark_after == "2026-03-03T14:10:00"


def test_apenas_o_que_mudou_e_buscado_na_segunda_vez():
    source = MemorySource(sample_records(), watermark_field=INSCRICOES.watermark_field)
    pipeline = IngestionPipeline(
        contract=INSCRICOES,
        source=source,
        sink=MemorySink(),
        checks=default_checks(),
    )
    state = PipelineState()

    pipeline.run(state)
    pipeline.run(state)

    assert source.calls == [None, "2026-03-03T14:10:00"]


def test_taxa_de_rejeicao_alta_bloqueia_a_carga():
    pipeline, sink = montar(dirty_records(), max_reject_ratio=0.1)

    with pytest.raises(IngestionBlocked, match="taxa_de_rejeicao"):
        pipeline.run(PipelineState())

    assert sink.records == []


def test_carga_bloqueada_nao_avanca_a_marca_dagua():
    pipeline, _ = montar(dirty_records(), max_reject_ratio=0.1)
    state = PipelineState()

    with pytest.raises(IngestionBlocked):
        pipeline.run(state)

    assert state.watermark is None
    assert state.seen_keys == set()


def test_tolerar_rejeicao_permite_carga_parcial():
    pipeline, sink = montar(dirty_records(), max_reject_ratio=1.0)
    manifest = pipeline.run(PipelineState())

    assert manifest.rejected == 3
    assert manifest.written == 2
    assert len(sink.records) == 2


def test_checagem_bloqueante_impede_a_escrita():
    registros = [*sample_records(), sample_records()[0] | {"atualizado_em": "2026-03-09T00:00:00"}]
    pipeline, sink = montar(registros, checks=[not_empty(), unique_key(("unidade",))])

    with pytest.raises(IngestionBlocked, match="chave_unica"):
        pipeline.run(PipelineState())

    assert sink.records == []


def test_origem_silenciosa_em_full_refresh_bloqueia():
    pipeline, _ = montar([], checks=[not_empty()])

    with pytest.raises(IngestionBlocked, match="lote_nao_vazio"):
        pipeline.run(PipelineState(), full_refresh=True)


def test_incremental_sem_novidade_e_sucesso_vazio():
    pipeline, _ = montar(sample_records())
    state = PipelineState()
    pipeline.run(state)

    manifest = pipeline.run(state)

    assert manifest.blocked is False
    assert manifest.written == 0
    assert manifest.checks == []


def test_manifesto_registra_o_caminho_dos_dados():
    pipeline, _ = montar(dirty_records(), max_reject_ratio=1.0)
    manifest = pipeline.run(PipelineState())
    data = manifest.as_dict()

    assert data["counts"] == {
        "fetched": 5,
        "valid": 2,
        "rejected": 3,
        "duplicates": 0,
        "written": 2,
    }
    assert data["reject_ratio"] == pytest.approx(0.6)
    assert len(data["violations"]) == 3
    assert data["watermark"]["before"] is None


def test_manifesto_lista_as_checagens_executadas():
    pipeline, _ = montar(sample_records())
    data = pipeline.run(PipelineState()).as_dict()

    nomes = [c["name"] for c in data["checks"]]
    assert "lote_nao_vazio" in nomes
    assert all("passed" in c and "severity" in c for c in data["checks"])


def test_aviso_nao_bloqueia_a_carga():
    """volume_esperado e WARNING: destoar do historico avisa, nao impede."""
    pipeline, sink = montar(sample_records(), checks=default_checks(expected_rows=1000))
    manifest = pipeline.run(PipelineState())

    assert manifest.written == 6
    assert any(not c.passed and not c.blocking for c in manifest.checks)
    assert len(sink.records) == 6


def test_max_reject_ratio_invalido_e_rejeitado():
    with pytest.raises(ValueError, match="max_reject_ratio"):
        IngestionPipeline(
            contract=INSCRICOES,
            source=MemorySource([]),
            sink=MemorySink(),
            max_reject_ratio=1.5,
        )
