"""Testes do estado e da idempotencia."""

from __future__ import annotations

from dataingest.state import PipelineState

CHAVE = ("evento_id", "inscrito_id")


def registro(evento: str, inscrito: str, quando: str) -> dict[str, object]:
    return {"evento_id": evento, "inscrito_id": inscrito, "atualizado_em": quando}


def test_chave_serializa_de_forma_estavel():
    r = registro("E1", "P1", "2026-01-01T00:00:00")
    assert PipelineState.key_of(r, CHAVE) == "E1|P1"


def test_estado_novo_deixa_tudo_passar():
    novos = PipelineState().filter_new([registro("E1", "P1", "x")], CHAVE)
    assert len(novos) == 1


def test_chave_ja_vista_e_descartada():
    state = PipelineState()
    state.commit([registro("E1", "P1", "2026-01-01T00:00:00")], CHAVE, "atualizado_em", "r1")
    assert state.filter_new([registro("E1", "P1", "2026-01-02T00:00:00")], CHAVE) == []


def test_duplicata_dentro_do_lote_mantem_a_ultima():
    lote = [
        registro("E1", "P1", "2026-01-01T00:00:00"),
        registro("E1", "P1", "2026-01-05T00:00:00"),
    ]
    novos = PipelineState().filter_new(lote, CHAVE)
    assert len(novos) == 1
    assert novos[0]["atualizado_em"] == "2026-01-05T00:00:00"


def test_marca_dagua_avanca_para_o_maior_valor():
    state = PipelineState()
    state.commit(
        [registro("E1", "P1", "2026-01-01T00:00:00"), registro("E2", "P2", "2026-03-01T00:00:00")],
        CHAVE,
        "atualizado_em",
        "r1",
    )
    assert state.watermark == "2026-03-01T00:00:00"


def test_marca_dagua_nao_retrocede():
    state = PipelineState(watermark="2026-06-01T00:00:00")
    state.commit([registro("E1", "P1", "2026-01-01T00:00:00")], CHAVE, "atualizado_em", "r1")
    assert state.watermark == "2026-06-01T00:00:00"


def test_marca_dagua_de_datetime_sai_em_iso():
    from datetime import datetime

    state = PipelineState()
    state.commit(
        [{"evento_id": "E", "inscrito_id": "P", "atualizado_em": datetime(2026, 3, 1, 14, 10)}],
        CHAVE,
        "atualizado_em",
        "r1",
    )
    assert state.watermark == "2026-03-01T14:10:00"


def test_ida_e_volta_pelo_disco(tmp_path):
    state = PipelineState()
    state.commit([registro("E1", "P1", "2026-01-01T00:00:00")], CHAVE, "atualizado_em", "r1")

    caminho = tmp_path / "sub" / "state.json"
    state.save(caminho)
    recuperado = PipelineState.load(caminho)

    assert recuperado.watermark == state.watermark
    assert recuperado.seen_keys == state.seen_keys
    assert recuperado.last_run_at == "r1"


def test_arquivo_inexistente_devolve_estado_vazio(tmp_path):
    state = PipelineState.load(tmp_path / "nao-existe.json")
    assert state.watermark is None
    assert state.seen_keys == set()


def test_gravacao_nao_deixa_arquivo_temporario(tmp_path):
    caminho = tmp_path / "state.json"
    PipelineState().save(caminho)
    assert caminho.exists()
    assert not list(tmp_path.glob("*.tmp"))
