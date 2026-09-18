"""Testes das checagens de qualidade."""

from __future__ import annotations

import pytest

from dataingest.quality import (
    Severity,
    has_blocking_failure,
    in_range,
    not_empty,
    not_null,
    row_count_within,
    run_checks,
    unique_key,
)

REGISTROS = [
    {"id": "A", "nota": 9.0, "unidade": "Centro"},
    {"id": "B", "nota": 7.5, "unidade": "Norte"},
]


def test_lote_vazio_falha():
    assert not_empty().run([]).passed is False


def test_lote_com_registro_passa():
    assert not_empty().run(REGISTROS).passed is True


def test_chave_unica_passa():
    assert unique_key(["id"]).run(REGISTROS).passed is True


def test_chave_duplicada_falha_e_mostra_exemplo():
    result = unique_key(["id"]).run([*REGISTROS, {"id": "A", "nota": 1.0}])
    assert result.passed is False
    assert "repetidas" in result.detail


def test_nao_nulo_detecta_nulo():
    result = not_null("unidade").run([*REGISTROS, {"id": "C", "unidade": None}])
    assert result.passed is False
    assert "1 nulos" in result.detail


def test_faixa_aceita_valores_dentro():
    assert in_range("nota", 0, 10).run(REGISTROS).passed is True


def test_faixa_rejeita_valor_fora():
    result = in_range("nota", 0, 10).run([*REGISTROS, {"id": "C", "nota": 42.0}])
    assert result.passed is False
    assert "42" in result.detail


def test_faixa_ignora_nulo():
    assert in_range("nota", 0, 10).run([{"id": "C", "nota": None}]).passed is True


def test_faixa_invalida_e_rejeitada():
    with pytest.raises(ValueError, match="minimo maior"):
        in_range("nota", 10, 0)


def test_volume_dentro_da_tolerancia_passa():
    assert row_count_within(2, tolerance=0.5).run(REGISTROS).passed is True


def test_volume_muito_abaixo_avisa_sem_bloquear():
    result = row_count_within(100, tolerance=0.2).run(REGISTROS)
    assert result.passed is False
    assert result.severity is Severity.WARNING
    assert result.blocking is False


def test_tolerancia_invalida_e_rejeitada():
    with pytest.raises(ValueError, match="tolerance"):
        row_count_within(10, tolerance=0)


def test_erro_bloqueia_e_aviso_nao():
    results = run_checks([], [not_empty(), row_count_within(10)])
    assert has_blocking_failure(results) is True
    assert [r.blocking for r in results] == [True, False]


def test_sem_falha_nao_bloqueia():
    assert has_blocking_failure(run_checks(REGISTROS, [not_empty()])) is False
