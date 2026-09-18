"""Testes do contrato de dados."""

from __future__ import annotations

from datetime import datetime

import pytest

from dataingest.contract import DataContract, FieldSpec, FieldType

CONTRATO = DataContract(
    name="t",
    fields=(
        FieldSpec("id", FieldType.STRING, nullable=False),
        FieldSpec("qtd", FieldType.INTEGER),
        FieldSpec("valor", FieldType.FLOAT),
        FieldSpec("ativo", FieldType.BOOLEAN),
        FieldSpec("quando", FieldType.TIMESTAMP, nullable=False),
    ),
    business_key=("id",),
    watermark_field="quando",
)


def linha(**over: object) -> dict[str, object]:
    base = {
        "id": "A1",
        "qtd": "3",
        "valor": "9,5",
        "ativo": "sim",
        "quando": "2026-03-01T10:00:00",
    }
    base.update(over)
    return base


def test_converte_os_tipos():
    result = CONTRATO.validate([linha()])
    assert result.valid[0]["qtd"] == 3
    assert result.valid[0]["valor"] == pytest.approx(9.5)
    assert result.valid[0]["ativo"] is True
    assert isinstance(result.valid[0]["quando"], datetime)


def test_aceita_decimal_com_virgula_e_com_ponto():
    assert CONTRATO.validate([linha(valor="7,25")]).valid[0]["valor"] == pytest.approx(7.25)
    assert CONTRATO.validate([linha(valor="7.25")]).valid[0]["valor"] == pytest.approx(7.25)


@pytest.mark.parametrize("texto", ["sim", "s", "1", "true", "yes", "t"])
def test_booleano_verdadeiro(texto):
    assert CONTRATO.validate([linha(ativo=texto)]).valid[0]["ativo"] is True


@pytest.mark.parametrize("texto", ["nao", "n", "0", "false", "no", "f"])
def test_booleano_falso(texto):
    assert CONTRATO.validate([linha(ativo=texto)]).valid[0]["ativo"] is False


def test_booleano_ambiguo_e_rejeitado():
    result = CONTRATO.validate([linha(ativo="talvez")])
    assert not result.valid
    assert "booleano" in result.violations[0].reason


def test_obrigatorio_ausente_e_rejeitado():
    result = CONTRATO.validate([linha(id="")])
    assert not result.valid
    assert "obrigatorio" in result.violations[0].reason


def test_nulavel_vazio_vira_none():
    assert CONTRATO.validate([linha(qtd="")]).valid[0]["qtd"] is None


def test_linha_ruim_nao_derruba_as_boas():
    result = CONTRATO.validate([linha(), linha(id="", qtd="x"), linha(id="A3")])
    assert len(result.valid) == 2
    assert result.rejected_rows == {2}
    assert result.reject_ratio == pytest.approx(1 / 3)


def test_violacao_aponta_linha_e_campo():
    violation = CONTRATO.validate([linha(), linha(qtd="oito")]).violations[0]
    assert violation.row == 2
    assert violation.field == "qtd"
    assert "linha 2" in str(violation)


def test_contrato_sem_campo_e_rejeitado():
    with pytest.raises(ValueError, match="sem campos"):
        DataContract(name="x", fields=(), business_key=("a",))


def test_campo_duplicado_e_rejeitado():
    with pytest.raises(ValueError, match="duplicado"):
        DataContract(
            name="x",
            fields=(FieldSpec("a", FieldType.STRING, nullable=False),) * 2,
            business_key=("a",),
        )


def test_chave_inexistente_e_rejeitada():
    with pytest.raises(ValueError, match="chave de negocio cita campo inexistente"):
        DataContract(
            name="x",
            fields=(FieldSpec("a", FieldType.STRING, nullable=False),),
            business_key=("b",),
        )


def test_chave_nulavel_e_rejeitada():
    with pytest.raises(ValueError, match="nao pode ser nulavel"):
        DataContract(
            name="x",
            fields=(FieldSpec("a", FieldType.STRING, nullable=True),),
            business_key=("a",),
        )


def test_watermark_inexistente_e_rejeitado():
    with pytest.raises(ValueError, match="watermark cita campo inexistente"):
        DataContract(
            name="x",
            fields=(FieldSpec("a", FieldType.STRING, nullable=False),),
            business_key=("a",),
            watermark_field="z",
        )
