"""Contrato e dados de exemplo.

Modela um caso comum de integracao administrativa: inscricoes em eventos
vindas de um sistema externo, com chave de negocio composta e marca d'agua de
atualizacao. Os dados sao ficticios.
"""

from __future__ import annotations

from dataingest.contract import DataContract, FieldSpec, FieldType, Record
from dataingest.quality import Check, in_range, not_empty, not_null, row_count_within, unique_key

INSCRICOES = DataContract(
    name="inscricoes_eventos",
    fields=(
        FieldSpec("evento_id", FieldType.STRING, nullable=False, description="Codigo do evento."),
        FieldSpec(
            "inscrito_id", FieldType.STRING, nullable=False, description="Codigo do inscrito."
        ),
        FieldSpec("unidade", FieldType.STRING, nullable=False),
        FieldSpec("carga_horaria", FieldType.INTEGER, description="Horas do evento."),
        FieldSpec("presente", FieldType.BOOLEAN),
        FieldSpec("nota_avaliacao", FieldType.FLOAT, description="Nota de 0 a 10."),
        FieldSpec("atualizado_em", FieldType.TIMESTAMP, nullable=False),
    ),
    business_key=("evento_id", "inscrito_id"),
    watermark_field="atualizado_em",
)


def default_checks(expected_rows: int = 6) -> list[Check]:
    """Conjunto de checagens do contrato de exemplo."""
    return [
        not_empty(),
        unique_key(INSCRICOES.business_key),
        not_null("unidade"),
        in_range("nota_avaliacao", 0.0, 10.0),
        in_range("carga_horaria", 1, 500),
        row_count_within(expected_rows, tolerance=0.5),
    ]


def sample_records() -> list[Record]:
    """Lote de exemplo, com os tipos ainda em texto como viriam de um CSV."""
    return [
        {
            "evento_id": "EVT-001",
            "inscrito_id": "P-1001",
            "unidade": "Unidade Centro",
            "carga_horaria": "8",
            "presente": "sim",
            "nota_avaliacao": "9,5",
            "atualizado_em": "2026-03-01T10:00:00",
        },
        {
            "evento_id": "EVT-001",
            "inscrito_id": "P-1002",
            "unidade": "Unidade Centro",
            "carga_horaria": "8",
            "presente": "nao",
            "nota_avaliacao": "",
            "atualizado_em": "2026-03-01T10:05:00",
        },
        {
            "evento_id": "EVT-002",
            "inscrito_id": "P-1003",
            "unidade": "Unidade Norte",
            "carga_horaria": "16",
            "presente": "true",
            "nota_avaliacao": "8.0",
            "atualizado_em": "2026-03-02T09:00:00",
        },
        {
            "evento_id": "EVT-002",
            "inscrito_id": "P-1004",
            "unidade": "Unidade Norte",
            "carga_horaria": "16",
            "presente": "1",
            "nota_avaliacao": "10",
            "atualizado_em": "2026-03-02T09:30:00",
        },
        {
            "evento_id": "EVT-003",
            "inscrito_id": "P-1005",
            "unidade": "Unidade Sul",
            "carga_horaria": "4",
            "presente": "s",
            "nota_avaliacao": "7,5",
            "atualizado_em": "2026-03-03T14:00:00",
        },
        {
            "evento_id": "EVT-003",
            "inscrito_id": "P-1006",
            "unidade": "Unidade Sul",
            "carga_horaria": "4",
            "presente": "n",
            "nota_avaliacao": "6",
            "atualizado_em": "2026-03-03T14:10:00",
        },
    ]


def dirty_records() -> list[Record]:
    """Lote com os defeitos que aparecem em integracao real."""
    return [
        *sample_records()[:2],
        {
            "evento_id": "EVT-004",
            "inscrito_id": "",
            "unidade": "Unidade Leste",
            "carga_horaria": "8",
            "presente": "sim",
            "nota_avaliacao": "9",
            "atualizado_em": "2026-03-04T08:00:00",
        },
        {
            "evento_id": "EVT-004",
            "inscrito_id": "P-1008",
            "unidade": "Unidade Leste",
            "carga_horaria": "oito",
            "presente": "sim",
            "nota_avaliacao": "9",
            "atualizado_em": "2026-03-04T08:05:00",
        },
        {
            "evento_id": "EVT-005",
            "inscrito_id": "P-1009",
            "unidade": "Unidade Oeste",
            "carga_horaria": "8",
            "presente": "talvez",
            "nota_avaliacao": "9",
            "atualizado_em": "2026-03-05T08:00:00",
        },
    ]
