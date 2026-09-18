# data-ingest

Pipeline de ingestão reprodutível. Contrato de dados que separa o registro ruim em vez de derrubar a carga, carga incremental idempotente, checagens de qualidade com severidade, e um manifesto por execução que responde "de onde veio este número".

*Reproducible data ingestion: an explicit data contract, idempotent incremental loads, severity-aware quality checks, and a per-run manifest for lineage.*

![ci](https://github.com/danieldevbel/data-ingest/actions/workflows/ci.yml/badge.svg)
![python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![license](https://img.shields.io/badge/license-MIT-green)

---

## Problema

O script de integração funciona na primeira vez. Os problemas aparecem depois:

- Rodou duas vezes e duplicou tudo, porque ninguém tinha pensado em reexecução.
- Caiu no meio e ninguém sabe até onde carregou.
- Três linhas vieram com data no formato errado e a carga inteira de dez mil abortou.
- A origem mudou uma coluna, a carga continuou rodando, e o relatório ficou errado por três semanas.
- Alguém pergunta de onde veio um número no painel e não há resposta.

Cada uma dessas falhas tem uma defesa correspondente neste repositório, e um teste que prova que a defesa funciona.

## As cinco defesas

**1. Idempotência: reexecutar não duplica.** Marca d'água (só busca o que mudou) combinada com chaves já carregadas (descarta o que já entrou, mesmo que a origem devolva de novo). As duas juntas, porque só a marca d'água falha quando a granularidade é de dia e o registro muda duas vezes no mesmo dia.

```python
def test_reexecucao_nao_duplica():
    pipeline.run(state)
    segunda = pipeline.run(state)
    assert segunda.written == 0
    assert len(sink.records) == 6
```

**2. Falha parcial não é falha total.** O contrato valida linha a linha e separa as ruins com o motivo, a linha e o campo. As boas seguem:

```
validos    : 2
rejeitados : 3
taxa       : 60.0%

  - linha 3, campo 'inscrito_id': campo obrigatorio ausente
  - linha 4, campo 'carga_horaria': nao convertivel para integer: 'oito'
  - linha 5, campo 'presente': valor booleano nao reconhecido: 'talvez'
```

**3. Mudança silenciosa da origem bloqueia a carga.** Se a taxa de rejeição passa do limite configurado, a carga para antes de escrever qualquer coisa. Rejeição alta não é dado sujo, é contrato quebrado, e deixar passar é como o relatório fica errado por três semanas.

**4. Checagem com severidade.** `ERROR` interrompe (chave duplicada, campo nulo, valor fora de faixa); `WARNING` registra e segue (volume destoando do histórico). Sem essa distinção, ou tudo derruba a carga, ou nada derruba.

**5. Manifesto por execução.** Cada carga grava um JSON com a contagem em cada etapa, as violações, as checagens executadas e o avanço da marca d'água:

```json
{
  "contract": "inscricoes_eventos",
  "counts": { "fetched": 6, "valid": 6, "rejected": 0, "duplicates": 0, "written": 6 },
  "reject_ratio": 0.0,
  "watermark": { "before": null, "after": "2026-03-03T14:10:00" },
  "blocked": false,
  "checks": [
    { "name": "chave_unica[evento_id,inscrito_id]", "passed": true, "severity": "error",
      "detail": "6 chaves unicas" }
  ]
}
```

## O estado só avança se a escrita deu certo

```
origem ──► contrato ──► deduplicação ──► checagens ──► destino ──► estado
           (forma)      (idempotência)   (sentido)                (commit)
```

A ordem não é arbitrária. A validação vem antes da deduplicação porque a chave precisa estar convertida para comparar. As checagens vêm antes da escrita porque uma checagem bloqueante precisa impedir a carga, não reclamar depois. E o commit do estado é a última coisa: uma falha no meio deixa o pipeline pronto para reexecutar sem duplicar, em vez de deixar a marca d'água à frente dos dados.

Um detalhe que só aparece rodando: carga incremental sem novidade é um no-op legítimo, não uma falha. Sem esse tratamento, a checagem "lote não vazio" falharia todo dia em que a origem não teve movimento, e o alerta viraria ruído que ninguém lê.

```
src/dataingest/
  contract.py    FieldSpec, DataContract, validação linha a linha
  quality.py     checagens com severidade e resultado descritivo
  state.py       marca d'água, chaves vistas, gravação atômica
  sources.py     protocolo Source, implementações memória e CSV
  sinks.py       protocolo Sink, implementações memória, JSONL e CSV
  pipeline.py    orquestração e manifesto
  example.py     contrato e dados fictícios de demonstração
  cli.py         linha de comando
```

`Source` e `Sink` são `Protocol`. Trocar CSV por API REST, ou JSONL por Postgres, não toca em nenhum outro módulo. São **73 testes** que rodam em segundos, sem banco e sem rede.

## Como rodar

```bash
git clone https://github.com/danieldevbel/data-ingest.git
cd data-ingest
make install
make check
```

```bash
uv run data-ingest contract              # descreve o contrato
uv run data-ingest validate              # valida um lote com defeitos
uv run data-ingest run                   # primeira carga: escreve 6
uv run data-ingest run                   # segunda: escreve 0, não duplica
uv run data-ingest run --full-refresh    # ignora a marca d'água: ainda não duplica
uv run data-ingest run --dirty --full-refresh   # bloqueia, sai com código 1
```

## Configuração

Por variável de ambiente, prefixo `DI_`. Ver [.env.example](.env.example).

| Variável | Padrão | Efeito |
|---|---|---|
| `DI_STATE_PATH` | `data/state.json` | Onde o estado é persistido |
| `DI_OUTPUT_PATH` | `data/output.jsonl` | Destino da carga |
| `DI_MANIFEST_DIR` | `data/manifests` | Onde cada execução grava seu manifesto |
| `DI_MAX_REJECT_RATIO` | `0.1` | Fração de rejeição acima da qual a carga é bloqueada |

## Decisões e alternativas descartadas

- **Sem pandas.** O núcleo opera sobre `list[dict]`. Pandas resolveria a validação em menos linhas, mas carregaria o lote inteiro em memória e tornaria o erro por linha mais difícil de reportar. Para volumes que não cabem em memória, o caminho é trocar `Source` por um que devolva em lotes, sem mexer no resto.
- **Contrato em código, não em YAML.** YAML é mais fácil de editar por quem não programa, e mais fácil de quebrar em silêncio. Em Python, o contrato é validado na importação: chave de negócio que cita campo inexistente, ou campo de chave marcado como nulável, falham na hora.
- **Estado em arquivo JSON, não em banco.** É o suficiente para pipeline de uma máquina, e gravação atômica (grava em `.tmp` e renomeia) evita corromper em queda. Para várias instâncias concorrentes, é preciso um estado com trava, que este repositório não implementa.
- **Sem orquestrador.** Airflow ou Prefect resolvem agendamento, retry e dependência entre tarefas, e o valor aqui é o que roda dentro de uma tarefa. `IngestionPipeline.run()` é chamado de dentro de um operador do Airflow sem adaptação.
- **`seen_keys` cresce sem limite.** Decisão consciente para manter o exemplo simples. Ver limitações.

## Limitações conhecidas

- **O conjunto de chaves vistas cresce indefinidamente.** Em milhões de registros, o arquivo de estado fica inviável. As saídas são guardar só uma janela recente de chaves, delegar a deduplicação ao destino (`MERGE`/`upsert` por chave), ou usar um filtro de Bloom. A versão correta depende do destino.
- **Carga em um lote só.** Não há processamento em fluxo nem paginação; a origem devolve tudo de uma vez.
- **Sem controle de concorrência.** Duas execuções simultâneas sobre o mesmo arquivo de estado se sobrescrevem.
- **Sem evolução de esquema.** Mudar o contrato não migra o que já foi carregado nem versiona a mudança.
- **`MemorySource` e `JsonlSink` são demonstração.** Uso real exige conector para a origem verdadeira (ERP, API, banco) e destino transacional.
- **Os dados de exemplo são fictícios.** `example.py` modela inscrições em eventos com dados inventados, apenas para exercitar contrato, deduplicação e checagens.

## Licença

MIT. Ver [LICENSE](LICENSE).
