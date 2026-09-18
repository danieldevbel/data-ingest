"""Interface de linha de comando."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from dataingest import __version__
from dataingest.example import INSCRICOES, default_checks, dirty_records, sample_records
from dataingest.pipeline import IngestionBlocked, IngestionPipeline
from dataingest.settings import get_settings
from dataingest.sinks import JsonlSink
from dataingest.sources import MemorySource
from dataingest.state import PipelineState

app = typer.Typer(
    add_completion=False,
    help="Pipeline de ingestao reprodutivel.",
    no_args_is_help=True,
)


@app.command()
def version() -> None:
    """Mostra a versao instalada."""
    typer.echo(__version__)


@app.command()
def contract() -> None:
    """Descreve o contrato de dados de exemplo."""
    typer.echo(f"contrato: {INSCRICOES.name}")
    typer.echo(f"chave de negocio: {', '.join(INSCRICOES.business_key)}")
    typer.echo(f"marca d'agua: {INSCRICOES.watermark_field}")
    typer.echo("\ncampos:")
    for spec in INSCRICOES.fields:
        obrigatorio = "" if spec.nullable else "  (obrigatorio)"
        typer.echo(f"  {spec.name:<16} {spec.type}{obrigatorio}")


@app.command()
def run(
    state_path: Path = typer.Option(Path("data/state.json"), help="Arquivo de estado."),
    output: Path = typer.Option(Path("data/output.jsonl"), help="Arquivo de saida."),
    dirty: bool = typer.Option(False, help="Usa o lote com defeitos."),
    full_refresh: bool = typer.Option(False, help="Ignora a marca d'agua."),
) -> None:
    """Executa uma carga e grava o manifesto da execucao."""
    settings = get_settings()
    records = dirty_records() if dirty else sample_records()

    pipeline = IngestionPipeline(
        contract=INSCRICOES,
        source=MemorySource(records, watermark_field=INSCRICOES.watermark_field),
        sink=JsonlSink(output),
        checks=default_checks(expected_rows=len(records)),
        max_reject_ratio=settings.max_reject_ratio,
    )

    state = PipelineState.load(state_path)

    try:
        manifest = pipeline.run(state, full_refresh=full_refresh)
    except IngestionBlocked as exc:
        typer.echo("carga bloqueada:", err=True)
        for failure in exc.failures:
            typer.echo(f"  - {failure.name}: {failure.detail}", err=True)
        raise typer.Exit(code=1) from exc

    state.save(state_path)

    settings.manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = settings.manifest_dir / f"{manifest.started_at.replace(':', '-')}.json"
    manifest_path.write_text(
        json.dumps(manifest.as_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    typer.echo(json.dumps(manifest.as_dict()["counts"], indent=2))
    typer.echo(f"\nmarca d'agua: {manifest.watermark_before} -> {manifest.watermark_after}")
    typer.echo(f"manifesto: {manifest_path}")


@app.command()
def validate(
    dirty: bool = typer.Option(True, help="Usa o lote com defeitos."),
) -> None:
    """Valida um lote contra o contrato e lista as violacoes."""
    records = dirty_records() if dirty else sample_records()
    result = INSCRICOES.validate(records)

    typer.echo(f"validos    : {len(result.valid)}")
    typer.echo(f"rejeitados : {len(result.rejected_rows)}")
    typer.echo(f"taxa       : {result.reject_ratio:.1%}\n")

    for violation in result.violations:
        typer.echo(f"  - {violation}")


if __name__ == "__main__":
    app()
