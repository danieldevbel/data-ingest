"""Configuracao do pipeline."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuracao tipada, carregada do ambiente."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="DI_",
        extra="ignore",
    )

    environment: Literal["dev", "staging", "prod"] = Field(default="dev")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")

    state_path: Path = Field(
        default=Path("data/state.json"),
        description="Arquivo de estado entre execucoes.",
    )
    output_path: Path = Field(default=Path("data/output.jsonl"))
    manifest_dir: Path = Field(
        default=Path("data/manifests"),
        description="Pasta onde cada execucao grava seu manifesto.",
    )
    max_reject_ratio: float = Field(
        default=0.1,
        ge=0,
        le=1,
        description="Fracao de rejeicao acima da qual a carga e bloqueada.",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Devolve a configuracao carregada uma unica vez por processo."""
    return Settings()
