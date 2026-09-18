# Contributing

## Ambiente

```bash
make install
```

## Antes de abrir um PR

```bash
make check
```

O CI roda lint (ruff), formatacao, tipos (mypy) e testes (pytest) em Python 3.11 e 3.12.
Um PR so e considerado pronto quando `make check` passa localmente.

## Convencoes

- Commits seguem [Conventional Commits](https://www.conventionalcommits.org): `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
- Toda funcao publica tem type hints e docstring.
- Toda correcao de bug vem acompanhada do teste que falhava antes.
