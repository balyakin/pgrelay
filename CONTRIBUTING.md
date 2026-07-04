# Contributing

## Local Setup

```bash
poetry install --with dev
docker compose up -d postgres
poetry run pgrelay migrate upgrade
```

## Checks

Run the same checks as CI before sending changes:

```bash
poetry run ruff format --check .
poetry run ruff check .
poetry run mypy src/pgrelay tests
poetry run pytest -q --cov=src/pgrelay --cov-fail-under=85
```

## Expectations

- Keep changes scoped to one issue.
- Add a focused regression test for behavior changes.
- Keep public API changes explicit in `README.md` and `CHANGELOG.md`.
- Do not commit local secrets, database dumps, or generated cache directories.
