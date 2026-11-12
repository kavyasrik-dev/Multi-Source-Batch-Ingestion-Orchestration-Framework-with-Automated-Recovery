# Multi-Source Batch Ingestion Framework

Day 2 establishes development standards and configuration. Connectors, pipelines,
orchestrators, service definitions and tests are not implemented yet. No CI/CD is configured.

## Prerequisites and setup

- CPython **3.12.x** with pip and venv; other Python minor versions are not supported.
- Git and GNU Make. On Windows, use PowerShell for setup and install GNU Make or use
  the Python command equivalents below. WSL is also suitable for a separate Linux setup.
- Docker Engine/Desktop with Compose v2 is needed only when service definitions are added.

Create an environment from the repository root on Windows:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

On Linux/macOS:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
cp .env.example .env
```

Replace every environment placeholder locally before future services use it. The template
contains no working credentials. `.env` and `.env.*` are ignored except `.env.example`.
Never commit secrets or real source data. Environment files are not loaded automatically.

## Dependencies and supported runtime

`pyproject.toml` is the source of package metadata and exact direct dependency pins:
Requests 2.32.5 for future HTTP ingestion, Psycopg binary 3.2.10 for PostgreSQL,
MinIO 7.2.18 for object storage, Ruff 0.14.4 and pytest 8.4.2 for development,
and setuptools 80.9.0 for builds. Transitive dependencies are resolved by pip;
these direct pins are not a fully locked dependency graph.

Airflow and Dagster are deliberately not installed in the shared environment.
Their future service environments must define their own compatible pins and constraints.
When updating a pin, resolve and validate it on Python 3.12 before committing the change.

## Local commands

Run from the repository root with the virtual environment active:

| Make target | Python equivalent / purpose |
| --- | --- |
| `make install-dev` | `python -m pip install -e ".[dev]"` |
| `make lint` | `python -m ruff check .` then `python -m ruff format --check .` |
| `make format` | `python -m ruff format .` |
| `make test-unit` | `python -m pytest tests/unit` |

Ruff uses Python 3.12 syntax, 100-character lines, four-space indentation, double
quotes, LF line endings and sorted imports. Lint checks do not modify files.

Tests use `test_*.py`, `test_*` functions and `Test*` classes. Place isolated tests
in `tests/unit`, service tests in `tests/integration`, failure/retry tests in
`tests/recovery`, and synthetic fixtures in `tests/fixtures`. Registered markers
are `unit`, `integration` and `recovery`; apply them explicitly when useful.
Unit tests must not require network access, databases or object storage. Share test
fixtures through `conftest.py` when needed. Unknown markers/configuration and warnings
fail validation. pytest exit code **5** currently means no tests were collected;
it is not a passing test suite. Do not add placeholder tests merely to hide that state.

`make compose-up`, `make compose-down` and `make compose-logs` are reserved for a
future `infrastructure/docker/compose.yaml`. They validate the file first and are
expected to fail until it exists. Override `COMPOSE_FILE` to select another stack.
`compose-down` does not request volume deletion. No Docker or Compose files are added yet.

## Package layout and naming

| Directory | Responsibility |
| --- | --- |
| `app/ingestion` | Batch ingestion coordination |
| `app/connectors` | Source and destination adapters |
| `app/transformation` | Record transformations |
| `app/validation` | Data and schema validation |
| `app/recovery` | Retry and checkpoint behavior |
| `app/common` | Small shared utilities |
| `orchestration/airflow/dags` | Future Airflow DAGs |
| `orchestration/dagster/{assets,jobs,resources}` | Future Dagster definitions |
| `infrastructure/{docker,postgres,minio}` | Future local service definitions |
| `monitoring/{prometheus,grafana}` | Future observability configuration |
| `tests/{unit,integration,recovery,fixtures}` | Tests and synthetic fixtures |
| `docs/{architecture,design}` | Architecture and design decisions |
| `scripts` | Developer and operational helpers |

The distribution name uses hyphens; Python imports use `app` (for example,
`app.connectors`). The current empty directories are reserved namespace packages;
setuptools discovers only `app` and its children, excluding tests and orchestrators.
Remove a directory's `.gitkeep` when its first real file is added.

Use `snake_case` for Python modules, functions, variables and test names;
`PascalCase` for classes; and `UPPER_SNAKE_CASE` for constants and environment
variables. Use descriptive domain names, type annotations on new public functions,
and concise docstrings for public behavior. Keep connectors independent of
orchestration frameworks and keep credentials out of source code and logs.
