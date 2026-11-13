# Local infrastructure (Day 3)

## Requirements

Install Docker Desktop with Linux containers (or Docker Engine on Linux) and
Docker Compose v2. Use at least 4 GB of available RAM; the initial MinIO source
build also needs Go dependency downloads and additional build memory/disk space.
Python 3.12 is required for the host verification script. No Airflow is included.

The four long-running services have versioned image tags, readiness checks,
`unless-stopped` restart policies and independent named volumes. All host ports
bind to loopback only. This is a single-node, plain-HTTP development stack.

MinIO is built from upstream `RELEASE.2025-09-07T16-13-09Z`, with Go 1.24.7 and
Alpine 3.22.1, because the upstream prebuilt public images have been withdrawn.
Its local image has the same release tag; build requires access to GitHub and
the Go module proxy. The initializer uses Python 3.12.11 and MinIO SDK 7.2.18.
These version pins are a development baseline, not a production security review.

## Local environment

The root `.env` is untracked and ignored. It contains development-only credentials
for this machine. `.env.example` remains a placeholder-only template. Do not
publish `.env` or use its credentials on shared or production systems.

If recreating `.env`, supply POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
MINIO_ACCESS_KEY, MINIO_SECRET_KEY, GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD.
Choose a MinIO user of at least 3 characters and password of at least 8 characters.
Optional port overrides: POSTGRES_PORT, MINIO_API_PORT, MINIO_CONSOLE_PORT,
PROMETHEUS_PORT and GRAFANA_PORT. Shell variables override the env file.

Run all commands from the repository root. An explicit `--env-file .env` is
required because the Compose file lives below the root. Do not print resolved
`docker compose config` output into logs; it contains credentials. Use `--quiet`.

```sh
docker compose --env-file .env -f infrastructure/docker/compose.yaml config --quiet
docker compose --env-file .env -f infrastructure/docker/compose.yaml up -d --build --wait --wait-timeout 240
docker compose --env-file .env -f infrastructure/docker/compose.yaml run --rm minio-init
python scripts/verify_infrastructure.py
```

Equivalent Make targets are `compose-check`, `compose-up`, `compose-verify`,
`compose-logs` and `compose-down`. The initializer is a one-shot job; success is
exit code 0, not an ongoing healthy container. It runs only after MinIO is healthy.
It ensures `raw`, `staging`, `curated` and `rejected` exist, preserves contents,
handles concurrent creation by the same owner and fails on other SDK errors.

## Endpoints and persistence

| Service | Default local endpoint | Named volume data path |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `/var/lib/postgresql/data` |
| MinIO S3 | `http://localhost:9000` | `/data` |
| MinIO console | `http://localhost:9001` | same MinIO volume |
| Prometheus | `http://localhost:9090` | `/prometheus` |
| Grafana | `http://localhost:3000` | `/var/lib/grafana` |

Compose prefixes volume names with `batch-ingestion-local`. Grafana provisions
Prometheus as its default data source. Prometheus initially scrapes itself only;
no PostgreSQL, MinIO or application exporter is implied.

The verification script starts the stack, checks repeated bucket initialization,
requires named volumes, writes uniquely named probe files into all four volume
paths, restarts services, waits for health and compares the same volumes and data.
It removes its probe files and leaves the stack running. Run only on this local
development stack: the restart briefly interrupts connections. This verifies
volume persistence, not backup/restore or application-level ingestion correctness.

`docker compose ... down` preserves named volumes. Never use `down -v` unless
you intend to delete all local data. Keep the project name stable across runs.
PostgreSQL and Grafana seed credentials on their first startup; changing `.env`
does not rotate credentials already stored in their volumes.

## Validation status

Docker is not installed on the authoring machine. Container builds, live health
and restart persistence therefore remain unverified; run the commands above on a
Docker-enabled host before accepting this infrastructure milestone. Unit tests
cover initializer idempotency and error handling without requiring services.
