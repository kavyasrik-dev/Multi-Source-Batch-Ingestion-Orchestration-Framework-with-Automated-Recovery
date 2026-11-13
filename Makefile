.DEFAULT_GOAL := help
PYTHON ?= python
COMPOSE ?= docker compose
COMPOSE_FILE ?= infrastructure/docker/compose.yaml
ENV_FILE ?= .env
STACK = $(COMPOSE) --env-file "$(ENV_FILE)" -f "$(COMPOSE_FILE)"

.PHONY: help install-dev lint format test-unit compose-check compose-up compose-down compose-logs compose-verify

help:
	@echo "install-dev: install editable project and pinned development tools"
	@echo "lint: check lint and formatting; format: apply formatting"
	@echo "test-unit: run tests/unit (exit 5 means no tests exist yet)"
	@echo "compose-up / compose-down / compose-logs: local infrastructure lifecycle"
	@echo "compose-verify: verify health, bucket initialization and restart persistence"

install-dev:
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .

test-unit:
	$(PYTHON) -m pytest tests/unit

# Explicit env-file resolves credentials from the repository root.
compose-check:
	$(STACK) config --quiet

compose-up: compose-check
	$(STACK) up -d --build --wait --wait-timeout 240
	$(STACK) run --rm minio-init

compose-down: compose-check
	$(STACK) down

compose-logs: compose-check
	$(STACK) logs -f

compose-verify:
	$(PYTHON) scripts/verify_infrastructure.py --env-file "$(ENV_FILE)" --compose-file "$(COMPOSE_FILE)"
