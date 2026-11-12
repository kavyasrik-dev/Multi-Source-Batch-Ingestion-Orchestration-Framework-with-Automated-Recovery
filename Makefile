.DEFAULT_GOAL := help
PYTHON ?= python
COMPOSE ?= docker compose
COMPOSE_FILE ?= infrastructure/docker/compose.yaml

.PHONY: help install-dev lint format test-unit compose-check compose-up compose-down compose-logs

help:
	@echo "install-dev: install editable project and pinned development tools"
	@echo "lint: check lint and formatting; format: apply formatting"
	@echo "test-unit: run tests/unit (exit 5 means no tests exist yet)"
	@echo "compose-up / compose-down / compose-logs: reserved for future Compose stack"

install-dev:
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .

test-unit:
	$(PYTHON) -m pytest tests/unit

# Validate first so lifecycle commands fail clearly until the stack is implemented.
compose-check:
	$(COMPOSE) -f "$(COMPOSE_FILE)" config --quiet

compose-up: compose-check
	$(COMPOSE) -f "$(COMPOSE_FILE)" up -d

compose-down: compose-check
	$(COMPOSE) -f "$(COMPOSE_FILE)" down

compose-logs: compose-check
	$(COMPOSE) -f "$(COMPOSE_FILE)" logs -f
