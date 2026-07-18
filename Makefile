# github-events-lakehouse — local-first demo targets.
# Everything runs without Docker or cloud credentials: seeded GH-Archive-style
# event files land locally and flow bronze -> raw vault -> business vault -> gold.

.DEFAULT_GOAL := help
SHELL := /bin/bash

UV ?= uv
EVENTS ?= 2000
SEED ?= 42
CORRUPT_PCT ?= 2

.PHONY: help sync events bronze raw-vault vault-verify business-vault gold dq report demo test lint fmt render bundle-validate clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

sync: ## Install/refresh the Python environment (uv)
	$(UV) sync --group dev

events: ## Generate seeded GH-Archive-style NDJSON event files into the landing zone
	$(UV) run python -m ingestion.github_archive --events $(EVENTS) --seed $(SEED) --corrupt-pct $(CORRUPT_PCT)

bronze: ## Bronze: COPY INTO-style batch load with exactly-once file ledger (+ quarantine)
	$(UV) run python -m pipelines.bronze.copy_into

raw-vault: ## Raw Vault: load hubs, links, satellites (insert-only, idempotent)
	$(UV) run python -m pipelines.raw_vault.job

vault-verify: ## Re-run the raw vault load and fail if any table gains rows
	$(UV) run python -m pipelines.raw_vault.job --verify-idempotent

business-vault: ## Business Vault: rebuild PIT, bridge, and derived satellites
	$(UV) run python -m pipelines.business_vault.job

gold: ## Gold: rebuild the three information marts
	$(UV) run python -m pipelines.gold.job

dq: ## Run data-quality suites over every layer and print a report
	$(UV) run python -m quality.expectations.runner

report: ## Render the static observability report (text + HTML)
	$(UV) run python -m observability.metrics.report

demo: ## End-to-end: events -> bronze -> raw vault (+idempotency proof) -> business vault -> gold -> dq -> summary
	EVENTS=$(EVENTS) SEED=$(SEED) CORRUPT_PCT=$(CORRUPT_PCT) $(UV) run python -m orchestration.demo

test: ## Run the Python test suite
	$(UV) run pytest

lint: ## Lint + type-check (ruff, mypy)
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy .

fmt: ## Auto-format Python code
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

render: ## Regenerate governance artifacts from contracts + grants.yaml
	$(UV) run python -m governance.unity_catalog.render

bundle-validate: ## Validate the Databricks Asset Bundle (needs databricks CLI)
	databricks bundle validate -t dev

clean: ## Remove all generated data (Delta tables, reports, warehouse)
	rm -rf data spark-warehouse metastore_db derby.log
