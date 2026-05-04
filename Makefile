.DEFAULT_GOAL := help

.PHONY: help install install-dev lint typecheck test test-unit test-integration build publish clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

install: ## Install package (production deps only)
	pip install -e .

install-dev: ## Install package with dev dependencies
	pip install -e ".[dev]"

lint: ## Run ruff linter
	ruff check nexus_spark_lib/ tests/
	ruff format --check nexus_spark_lib/ tests/

format: ## Auto-format with ruff
	ruff format nexus_spark_lib/ tests/
	ruff check --fix nexus_spark_lib/ tests/

typecheck: ## Run mypy type checker
	mypy nexus_spark_lib/

test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only (no Spark/DB required)
	pytest tests/unit/ -v -m unit

test-integration: ## Run integration tests (requires docker-compose.test.yml running)
	pytest tests/integration/ -v -m integration

test-cov: ## Run tests with coverage report
	pytest tests/ --cov=nexus_spark_lib --cov-report=term-missing --cov-report=html

build: ## Build wheel
	pip install build
	python -m build --wheel

clean: ## Remove build artifacts
	rm -rf dist/ build/ *.egg-info/ htmlcov/ .coverage

seed-thresholds: ## Seed er_thresholds for dev/staging
	python scripts/seed_er_thresholds.py

benchmark: ## Run stage latency benchmarks
	python scripts/benchmark_stages.py
