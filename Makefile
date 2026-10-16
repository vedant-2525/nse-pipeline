.PHONY: help up down test lint format backfill dashboard clean

help:
	@echo "Available commands:"
	@echo "  make lint        - Run ruff linter and formatting check"
	@echo "  make format      - Auto-format code using ruff"
	@echo "  make test        - Run test suite with pytest"
	@echo "  make up          - Start all docker containers (Postgres, Airflow, Dashboard)"
	@echo "  make down        - Stop all docker containers"
	@echo "  make dashboard   - Launch Streamlit dashboard locally"
	@echo "  make backfill    - Run backfill pipeline for configured date range"
	@echo "  make clean       - Remove caches and temporary files"

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

test:
	pytest

up:
	docker compose up -d

down:
	docker compose down

dashboard:
	streamlit run dashboard/app.py

backfill:
	python -m nse_pipeline.cli backfill

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
