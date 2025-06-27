.PHONY: help setup test run clean docker-up docker-down docker-logs

# Variables
PYTHON := python3
PROJECT_NAME := ecommerce-analytics-platform
DOCKER_COMPOSE := docker-compose

# Detect Python version
PYTHON_VERSION := $(shell python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
ifeq ($(PYTHON_VERSION),)
    PYTHON := python
else
    PYTHON := python3
endif

# Colors
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
BLUE := \033[0;34m
NC := \033[0m # No Color

##@ General

help: ## Show this help message
	@echo '$(BLUE)$(PROJECT_NAME)$(NC)'
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Development Setup

setup: ## Setup development environment
	@echo "$(GREEN)Setting up development environment...$(NC)"
	$(PYTHON) -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt
	./venv/bin/pip install -r requirements-dev.txt
	@echo "$(GREEN)✓ Setup complete! Activate with: source venv/bin/activate$(NC)"

install: ## Install dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements-dev.txt

##@ Testing

test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	pytest tests/ -v

test-unit: ## Run unit tests only
	pytest tests/unit -v

test-integration: ## Run integration tests only
	pytest tests/integration -v

test-coverage: ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

test-fast: ## Run tests in parallel
	pytest tests/ -v -n auto

##@ Code Quality

format: ## Format code with black and isort
	@echo "$(GREEN)Formatting code...$(NC)"
	black src tests
	isort src tests

lint: ## Run linting checks
	@echo "$(GREEN)Running linting checks...$(NC)"
	flake8 src tests --max-line-length=100 --ignore=E203,W503
	mypy src --ignore-missing-imports

quality: format lint ## Run all code quality checks
	@echo "$(GREEN)✓ Code quality checks passed!$(NC)"

##@ Pipeline

run: ## Run the pipeline locally
	@echo "$(GREEN)Running analytics pipeline...$(NC)"
	$(PYTHON) -m src.pipeline.main

download-data: ## Download dataset from Kaggle
	@echo "$(GREEN)Downloading e-commerce dataset...$(NC)"
	$(PYTHON) scripts/download_data.py

##@ Docker - Build Commands

docker-build: ## Build all Docker images
	@echo "$(GREEN)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build

docker-build-analytics: ## Build only analytics container
	@echo "$(GREEN)Building analytics container...$(NC)"
	docker build -t $(PROJECT_NAME) .

docker-build-dev: ## Build development Docker image
	@echo "$(GREEN)Building development Docker image...$(NC)"
	docker build -f docker/Dockerfile.dev -t $(PROJECT_NAME)-dev .

docker-build-retry: ## Build with retry logic
	@echo "$(GREEN)Building with retry logic...$(NC)"
	@chmod +x scripts/docker_build_with_retry.sh 2>/dev/null || true
	@./scripts/docker_build_with_retry.sh

docker-build-minimal: ## Build minimal services (no Spark/Java)
	@echo "$(GREEN)Building minimal services...$(NC)"
	$(DOCKER_COMPOSE) build postgres analytics

##@ Docker - Runtime Commands

docker-up: ## Start all services with Docker
	@echo "$(GREEN)Starting services...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)✓ Services started!$(NC)"
	@echo "Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "Spark UI: http://localhost:8081"
	@echo "Jupyter: http://localhost:8888"

docker-up-minimal: ## Start minimal services
	@echo "$(GREEN)Starting minimal services...$(NC)"
	$(DOCKER_COMPOSE) up -d postgres analytics
	@echo "$(GREEN)✓ Minimal services started!$(NC)"

docker-down: ## Stop all Docker services
	@echo "$(YELLOW)Stopping services...$(NC)"
	$(DOCKER_COMPOSE) down

docker-restart: docker-down docker-up ## Restart all services

##@ Docker - Logging & Monitoring

docker-logs: ## View Docker logs
	$(DOCKER_COMPOSE) logs -f

docker-logs-analytics: ## View analytics service logs
	$(DOCKER_COMPOSE) logs -f analytics

docker-logs-airflow: ## View Airflow logs
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler

docker-ps: ## Show running containers
	$(DOCKER_COMPOSE) ps

docker-stats: ## Show container resource usage
	docker stats --no-stream

##@ Docker - Development

docker-shell: ## Open shell in analytics container
	$(DOCKER_COMPOSE) run --rm analytics-dev /bin/bash

docker-shell-airflow: ## Open shell in Airflow container
	$(DOCKER_COMPOSE) exec airflow-webserver /bin/bash

docker-dev: ## Start development environment
	@echo "$(GREEN)Starting development environment...$(NC)"
	$(DOCKER_COMPOSE) run --rm --service-ports analytics-dev

docker-jupyter: ## Start Jupyter notebook
	@echo "$(GREEN)Starting Jupyter notebook...$(NC)"
	$(DOCKER_COMPOSE) up -d jupyter
	@echo "$(GREEN)Jupyter available at: http://localhost:8888$(NC)"

##@ Docker - Testing

docker-test: ## Run tests in Docker
	@echo "$(GREEN)Running tests in Docker...$(NC)"
	$(DOCKER_COMPOSE) run --rm analytics pytest tests/ -v

docker-test-unit: ## Run unit tests in Docker
	@echo "$(GREEN)Running unit tests in Docker...$(NC)"
	$(DOCKER_COMPOSE) run --rm analytics pytest tests/unit -v

docker-test-integration: ## Run integration tests in Docker
	@echo "$(GREEN)Running integration tests in Docker...$(NC)"
	$(DOCKER_COMPOSE) run --rm analytics pytest tests/integration -v

test-docker-setup: ## Run Docker setup tests
	@echo "$(GREEN)Running Docker setup tests...$(NC)"
	@chmod +x scripts/test_docker_setup.sh
	@./scripts/test_docker_setup.sh

test-docker-all: test-docker-setup test-docker-python ## Run all Docker tests

##@ Docker - Pipeline Operations

docker-run-pipeline: ## Run pipeline in Docker
	@echo "$(GREEN)Running pipeline in Docker...$(NC)"
	$(DOCKER_COMPOSE) run --rm analytics python -m src.pipeline.main

docker-download-data: ## Download data using Docker
	@echo "$(GREEN)Downloading data in Docker...$(NC)"
	$(DOCKER_COMPOSE) run --rm analytics python scripts/download_data.py

##@ Docker - Maintenance

docker-clean: ## Remove all containers and volumes
	@echo "$(RED)Removing all containers and volumes...$(NC)"
	$(DOCKER_COMPOSE) down -v
	docker system prune -f

docker-clean-images: ## Remove project Docker images
	@echo "$(RED)Removing project Docker images...$(NC)"
	docker rmi $(PROJECT_NAME) $(PROJECT_NAME)-dev 2>/dev/null || true
	$(DOCKER_COMPOSE) down --rmi local

validate-docker: ## Validate Docker configuration
	@echo "$(GREEN)Validating Docker configuration...$(NC)"
	@docker-compose config > /dev/null && echo "✅ docker-compose.yml is valid" || echo "❌ docker-compose.yml has errors"
	@echo "$(GREEN)Checking Dockerfile syntax...$(NC)"
	@docker build --no-cache -f Dockerfile . --dry-run 2>/dev/null && echo "✅ Dockerfile syntax valid" || echo "⚠️  Dockerfile needs review"

##@ Airflow Commands

airflow-shell: ## Access Airflow shell
	$(DOCKER_COMPOSE) exec airflow-webserver airflow shell

airflow-info: ## Show Airflow info
	$(DOCKER_COMPOSE) exec airflow-webserver airflow info

airflow-list-dags: ## List all DAGs
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags list

airflow-trigger-dag: ## Trigger analytics DAG
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger ecommerce_analytics_pipeline

##@ Cleanup

clean: ## Clean generated files and caches
	@echo "$(YELLOW)Cleaning generated files...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov
	rm -rf spark-warehouse metastore_db derby.log
	rm -rf logs/*.log
	@echo "$(GREEN)✓ Cleanup complete!$(NC)"

clean-data: ## Clean data directories
	@echo "$(YELLOW)Cleaning data directories...$(NC)"
	rm -rf data/raw/* data/processed/*
	touch data/raw/.gitkeep data/processed/.gitkeep

clean-all: clean clean-data docker-clean ## Clean everything
	@echo "$(GREEN)✓ Full cleanup complete!$(NC)"

##@ Documentation

docs: ## Build documentation
	@echo "$(GREEN)Building documentation...$(NC)"
	cd docs && mkdocs build

docs-serve: ## Serve documentation locally
	cd docs && mkdocs serve

##@ Shortcuts

all: quality test ## Run quality checks and tests

pipeline-local: setup download-data run ## Complete local pipeline setup and run

pipeline-docker: docker-build docker-up ## Complete Docker pipeline setup

dev: docker-build-dev docker-dev ## Start development environment

quick-start: docker-build-minimal docker-up-minimal ## Quick start with minimal services

.DEFAULT_GOAL := help