# Data Observability Platform - Makefile
# Provides convenient commands for development and deployment

.PHONY: help install setup test clean run-profiler run-detector run-contract run-orchestrator run-production

# Default target
help:
	@echo "Data Observability Platform - Available Commands:"
	@echo ""
	@echo "Setup Commands:"
	@echo "  install     Install Python dependencies"
	@echo "  setup       Initialize database schema"
	@echo "  test        Run all tests including chaos engineering"
	@echo ""
	@echo "Development Commands:"
	@echo "  run-profiler    Run the profiler component"
	@echo "  run-detector    Run the detector component"
	@echo "  run-contract    Run the contract guard"
	@echo "  run-orchestrator Run the full pipeline"
	@echo "  run-production  Run production-hardened pipeline"
	@echo "  scorecard       Generate portfolio health report"
	@echo ""
	@echo "Utility Commands:"
	@echo "  clean       Clean temporary files and logs"
	@echo "  lint        Run code linting"
	@echo "  format      Format Python code"
	@echo ""

# Setup Commands
install:
	@echo "Installing Python dependencies..."
	pip install -r requirements.txt

setup:
	@echo "Setting up database schema..."
	python scripts/setup_monitoring.py

test:
	@echo "Running chaos engineering tests..."
	python tests/chaos_volume.py
	python tests/chaos_freshness.py
	python tests/chaos_contract.py

# Development Commands
run-profiler:
	@echo "Running profiler..."
	python src/profiler.py

run-detector:
	@echo "Running detector..."
	python src/detector.py

run-contract:
	@echo "Running contract guard..."
	python src/contract_guard.py

run-orchestrator:
	@echo "Running full pipeline..."
	python src/orchestrator.py

run-production:
	@echo "Running production-hardened pipeline..."
	python src/production_orchestrator.py

scorecard:
	@echo "Generating portfolio health report..."
	python scripts/generate_scorecard.py

# Utility Commands
clean:
	@echo "Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf logs/
	rm -f *.md

lint:
	@echo "Running code linting..."
	python -m flake8 src/ scripts/ tests/ --max-line-length=100

format:
	@echo "Formatting Python code..."
	python -m black src/ scripts/ tests/ --line-length=100

# Full development workflow
dev-setup: install setup
	@echo "Development environment setup complete!"

# Production deployment check
prod-check:
	@echo "Checking production readiness..."
	@test -f .env || (echo "❌ .env file missing" && exit 1)
	@python -c "import yaml; yaml.safe_load(open('config/databases.yaml'))" || (echo "❌ Invalid YAML config" && exit 1)
	@echo "✅ Production checks passed"
