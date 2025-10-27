.PHONY: help start stop restart logs logs-postgres logs-metabase status psql clean reset backup restore run-etl verify install

# Load environment variables
include settings.env
export

# Colors
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

help: ## Show this help message
	@echo "$(GREEN)Mastodon Data Pipeline - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-18s$(NC) %s\n", $$1, $$2}'

# Docker Services
start: ## Start PostgreSQL and Metabase services
	@echo "$(GREEN)Starting PostgreSQL and Metabase services...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "$(YELLOW)PostgreSQL: localhost:$(DATABASE_PORT)$(NC)"
	@echo "$(YELLOW)Metabase: http://localhost:3000$(NC)"

stop: ## Stop all services
	@echo "$(YELLOW)Stopping services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)Services stopped$(NC)"

restart: ## Restart all services
	@echo "$(YELLOW)Restarting services...$(NC)"
	@docker-compose restart
	@echo "$(GREEN)Services restarted$(NC)"

logs: ## Show logs for all services
	@echo "$(GREEN)Showing logs (Ctrl+C to exit)...$(NC)"
	@docker-compose logs -f

logs-postgres: ## Show PostgreSQL logs
	@echo "$(GREEN)Showing PostgreSQL logs...$(NC)"
	@docker-compose logs -f postgres

logs-metabase: ## Show Metabase logs
	@echo "$(GREEN)Showing Metabase logs...$(NC)"
	@docker-compose logs -f metabase

status: ## Show service status
	@echo "$(GREEN)Service status:$(NC)"
	@docker-compose ps

# Database Operations
psql: ## Connect to PostgreSQL shell
	@echo "$(GREEN)Connecting to PostgreSQL...$(NC)"
	@docker-compose exec postgres psql -U $(DATABASE_USER) -d $(DATABASE_NAME)

db-query: ## Run SQL query (usage: make db-query SQL='SELECT * FROM ...')
	@if [ -z "$(SQL)" ]; then \
		echo "$(RED)Usage: make db-query SQL='SELECT * FROM hashtag_based.toots_with_sentiment_data LIMIT 5'$(NC)"; \
		exit 1; \
	fi
	@docker-compose exec postgres psql -U $(DATABASE_USER) -d $(DATABASE_NAME) -c "$(SQL)"

clean: ## Remove all data and volumes (WARNING: destructive)
	@echo "$(RED)WARNING: This will remove all data and volumes!$(NC)"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		docker-compose down -v; \
		echo "$(GREEN)All data cleaned$(NC)"; \
	else \
		echo "$(YELLOW)Cleanup cancelled$(NC)"; \
	fi

reset: ## Reset services (keep data)
	@echo "$(YELLOW)Resetting services (keeping data)...$(NC)"
	@docker-compose down
	@docker-compose up -d
	@echo "$(GREEN)Services reset$(NC)"

backup: ## Backup database
	@BACKUP_FILE="backup_$$(date +%Y%m%d_%H%M%S).sql"; \
	echo "$(GREEN)Backing up database to $$BACKUP_FILE...$(NC)"; \
	docker-compose exec postgres pg_dump -U $(DATABASE_USER) $(DATABASE_NAME) > "$$BACKUP_FILE"; \
	echo "$(GREEN)Backup saved: $$BACKUP_FILE$(NC)"

restore: ## Restore database from backup (usage: make restore FILE=backup.sql)
	@if [ -z "$(FILE)" ]; then \
		echo "$(RED)Usage: make restore FILE=backup_file.sql$(NC)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Restoring database from $(FILE)...$(NC)"
	@cat "$(FILE)" | docker-compose exec -T postgres psql -U $(DATABASE_USER) $(DATABASE_NAME)
	@echo "$(GREEN)Database restored$(NC)"

# ETL Operations
run-etl: ## Run the ETL pipeline
	@echo "$(GREEN)Running ETL pipeline...$(NC)"
	@source .venv/bin/activate && python src/main.py

verify: ## Verify all components
	@echo "$(GREEN)Verifying components...$(NC)"
	@source .venv/bin/activate && python -c "from loader import ModelExecutor; ModelExecutor().verify_schemas()"

verify-loader: ## Verify PostgreSQL loader
	@echo "$(GREEN)Verifying PostgreSQL loader...$(NC)"
	@source .venv/bin/activate && python -c "from loader import BronzeLayerLoader, SilverLayerETL, GoldLayerRefresh; print('All loaders imported successfully')"

# Development
install: ## Install Python dependencies
	@echo "$(GREEN)Installing dependencies...$(NC)"
	@source .venv/bin/activate && uv pip install -r requirements.txt
	@echo "$(GREEN)Dependencies installed$(NC)"

# Full Pipeline
pipeline: start run-etl ## Start services and run full ETL pipeline
	@echo "$(GREEN)Complete pipeline executed!$(NC)"
	@echo "$(YELLOW)View results at http://localhost:3000$(NC)"

# Quick start for new developers
quickstart: install start ## Complete setup for new developers
	@echo "$(GREEN)Waiting 30 seconds for services to initialize...$(NC)"
	@sleep 30
	@echo "$(GREEN)Ready! Run 'make run-etl' to start processing data$(NC)"
	@echo "$(YELLOW)Metabase UI: http://localhost:3000$(NC)"

