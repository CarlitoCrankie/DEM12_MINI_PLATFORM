include .env
export

.PHONY: help up down clean logs ps generate trigger validate shell-pg shell-airflow health

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up:             ## Start all services
	docker compose up -d
	@echo ""
	@echo "  Airflow  → http://localhost:$(AIRFLOW_WEBSERVER_PORT)  ($(AIRFLOW_ADMIN_USER)/$(AIRFLOW_ADMIN_PASSWORD))"
	@echo "  MinIO    → http://localhost:$(MINIO_CONSOLE_PORT)     ($(MINIO_ROOT_USER)/$(MINIO_ROOT_PASSWORD))"
	@echo "  Metabase → http://localhost:$(METABASE_PORT)"

down:           ## Stop services (keep data)
	docker compose stop

clean:          ## Stop and remove all volumes — DELETES DATA
	docker compose down -v

logs:           ## Follow all logs
	docker compose logs -f

ps:             ## Show service status
	docker compose ps

generate:       ## Generate and upload sample CSV data to MinIO
	docker compose run --rm data-generator

trigger:        ## Trigger the sales pipeline DAG
	docker compose exec airflow-scheduler airflow dags trigger sales_pipeline

validate:       ## Trigger the data flow validation DAG
	docker compose exec airflow-scheduler airflow dags trigger data_flow_validation

shell-pg:       ## Open psql shell to salesdb
	docker compose exec postgres psql -U $(SALES_DB_USER) -d $(SALES_DB)

shell-airflow:  ## Open bash shell in Airflow scheduler
	docker compose exec airflow-scheduler bash

health:         ## Check health of all services
	@echo "PostgreSQL :" && docker compose exec postgres pg_isready -U $(POSTGRES_ADMIN_USER)
	@echo "MinIO      :" && docker compose exec minio mc ready local
	@echo "Airflow    :" && curl -sf http://localhost:$(AIRFLOW_WEBSERVER_PORT)/health | python3 -m json.tool
	@echo "Metabase   :" && curl -sf http://localhost:$(METABASE_PORT)/api/health    | python3 -m json.tool
