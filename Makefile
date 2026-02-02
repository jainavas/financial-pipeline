.PHONY: help init up down restart logs test clean

help:
	@echo "Available commands:"
	@echo "  make init        - Initialize Airflow (first time setup)"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart services"
	@echo "  make logs        - View logs"
	@echo "  make test        - Run pipeline tests"
	@echo "  make clean       - Clean up volumes"

init:
	@echo "Initializing Airflow..."
	mkdir -p ./dags ./logs ./plugins
	echo -e "AIRFLOW_UID=$$(id -u)" > .env
	docker-compose up airflow-init

up:
	@echo "Starting services..."
	docker-compose up -d

down:
	@echo "Stopping services..."
	docker-compose down

restart:
	@echo "Restarting services..."
	docker-compose restart

logs:
	@echo "Viewing logs..."
	docker-compose logs -f

logs-airflow:
	@echo "Viewing Airflow scheduler logs..."
	docker-compose logs -f airflow-scheduler

test:
	@echo "Running pipeline test..."
	@mkdir -p logs
	@chmod 755 logs
	@bash -c 'source venv/bin/activate && python pipeline.py'

clean:
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf logs/*