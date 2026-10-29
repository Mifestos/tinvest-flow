.PHONY: up down logs test lint format

up:
	docker-compose up -d --build

down:
	docker-compose down -v

logs:
	docker-compose logs -f --tail=100

test:
	pytest

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/