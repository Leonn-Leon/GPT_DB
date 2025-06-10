# Makefile
include .env
export

# Основные команды
build:
	docker-compose build
up:
	docker-compose up -d
down:
	docker-compose down --remove-orphans
logs:
	docker-compose logs -f gpt-db-app
shell:
	docker-compose exec gpt-db-app /bin/bash

# Комплексные команды
start: build up
restart: down build up
