.PHONY: up down reset logs seed smoke backend-test frontend-test test e2e e2e-ui recovery dlq migrations

up:
	docker compose up --build -d

down:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans
	docker compose up --build -d

logs:
	docker compose logs -f --tail=200

seed:
	docker compose run --rm seed-data

smoke:
	docker compose --profile tools run --rm smoke

backend-test:
	docker compose --profile tools run --rm order-tests
	docker compose --profile tools run --rm supplier-tests
	docker compose --profile tools run --rm customer-tests

frontend-test:
	docker compose --profile tools run --rm frontend-tests

test: backend-test frontend-test

e2e:
	docker compose --profile tools run --rm e2e

e2e-ui:
	cd e2e && pnpm test:ui

recovery:
	bash scripts/recovery-smoke.sh

dlq:
	docker compose --profile tools run --rm dlq-tools list

migrations:
	docker compose run --rm order-migrations alembic -c /app/alembic.ini current
	docker compose run --rm supplier-migrations alembic -c /app/alembic.ini current
