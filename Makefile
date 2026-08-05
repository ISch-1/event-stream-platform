.PHONY: help up down clean logs build test it run smoke seed schemas topics

help:
	@echo "Targets:"
	@echo "  up        Start the full stack (Kafka, Schema Registry, ClickHouse, observability)"
	@echo "  down      Stop containers (keep volumes)"
	@echo "  clean     Stop and remove volumes"
	@echo "  logs      Tail logs from all services"
	@echo "  build     Build the app jar"
	@echo "  test      Run unit tests"
	@echo "  it        Run integration tests (Testcontainers)"
	@echo "  run       Run the app with the dev profile"
	@echo "  smoke     Run the k6 smoke script against http://localhost:8080"
	@echo "  seed      Publish 5 sample events via the ingest API"
	@echo "  schemas   Print schemas registered in Schema Registry"
	@echo "  topics    List Kafka topics"

up:
	docker compose up -d
	@echo "→ Kafka UI:        http://localhost:8085"
	@echo "→ Schema Registry: http://localhost:8081"
	@echo "→ ClickHouse HTTP: http://localhost:8123/play"
	@echo "→ Grafana:         http://localhost:3000"
	@echo "→ Prometheus:      http://localhost:9090"
	@echo "→ Jaeger:          http://localhost:16686"

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

build:
	./mvnw -q -DskipTests package

test:
	./mvnw -q test

it:
	./mvnw -q verify

run:
	./mvnw spring-boot:run -Dspring-boot.run.profiles=dev

smoke:
	k6 run load/ingest-smoke.js

seed:
	@for i in 1 2 3 4 5; do \
	  curl -sf -X POST http://localhost:8080/api/v1/events \
	    -H 'Content-Type: application/json' \
	    -d "{\"type\":\"page_view\",\"userId\":\"user-$$i\",\"payload\":{\"path\":\"/home\",\"referrer\":\"google\"}}" | jq . ; \
	done

schemas:
	@curl -sf http://localhost:8081/subjects | jq

topics:
	docker exec esp-kafka kafka-topics --bootstrap-server localhost:9092 --list
