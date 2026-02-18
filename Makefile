.PHONY: up down logs ps

up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down

logs:
	docker compose -f infra/docker-compose.yml logs -f --tail=200

ps:
	docker compose -f infra/docker-compose.yml ps
