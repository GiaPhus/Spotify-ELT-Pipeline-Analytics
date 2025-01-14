build:
	docker compose build

up:
	docker compose up

down:
	docker compose down

restart:
	make down && make up

