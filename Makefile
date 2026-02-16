.PHONY: build up down logs parser-run

build:
	docker compose build

up:
	docker compose up -d postgres neo4j minio redis backend worker frontend

down:
	docker compose down

logs:
	docker compose logs -f

parser-run:
	@echo "Run parser example:"
	@echo "docker compose run --rm parser python run_parser.py --input /data/RVTools.xlsx --sheet vInfo --out /data/chunks --chunk-size 5000"
