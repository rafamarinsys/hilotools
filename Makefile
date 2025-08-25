.PHONY: run-all ingest model analytics test

run-all:
	python -m pipeline run-all --prefer-gdrive

ingest:
	python -m pipeline ingest --prefer_gdrive

model:
	python -m pipeline model

analytics:
	python -m pipeline analytics

test:
	pytest -q