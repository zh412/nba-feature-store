.PHONY: install lint test run

install:
	pip install -r requirements.txt

lint:
	flake8 .

test:
	PYTHONPATH=src pytest

run:
	PYTHONPATH=src python -m nba_feature_store