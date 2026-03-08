.PHONY: install lint test run monitor-command monitor-health monitor-integrity monitor-all pipeline-run

install:
	pip install -r requirements.txt

lint:
	flake8 .

test:
	PYTHONPATH=src pytest

run:
	PYTHONPATH=src python -m nba_feature_store


# -------------------------
# Monitoring Commands
# -------------------------

monitor-command:
	PYTHONPATH=src python -m nba_feature_store.monitoring.feature_store_command_center

monitor-health:
	PYTHONPATH=src python -m nba_feature_store.monitoring.data_health_audit

monitor-integrity:
	PYTHONPATH=src python -m nba_feature_store.monitoring.game_integrity_audit

monitor-all:
	$(MAKE) monitor-command
	$(MAKE) monitor-health
	$(MAKE) monitor-integrity


# -------------------------
# Full Pipeline Check
# -------------------------

pipeline-run:
	$(MAKE) run
	$(MAKE) monitor-all