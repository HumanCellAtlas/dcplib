.PHONY: test lint tests
MODULES=dcplib tests

test: lint tests

lint:
	if [[ "$$(python --version)" < "Python 3.6" ]]; then FLAKE8_OPTS="--exclude dcplib/etl,dcplib/test_helpers,dcplib/component_agents,dcplib/component_entities"; fi; \
	flake8 $(MODULES) *.py $$FLAKE8_OPTS

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=dcplib \
		-m unittest discover --start-directory tests --top-level-directory . --verbose

include common.mk
