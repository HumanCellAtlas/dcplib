.PHONY: test lint tests
MODULES=dcplib tests

test: lint tests

lint:
	flake8 $(MODULES) *.py

tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=dcplib \
		-m unittest discover --start-directory tests --top-level-directory . --verbose
