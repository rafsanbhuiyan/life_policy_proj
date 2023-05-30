# Makefile for running Python Scripts Individually or in Sequence

# Set default location
SCRIPTS_DIR := /Users/rafsanbhuiyan/Documents/GitHub/life_policy_proj/src

# Run all scripts in order by typing 'make' in your terminal or command prompt.
all: setup create_db transform_and_load flask_api policy_info_app drop_database

# Install Dependencies
setup:
	pipenv install

# Run create_db.py by typing 'make create_db' in your terminal or command prompt.
create_db:
	@echo "Running create_db.py..."
	python3 $(SCRIPTS_DIR)/create_db.py

# Run transform_and_load.py by typing 'make transform_and_load' in your terminal or command prompt.
transform_and_load:
	@echo "Running life_policy_job.py..."
	python3 $(SCRIPTS_DIR)/transform_and_load.py

# This module provides API endpoints for life policy management using Flask.
flask_api:
	@echo "Running flask_api.py..."
	python3 $(SCRIPTS_DIR)/flask_api.py

# This module provides a command-line interface for interacting with the life policy information system.
policy_info_app:
	@echo "Running policy_info_app.py..."
	python3 $(SCRIPTS_DIR)/policy_info_app.py

# This module drops the specified Database
drop_database:
	@echo "Running drop_database.py..."
	python3 $(SCRIPTS_DIR)/drop_database.py

