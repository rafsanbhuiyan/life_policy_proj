# Makefile for running Python Scripts Individually or in Sequence

# Set default location
SCRIPTS_DIR := /Users/rafsanbhuiyan/Documents/GitHub/life_policy_proj/src

# Run all scripts in order by typing 'make' in your terminal or command prompt.
all: setup create_db transform_and_load

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