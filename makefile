# Makefile for running Python Scripts Individually or in Sequence

# Set default location
SCRIPTS_DIR := /Users/rafsanbhuiyan/Documents/GitHub/life_policy_proj/src

# Run all scripts in order by typing 'make' in your terminal or command prompt.
all: setup create_db life_policy_job

# Install Dependencies
setup:
	pipenv install

# Run create_db.py by typing 'make create_db' in your terminal or command prompt.
create_db:
	@echo "Running create_db.py..."
	python3 $(SCRIPTS_DIR)/create_db.py  # This line runs create_db.py with python3.

# Run life_policy_job.py by typing 'make life_policy_job' in your terminal or command prompt.
life_policy_job:
	@echo "Running life_policy_job.py..."
	python3 $(SCRIPTS_DIR)/life_policy_job.py  # This line runs life_policy_job.py with python3.