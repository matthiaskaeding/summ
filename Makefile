.DEFAULT_GOAL := help

help:
	@echo "Available targets:"
	@echo "  data   - Create data directories and generate datasets"
	@echo "  help   - Display this help message"

data:
	mkdir -p data/subfolder
	duckdb < scripts/make-dfs.sql
.PHONY: data
