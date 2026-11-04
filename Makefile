.PHONY: install demo test lint verify clean
install:
	python -m pip install -r requirements.lock
	python -m pip install --no-deps -e .
demo:
	retail fixture
	retail ingest data/fixtures/initial.csv --provenance synthetic-fixture
	retail run
	retail ingest data/fixtures/late.csv --provenance synthetic-fixture
	retail run
	retail query "SELECT sum(net_sales) AS net_sales FROM mart_daily_sales"
test:
	pytest -q
lint:
	ruff check .
verify: lint test
clean:
	@echo "Delete data/ manually only after reviewing its immutable source files and releases."
