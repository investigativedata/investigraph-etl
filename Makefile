all: clean install test

agent:
	prefect agent start -q "default"

server:
	prefect server start

test:
	rm -rf .test
	poetry run pytest tests -s --cov=investigraph --cov-report term-missing
	rm -rf .test

typecheck:
	poetry run mypy --strict investigraph

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
