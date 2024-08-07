all: clean install test

agent:
	poetry run prefect agent start -q "default"

server:
	poetry run prefect server start

lint:
	poetry run flake8 investigraph --count --select=E9,F63,F7,F82 --show-source --statistics
	poetry run flake8 investigraph --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

pre-commit:
	poetry run pre-commit install
	poetry run pre-commit run -a

test:
	rm -rf .test
	poetry run pytest tests -v --capture=sys --cov=investigraph --cov-report lcov
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
