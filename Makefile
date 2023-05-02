all: clean install test

install:
	pip install -e .
	pip install twine coverage nose moto pytest pytest-cov black flake8 isort bump2version mypy ipdb

test:
	pytest tests -s --cov=investigraph --cov-report term-missing

typecheck:
	mypy --strict ftm_geocode

build:
	python setup.py sdist bdist_wheel

release: clean build
	twine upload dist/*

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
