.PHONY: install
install: ## Install the poetry environment and install the pre-commit hooks
	@echo "🚀 Creating virtual environment using pyenv and poetry"
	@pre-commit install
	@pre-commit autoupdate

.PHONY: check
check:
	@echo "🚀 Checking Poetry lock file consistency with 'pyproject.toml': Running poetry lock --check"
	@pre-commit run -a
	@mypy

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@poetry run pytest -v --cov --cov-config=pyproject.toml

.PHONY: build
build: clean-build ## Build wheel file using poetry
	@echo "🚀 Creating wheel file"
	@poetry build	

.PHONY: clean-build
clean-build: ## clean build artifacts
	@rm -rf dist

publish:
	$(MAKE) publish.codeartifact

.PHONY: publish.pypi
publish.pypi: ## publish a release to pypi.
	@echo "🚀 Publishing: Dry run."
	@poetry config pypi-token.pypi $(PYPI_TOKEN)
	@poetry publish --dry-run
	@echo "🚀 Publishing."
	@poetry publish