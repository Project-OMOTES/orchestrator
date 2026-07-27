# CI tasks

# Install dependencies with dev group
install:
    uv sync --locked --group dev

# Run linter checks
lint:
    uv run ruff check ./src ./tests

# Fix linting issues
format:
    uv run ruff format ./src ./tests

# Check formatting without modifying files
format-check:
    uv run ruff format --check ./src ./tests

# Run type checker
typecheck:
    uv run ty check ./src ./tests

# Run unit tests
test:
    uv run pytest --junit-xml=test-results.xml tests/

# Run all checks (install, lint, format-check, typecheck, test)
ci: install lint format-check typecheck test

# Show this help message
help:
    @just --list
