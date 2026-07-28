# BUILD STAGE
FROM python:3.12-slim AS builder

# Copy uv binaries from the official image into the builder stage
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory and optimization variables
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Copy dependency definition files to leverage Docker layer caching
COPY orchestrator/pyproject.toml orchestrator/uv.lock orchestrator/README.md ./

# Install project dependencies into the virtual environment (excluding dev packages)
RUN uv sync --frozen --no-install-project --no-dev

# Copy application source code
COPY orchestrator/src/ ./src/

# Install the project itself as an immutable package
RUN uv sync --frozen --no-dev

# install omotes-sdk-python and mesido from local code
COPY omotes-sdk-python/ /omotes-sdk-python/
RUN uv pip install --python /app/.venv/bin/python /omotes-sdk-python/

# RUN STAGE
FROM python:3.12-slim

WORKDIR /app

# Enforce secure Python runtime habits
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_PORT=9200

# Copy only the virtual environment and project code from the builder
COPY --from=builder /app /app

# Prepend the virtual environment binaries to the system PATH
ENV PATH="/app/.venv/bin:$PATH"

# Create a non-root system user for runtime application security
RUN useradd -u 8888 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 9200

# Execute using fastapi CLI with production server options
CMD ["sh", "-c", "fastapi run src/orchestrator/main.py --proxy-headers --port ${APP_PORT} --host 0.0.0.0"]
