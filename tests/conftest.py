import os

# Set required env vars for testing before importing orchestrator modules
os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")
os.environ.setdefault("PREFECT_API_AUTH_STRING", "test-token")
