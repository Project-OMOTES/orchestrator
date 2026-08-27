import os

# Set required env vars for testing before importing orchestrator modules
os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")
os.environ.setdefault("PREFECT_API_AUTH_STRING", "test-token")
os.environ.setdefault("MINIO_HOST", "localhost")
os.environ.setdefault("MINIO_PORT", "9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "test-access-key")
os.environ.setdefault("MINIO_SECRET", "test-secret")
