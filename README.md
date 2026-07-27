# Omotes REST API - FastAPI Version

FastAPI+Pydantic REST API for Omotes job management and workflows.

## Project Structure

```
omotes-rest-fastapi/
├── src/
│   └── orchestrator/
│       ├── __init__.py
│       ├── main.py              # FastAPI app factory
│       ├── models.py            # Pydantic models
│       └── routes/
│           ├── __init__.py
│           ├── job.py           # Job management endpoints
│           └── workflow.py      # Workflow endpoints
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Endpoints

### Job Management (`/job`)

- `POST /job/` - Start a new job
- `GET /job/` - List all jobs
- `GET /job/{job_id}` - Get job details
- `DELETE /job/{job_id}` - Delete a job
- `GET /job/{job_id}/status` - Get job status
- `GET /job/{job_id}/result` - Get job result with output ESDL
- `GET /job/{job_id}/logs` - Get job logs
- `GET /job/user/{user_name}` - Get all jobs by user
- `GET /job/project/{project_name}` - Get all jobs by project

### Workflows (`/workflow`)

- `GET /workflow/` - Get the current in-memory workflows list
- `POST /workflow/` - Post a JSON list and replace the current in-memory workflows list
- On startup, the app preloads workflows from `WORKFLOW_SETTINGS_FILE` when that env var is set

## Installation

```bash
# Development install with dev dependencies
pip install -e ".[dev]"
```

### Production Install

```bash
pip install -e .
```

## Running the Application

### Development

```bash
uvicorn orchestrator.main:app --reload --host 0.0.0.0 --port 5000
```

### Production

```bash
uvicorn orchestrator.main:app --host 0.0.0.0 --port 5000 --workers 4
```

### Docker

Build:

```bash
docker build -t omotes-rest-fastapi:latest .
```

Run:

```bash
docker run -p 5000:5000 omotes-rest-fastapi:latest
```

## Testing

```bash
pytest tests/
```

With coverage:

```bash
pytest --cov=orchestrator tests/
```

## API Documentation

Once running, visit:

- Interactive API docs: http://localhost:5000/docs
- Alternative API docs: http://localhost:5000/redoc

## Status

🚧 **Work in Progress** - This is a new FastAPI version of the Omotes REST API.
Currently implements endpoint signatures. Business logic (TODO) to be implemented step by step.

## Next Steps

- [ ] Implement job submission logic
- [ ] Implement database integration
- [ ] Implement job status tracking
- [x] Implement workflow retrieval
- [ ] Add authentication/authorization
- [ ] Add comprehensive error handling
- [ ] Add request logging and monitoring
