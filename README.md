# Omotes REST API - FastAPI Version

FastAPI+Pydantic REST API for Omotes workflows and job management using prefect api.

## Endpoints

### Job Management (`/job`)

- `POST /job/` - Create and trigger a new job
- `GET /job/` - List all jobs from Prefect server
- `GET /job/{job_id}` - Get job details, status and results if available
- `DELETE /job/{job_id}` - Delete/terminate a job

### Workflows (`/workflow`)

- `GET /workflow/` - Get current in-memory workflows list with Prefect versions
- `POST /workflow/` - Upload and replace workflows in memory
- On startup, workflows are preloaded from `WORKFLOW_SETTINGS_FILE` (if set)

## Development

### Tools

This project uses:

- **uv**: Fast Python package manager and resolver. Install via [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)
- **just**: Command runner for common tasks (similar to Make). Install via [https://github.com/casey/just](https://github.com/casey/just)

### Setup

1. Install dependencies:

   ```bash
   uv sync
   ```

2. Copy `.env.template` to `.env`

### Run/debug the orchestrator locally

In vscode go to the debug view and run "omotes_orchestrator".

The app will start on `http://localhost:9200`

You can try out `POST /job/` on `http://localhost:9200/docs` with the omotes_system stack up (without the orchestrator) and use `config/job_post.json`.

**Note** to use local code for sdk run `uv pip install -e ../omotes-sdk-python/` before starting the app

### Lint/typecheck/test locally

Run via just (also used in in github actions):

```bash
just ci            # run all CI checks (lint, format-check, typecheck, test)

just lint          # ruff checks
just format        # ruff format
just format-check  # verify formatting
just typecheck     # ty type checking
just test          # pytest
```

To debug test go to the debug view in vscode and run "pytest".

## Project Structure

```
orchestrator/
├── src/
│   └── orchestrator/
│       ├── __init__.py
│       ├── main.py              # FastAPI app factory, lifespan, exception handling
│       ├── models.py            # Pydantic response models (JobSummary, WorkflowResponse, etc.)
│       ├── settings.py          # Environment configuration (Pydantic BaseSettings)
│       ├── workflow_types.py    # Workflow domain models (WorkflowType, WorkflowUpload)
│       ├── workflow_registry.py # In-memory workflow state management
│       └── routes/
│           ├── __init__.py
│           ├── job.py           # Job management endpoints (/job)
│           └── workflow.py      # Workflow endpoints (/workflow)
├── tests/
│   ├── conftest.py              # Pytest configuration and fixtures
│   ├── test_job_routes.py       # Unit tests for job endpoints
│   └── test_workflow_routes.py  # Unit tests for workflow endpoints
├── config/
│   ├── workflow_config_nwn_no_gurobi.json  # Example workflow definitions
│   └── *.json                   # Job feedback/status files
├── Dockerfile                   # Multi-stage production image
├── justfile                     # Task runner commands
├── pyproject.toml               # Dependencies and project metadata
└── README.md
```
