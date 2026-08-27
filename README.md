# Omotes REST API - FastAPI Version

FastAPI+Pydantic REST API for Omotes workflows and job management using prefect.

## Endpoints

### Health and readiness

- `GET /health` - Liveness check: returns `200` when the API process is running

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

The workflows are configured in `WORKFLOW_SETTINGS_FILE`.
Each workflow contains `workflow_type_name`, `workflow_type_description_name` and `prefect_flow_name`.
Optional are `workflow_parameters` and `memory_limit` which is for example: `512Mi`, `2Gi`, `750M` or `1000000`.\
`workflow_parameters` is a dict in jsonforms format, see `/config/workflow_config_example.json` and https://jsonforms.io/.

### Run/debug the orchestrator locally

In vscode go to the debug view and run `omotes_orchestrator`.

The app will start on `http://localhost:9200`

You can try out `POST /job/` on `http://localhost:9200/docs` with the omotes_system stack up (without the orchestrator) and use `config/job_post.json`.

**Note** to use local code for sdk run `.venv/bin/pip install -e ../omotes-sdk-python/` before starting the app

### Lint/typecheck/test locally

Run via just (also used in in github actions):

```bash
just ci            # run all CI checks (lint, security, format-check, typecheck, test)

just lint          # ruff checks
just security      # ruff security
just format        # ruff format
just format-check  # verify formatting
just typecheck     # ty type checking
just test          # pytest
```

To debug test go to the debug view in vscode and run "pytest".\
When using an editable install of the sdk, don't use the just command as `uv run ...` will remove this editable install.

## Project Structure

```
orchestrator/
├── src/
│   └── orchestrator/
│       ├── __init__.py
│       ├── main.py              # FastAPI app factory, lifespan, exception handling
│       ├── jsonforms.py         # JSON Forms schema validation helpers
│       ├── models.py            # Pydantic request and response models
│       ├── settings.py          # Environment configuration (Pydantic BaseSettings)
│       ├── workflow_types.py    # Workflow domain models (WorkflowDefinition)
│       ├── workflow_registry.py # In-memory workflow state management
│       └── routes/
│           ├── __init__.py
│           ├── job.py           # Job management endpoints (/job)
│           └── workflow.py      # Workflow endpoints (/workflow)
├── tests/
│   ├── conftest.py              # Pytest configuration and fixtures
│   └── test_workflow_routes.py  # Unit tests for workflow and job endpoints
├── config/
│   ├── workflow_config_example.json       # Example workflow definitions
│   ├── workflow_config_nwn_no_gurobi.json  # Example workflow definitions
│   ├── job_post.json                       # Example job request
│   └── job_post feedback.json              # Example job feedback
├── Dockerfile                   # Multi-stage production image
├── dev.Dockerfile                # Development container image
├── justfile                     # Task runner commands
├── pyproject.toml               # Dependencies and project metadata
├── uv.lock                       # Locked Python dependencies
└── README.md
```
