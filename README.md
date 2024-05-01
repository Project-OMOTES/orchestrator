# OMOTES orchestrator

This repository is part of the 'Nieuwe Warmte Nu Design Toolkit' project. 

Orchestrator component of OMOTES project which monitors workflows and starts the various steps of
each workflow.

# Directory structure
The following directory structure is used:

- `ci/`: Contains all CI & other development scripts to help standardize the development workflow
    for both Linux and Windows.
- `computation-engine-at-orchestrator/`: Submodule link to the latest `computation-engine` release.
    Necessary for infrastructure that is necessary during the integration test.
- `integration_test/`: Contains a large integration test which is run to check for robustness and
    stability.
- `src/`: Source code for the orchestrator as well as the necessary database models.
- `unit_test/`: All unit tests for the orchestrator.
- `.dockerignore`: Contains all files and directories which should not be available while building 
    the docker image.
- `.env-template`: Template `.env` file to run the orchestrator locally outside of docker.
- `.gitignore`: Contains all files and directories which are not kept in Git source control.
- `dev-requirements.txt`: Pinned versions of all development and non-development dependencies.
- `Dockerfile`: The build instructions for building the docker image.
- `pyproject.toml`: The Python project (meta) information.
- `requirements.txt`: Pinned versions of all dependencies needed to run the orchestrator.
- `run.sh`: Script to start the orchestrator locally outside of docker on Linux.
- `run_windows.sh`: Script to start the orchestrator locally outside of docker on Windows.

# Development workflow
The scripts under `ci/` are used to standardize the development proces. The following scripts are
available for Windows (under `ci/win32/` with extension `.cmd`) and Linux (under `ci/linux/` with
extension `.sh).

- `create_venv`: Creates a local virtual environment (`.venv/`) in which all dependencies may be
  installed.
- `db_models_apply_schema`: Will apply all available SQL db schema revisions to the local SQL
  database.
- `db_models_generate_new_revision`: Can be used to generate a new revision of the SQL db schema.
  Expects 1 argument e.g. `ci/linux/generate_new_db_models.sh "this is the revision message`.
- `install_dependencies`: Installs all development and non-development dependencies in the local
  virtual environment.
- `lint`: Run the `flake8` to check for linting issues.
- `test_unit`: Run all unit tests under `unit_test/` using `pytest`.
- `typecheck`: Run `mypy` to check the type annotations and look for typing issues.
- `update_dependencies`: Update `dev-requirements.txt` and `requirements.txt` based on the
  dependencies specified in `pyproject.toml`

A normal development workflow would be to first use `create_venv` and then finish setting up the
environment using `install_dependencies`. Finally, once all changes are made, you can use `lint`,
`test_unit` and `typecheck` to check for code quality issues.

In case you need to run the orchestrator locally, both `run.sh` and `run_windows.sh` are available
to run the orchestrator bare-metal (without docker).

All these scripts are expected to run from the root of the repository

## Working with computation-engine submodule
The [computation-engine](https://github.com/Project-OMOTES/computation-engine/) is available
as a submodule at `computation-engine-at-orchestrator`. The name of this path is chosen
to make sure starting the `computation-engine` with `docker compose` uses the
`computation-engine-at-orchestrator` project name instead of `computation-engine`. If a developer
is both developing the `orchestrator` and non-submodule `computation-engine` otherwise the 
environments may conflict in docker.

To make the submodule available after cloning this repository:
```bash
git submodule update --init
```

Also, checking out a different branch on `orchestrator` may reference a different commit than
the branch you are moving from. So, whenever you checkout a new branch, make sure you run
the command:
```bash
git submodule update --init
```

This will update the reference to point to the correct submodule.

## How to work with alembic to make database revisions
First set up the development environment with `create_venv` and `install_dependencies`. Then you
can make the necessary changes to `omotes_orchestrator/db_models/`. Finally, a new SQL schema
revision may be generated using `alembic` by running `generate_new_db_models`. In order to apply
all database revisions you can run `db_models_apply_schema`.

Do not forget to actually start the PostgreSQL database locally!
This may be done with:
```bash
cd computation-engine-at-orchestrator/
cp .env-template .env
./scripts/setup.sh
./scripts/start_postgres_in_dev_mode.sh  # This will start PostgreSQL with port 5432 opened on localhost 
```

## Direct Alembic control
In case more control is necessary, you can run the necessary alembic commands directly after
activating the virtual environment (Linux: `. ./.venv/bin/activate`, 
Windows: `call venv\Scripts\activate.bat`).

First, change directory: `cd src/`

- Make a revision: `alembic revision --autogenerate -m "<some message>"`
- Perform all revisions: `alembic upgrade head`
- Downgrade to a revision: `alembic downgrade <revision>` (revision 'base' to 
  undo everything.)
