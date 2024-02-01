#!/usr/bin/env sh

. .venv/bin/activate
python -m mypy ./src/omotes_orchestrator ./unit_test/
