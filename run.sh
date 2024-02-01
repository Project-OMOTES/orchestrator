#!/usr/bin/env sh

. .venv/bin/activate

PYTHONPATH="src/" python -m omotes_orchestrator.main
