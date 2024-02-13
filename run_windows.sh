#!/bin/bash

source ./venv/Scripts/activate

export PYTHONPATH=./src/
python -m omotes_orchestrator.main
