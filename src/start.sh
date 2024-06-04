#!/bin/bash

set -e

echo "Upgrading SQL schema."
alembic upgrade head
echo "Starting orchestrator."
python -m omotes_orchestrator.main
