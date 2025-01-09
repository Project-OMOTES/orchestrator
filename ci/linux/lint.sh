#!/usr/bin/env bash

which python
which python3
ls -alh .
echo $PWD
python --version
python3 --version

if [[ "$OSTYPE" != "win32" && "$OSTYPE" != "msys" ]]; then
  echo "Activating .venv first."
  . .venv/bin/activate
  echo .venv/bin/activate
fi


which python
which python3
ls -alh .
echo $PWD
python --version
python3 --version

flake8 ./src/omotes_orchestrator ./unit_test/
