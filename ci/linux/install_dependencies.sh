#!/usr/bin/env sh

if [[ "$OSTYPE" != "win32" && "$OSTYPE" != "msys" ]]; then
  . .venv/bin/activate
fi

pip-sync ./dev-requirements.txt ./requirements.txt
