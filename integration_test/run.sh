#!/bin/bash

if [[ "$OSTYPE" != "win32" && "$OSTYPE" != "msys" ]]; then # Linux
  . .venv/bin/activate
 python3 src/job_submitter.py
else
  source venv/Scripts/activate
  python src/job_submitter.py
fi
