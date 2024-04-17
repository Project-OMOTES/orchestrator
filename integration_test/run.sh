#!/bin/bash

#!/bin/bash

if [[ "$OSTYPE" != "win32" && "$OSTYPE" != "msys" ]]; then # Linux
  . .venv/bin/activate
 python3 job_submitter.py
else
  source venv/Scripts/activate
  python job_submitter.py
fi
