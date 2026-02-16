#!/usr/bin/env bash
set -euo pipefail

# default to running passed command
if [ "$#" -eq 0 ]; then
  echo "No command provided. Example: python run_parser.py --input /data/RVTools.xlsx --sheet vInfo --out /data/chunks --chunk-size 5000"
  exec bash
else
  exec "$@"
fi
