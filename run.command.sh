#!/usr/bin/env bash
set -euo pipefail
python run_pipeline.py --full data/full.csv --workdir out --writeback-notion
