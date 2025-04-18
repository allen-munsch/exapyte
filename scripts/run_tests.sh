#!/bin/bash
# Run tests with coverage

# Exit on error
set -e

# Change to project root directory
cd "$(dirname "$0")/.."

# Run pytest with coverage
#python -m pytest tests/ -v --cov=src/exapyte --cov-report=term --cov-report=html
#echo "Coverage report generated in htmlcov/"

python -m pytest -svvl tests/
