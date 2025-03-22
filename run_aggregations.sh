#!/bin/bash

# Change to the project directory
cd ~/knowledgegraph

# Activate virtual environment
source venv/bin/activate

# Run the appropriate aggregation based on the argument
case "$1" in
  "weekly")
    echo "Running weekly aggregation..."
    python3 aggregator_weekly.py >> logs/weekly_aggregation.log 2>&1
    ;;
  "monthly")
    echo "Running monthly aggregation..."
    python3 aggregator_monthly.py >> logs/monthly_aggregation.log 2>&1
    ;;
  *)
    echo "Usage: $0 {weekly|monthly}"
    exit 1
    ;;
esac

# Deactivate virtual environment
deactivate 