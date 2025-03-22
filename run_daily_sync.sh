#!/bin/bash

# Change to the project directory
cd ~/knowledgegraph

# Activate virtual environment
source venv/bin/activate

# Run the daily sync script
python3 sync_daily_updates.py >> sync_daily.log 2>&1

# Deactivate virtual environment
deactivate 