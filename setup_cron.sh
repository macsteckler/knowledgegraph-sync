#!/bin/bash

# Get the absolute path of the run_daily_sync.sh script
SCRIPT_PATH="$HOME/knowledgegraph/run_daily_sync.sh"

# Create a temporary file
TEMP_CRON=$(mktemp)

# Export existing crontab
crontab -l > "$TEMP_CRON" 2>/dev/null

# Check if the job already exists
if ! grep -q "$SCRIPT_PATH" "$TEMP_CRON"; then
    # Add the new cron job - runs at 2 AM every day
    echo "0 2 * * * $SCRIPT_PATH" >> "$TEMP_CRON"
    
    # Install the new cron file
    crontab "$TEMP_CRON"
    echo "Cron job installed successfully!"
else
    echo "Cron job already exists!"
fi

# Clean up
rm "$TEMP_CRON" 