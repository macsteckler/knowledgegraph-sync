#!/bin/bash

# Update system
sudo dnf update -y

# Install Python and dependencies
sudo dnf install -y python3-pip python3-devel gcc

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
pip install psycopg2-binary neo4j python-dotenv

# Create log directory with proper permissions
mkdir -p ~/knowledgegraph/logs
chmod 755 ~/knowledgegraph/logs

# Ensure scripts are executable
chmod +x run_daily_sync.sh
chmod +x setup_cron.sh

echo "Environment setup complete! Daily sync has been configured." 