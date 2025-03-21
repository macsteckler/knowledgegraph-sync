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

echo "Environment setup complete! You can now run: python3 sync_to_neo4j.py" 