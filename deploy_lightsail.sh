#!/bin/bash

# Update system
sudo dnf update -y

# Install Python and dependencies
sudo dnf install -y python3-pip python3-devel gcc

# Create project directory
mkdir -p ~/knowledgegraph
cd ~/knowledgegraph

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
pip install psycopg2-binary neo4j python-dotenv

# Create .env file
cat > .env << EOL
# Supabase Transaction Pooler connection
user=postgres.jcotpnywnrywfthrzvdk
password=8P9WzFGiVQEjencW
host=aws-0-us-west-1.pooler.supabase.com
port=6543
dbname=postgres

# Neo4j Aura credentials
NEO4J_URI=neo4j+s://8549a3f3.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=VKaEpgBmAg66dP6CtBscJEy5Kk-CLnipTKyfDz4gDms
AURA_INSTANCEID=8549a3f3
AURA_INSTANCENAME=Instance01
EOL

# Copy sync script
cat > sync_to_neo4j.py << 'EOL'
import os
import psycopg2
from neo4j import GraphDatabase
from dotenv import load_dotenv
import sys
from decimal import Decimal

# Load environment variables
load_dotenv()

# Postgres connection parameters
DATABASE_URL = f"postgresql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}"

# Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")

[REST OF THE SYNC_TO_NEO4J.PY FILE CONTENT]
EOL

# Make scripts executable
chmod +x sync_to_neo4j.py

echo "Deployment complete! You can now run: python3 sync_to_neo4j.py" 