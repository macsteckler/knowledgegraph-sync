#!/bin/bash

# Create log directory
mkdir -p ~/knowledgegraph/logs

# Create the cron job script
cat > ~/knowledgegraph/run_daily_sync.sh << 'EOL'
#!/bin/bash
cd ~/knowledgegraph
source venv/bin/activate
python3 sync_daily_updates.py >> logs/daily_sync_$(date +\%Y\%m\%d).log 2>&1
EOL

# Make the script executable
chmod +x ~/knowledgegraph/run_daily_sync.sh

# Add cron job to run at 2 AM daily
(crontab -l 2>/dev/null; echo "0 2 * * * ~/knowledgegraph/run_daily_sync.sh") | crontab -

echo "Cron job has been set up to run daily at 2 AM"
echo "Logs will be stored in ~/knowledgegraph/logs/" 