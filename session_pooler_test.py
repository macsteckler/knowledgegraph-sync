import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()

# Get raw password and URL encode it
raw_password = os.getenv('password')
encoded_password = quote_plus(raw_password) if raw_password else None

# Try both pooler configurations
configs = [
    {
        'name': 'Transaction Pooler',
        'params': {
            'user': os.getenv('user'),
            'password': raw_password,  # Try raw password first
            'host': os.getenv('host'),
            'port': '6543',
            'dbname': os.getenv('dbname')
        }
    },
    {
        'name': 'Transaction Pooler (URL encoded)',
        'params': {
            'user': os.getenv('user'),
            'password': encoded_password,  # Try URL encoded password
            'host': os.getenv('host'),
            'port': '6543',
            'dbname': os.getenv('dbname')
        }
    },
    {
        'name': 'Session Pooler',
        'params': {
            'user': os.getenv('user'),
            'password': raw_password,  # Try raw password
            'host': os.getenv('host'),
            'port': '5432',
            'dbname': os.getenv('dbname')
        }
    }
]

# Try each configuration
for config in configs:
    print(f"\n--- Trying {config['name']} ---")
    print("Connection Parameters:")
    for key, value in config['params'].items():
        if key != 'password':
            print(f"{key}: {value}")
    print("password: ********")

    try:
        print(f"\nAttempting to connect via {config['name']}...")
        conn = psycopg2.connect(**config['params'])
        print("✓ Connection successful!")
        
        # Test query
        with conn.cursor() as cur:
            cur.execute("SELECT current_timestamp;")
            result = cur.fetchone()
            print(f"Current timestamp: {result[0]}")
        
        conn.close()
        print("Connection closed.")
        print(f"\n✓ {config['name']} connection worked!")
        break  # Exit after first successful connection
        
    except Exception as e:
        print(f"\n✗ Connection failed!")
        print(f"Error: {str(e)}")
        continue  # Try next configuration

else:  # This runs if no connection was successful
    print("\nAll connection attempts failed. Troubleshooting suggestions:")
    print("1. Verify these credentials match exactly with Supabase dashboard")
    print("2. Check if you're on IPv4 network (Pooler is designed for IPv4)")
    print("3. Ensure ports 5432 and 6543 are not blocked by firewall")
    print("4. Verify the project is active in Supabase")
    print("5. Try putting the password in quotes in the .env file") 