import psycopg2
import socket
from dotenv import load_dotenv
import os

# Force IPv4
def force_ipv4():
    # Create a socket that only supports IPv4
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Load environment variables
load_dotenv()

# Connection parameters for Transaction Pooler
params = {
    'user': os.getenv('user'),
    'password': os.getenv('password').strip('"'),  # Remove quotes if present
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'dbname': os.getenv('dbname')
}

print("Connection Parameters:")
for key, value in params.items():
    if key != 'password':
        print(f"{key}: {value}")
print("password: ********")

try:
    # Create connection with IPv4 socket
    conn = psycopg2.connect(
        **params,
        connect_timeout=10,
        tcp_user_timeout=10000,  # 10 seconds in milliseconds
        socket_factory=force_ipv4
    )
    
    print("\n✓ Connection successful!")
    
    # Test query
    with conn.cursor() as cur:
        cur.execute("SELECT current_timestamp;")
        result = cur.fetchone()
        print(f"Current timestamp: {result[0]}")
    
    conn.close()
    print("Connection closed.")
    
except Exception as e:
    print("\n✗ Connection failed!")
    print(f"Error: {str(e)}")
    print("\nTroubleshooting Info:")
    print("1. Check if the password contains any special characters")
    print("2. Verify the connection is not being blocked by a firewall")
    print("3. Try using the Session Pooler (port 5432) instead")
    print("4. Check if your IP is allowlisted in Supabase") 