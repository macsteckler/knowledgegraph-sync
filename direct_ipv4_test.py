import psycopg2
import socket
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get IPv4 address for the host
def get_ipv4_addr(host):
    try:
        # Get all address info
        addrinfo = socket.getaddrinfo(host, None)
        # Filter for IPv4 addresses
        ipv4_addrs = [addr[4][0] for addr in addrinfo if addr[0] == socket.AF_INET]
        return ipv4_addrs[0] if ipv4_addrs else None
    except Exception as e:
        print(f"Error resolving host: {e}")
        return None

# Get the IPv4 address
host = os.getenv('host')
ipv4_addr = get_ipv4_addr(host)

if ipv4_addr:
    print(f"Resolved {host} to IPv4 address: {ipv4_addr}")
    
    # Try both pooler configurations
    configs = [
        {
            'name': 'Transaction Pooler',
            'port': '6543',
            'password': os.getenv('password').strip('"')  # Remove quotes if present
        },
        {
            'name': 'Session Pooler',
            'port': '5432',
            'password': os.getenv('password').strip('"')  # Remove quotes if present
        }
    ]

    for config in configs:
        print(f"\n--- Trying {config['name']} ---")
        
        # Build connection parameters
        params = {
            'user': os.getenv('user'),
            'password': config['password'],
            'host': ipv4_addr,  # Use IPv4 address instead of hostname
            'port': config['port'],
            'dbname': os.getenv('dbname')
        }

        print("Connection Parameters:")
        for key, value in params.items():
            if key != 'password':
                print(f"{key}: {value}")
        print("password: ********")

        try:
            print(f"\nAttempting to connect via {config['name']}...")
            conn = psycopg2.connect(**params)
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
        print("1. Check if your IP is allowlisted in Supabase")
        print("2. Verify the password in Supabase dashboard")
        print("3. Try connecting through a different network")
else:
    print(f"Could not resolve {host} to an IPv4 address") 