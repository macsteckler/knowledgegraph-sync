import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get connection parameters
USER = os.getenv('user')
PASSWORD = os.getenv('password')
HOST = os.getenv('host')
PORT = os.getenv('port')
DBNAME = os.getenv('dbname')

# Print loaded variables (useful for debugging)
print("Environment variables loaded:")
print(f"User: {USER}")
print(f"Host: {HOST}")
print(f"Port: {PORT}")
print(f"Database: {DBNAME}")
print(f"Password length: {len(PASSWORD) if PASSWORD else 0}")

# Connect to the database
try:
    print("\nAttempting to connect with individual parameters...")
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # Test query - get list of tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        LIMIT 5;
    """)
    tables = cursor.fetchall()
    
    print("\nAvailable tables (showing up to 5):")
    for table in tables:
        print(f"- {table[0]}")

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("\nConnection closed.")

except Exception as e:
    print(f"\nFailed to connect: {e}") 