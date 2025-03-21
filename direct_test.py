import psycopg2

# Try different username format
try:
    print(f"Attempting to connect with modified credentials...")
    conn = psycopg2.connect(
        user="postgres.jcotpnywnrywfthrzvdk",
        password="JmP222209!!",  # Exact password without quotes
        host="aws-0-us-west-1.pooler.supabase.com",
        port="5432",
        dbname="postgres"
    )
    print("Connection successful!")
    
    # Test basic query
    cursor = conn.cursor()
    cursor.execute("SELECT NOW();")
    print(f"Current time: {cursor.fetchone()}")
    
    cursor.close()
    conn.close()
    print("Connection closed.")
except Exception as e:
    print(f"Failed to connect with Session Pooler: {e}")
    
    # Try with direct connection (but not likely to work due to IPv6 requirement)
    try:
        print("\nTrying direct connection as fallback...")
        conn = psycopg2.connect(
            user="postgres",
            password="JmP222209!!",
            host="db.jcotpnywnrywfthrzvdk.supabase.co",
            port="5432",
            dbname="postgres"
        )
        print("Direct connection successful!")
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        print(f"Current time: {cursor.fetchone()}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to connect with direct connection: {e}") 