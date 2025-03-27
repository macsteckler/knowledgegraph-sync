import os
import psycopg2
from neo4j import GraphDatabase
from dotenv import load_dotenv
import sys
from decimal import Decimal
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Postgres connection parameters
DATABASE_URL = f"postgresql://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}"

# Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")

def get_pg_connection():
    print("\nAttempting to connect to PostgreSQL database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("PostgreSQL connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        raise

def get_neo4j_driver():
    print("\nAttempting to connect to Neo4j database...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        print("Neo4j connection successful!")
        return driver
    except Exception as e:
        print(f"Error connecting to Neo4j: {str(e)}")
        raise

def sync_recent_news_articles(pg_conn, neo4j_driver, hours=24):
    """Sync news articles from the last X hours"""
    with pg_conn.cursor() as cur:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        print(f"\nSyncing news_articles from the last {hours} hours...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM news_articles 
            WHERE date >= %s
            """, (cutoff_time,))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} new records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 100
        processed = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM news_articles 
                WHERE date >= %s
                ORDER BY id 
                LIMIT %s OFFSET %s
                """, (cutoff_time, batch_size, processed))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_news_article, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing recent news_articles")

def sync_recent_articles(pg_conn, neo4j_driver, hours=24):
    """Sync articles from the last X hours"""
    with pg_conn.cursor() as cur:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        print(f"\nSyncing articles from the last {hours} hours...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM articles 
            WHERE processed_at >= %s
            """, (cutoff_time,))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} new records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 100
        processed = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM articles 
                WHERE processed_at >= %s
                ORDER BY id 
                LIMIT %s OFFSET %s
                """, (cutoff_time, batch_size, processed))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_articles_table, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing recent articles")

def sync_recent_council_articles(pg_conn, neo4j_driver, hours=24):
    """Sync council meeting articles from the last X hours"""
    with pg_conn.cursor() as cur:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        print(f"\nSyncing council_meeting_articles from the last {hours} hours...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM council_meeting_articles 
            WHERE created_at >= %s
            """, (cutoff_time,))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} new records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 100
        processed = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM council_meeting_articles 
                WHERE created_at >= %s
                ORDER BY id 
                LIMIT %s OFFSET %s
                """, (cutoff_time, batch_size, processed))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_article, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing recent council_meeting_articles")

def sync_recent_council_insights(pg_conn, neo4j_driver, hours=24):
    """Sync council meeting insights from the last X hours"""
    with pg_conn.cursor() as cur:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        print(f"\nSyncing council_meeting_insights from the last {hours} hours...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM council_meeting_insights 
            WHERE created_at >= %s
            """, (cutoff_time,))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} new records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 100
        processed = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM council_meeting_insights 
                WHERE created_at >= %s
                ORDER BY id 
                LIMIT %s OFFSET %s
                """, (cutoff_time, batch_size, processed))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_insight, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing recent council_meeting_insights")

def sync_recent_council_videos(pg_conn, neo4j_driver, hours=24):
    """Sync council meeting videos from the last X hours"""
    with pg_conn.cursor() as cur:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        print(f"\nSyncing council_meeting_videos from the last {hours} hours...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM council_meeting_videos 
            WHERE created_at >= %s
            """, (cutoff_time,))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} new records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 100
        processed = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM council_meeting_videos 
                WHERE created_at >= %s
                ORDER BY id 
                LIMIT %s OFFSET %s
                """, (cutoff_time, batch_size, processed))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_video, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing recent council_meeting_videos")

def main():
    try:
        # Get connections
        pg_conn = get_pg_connection()
        neo4j_driver = get_neo4j_driver()

        # Sync all tables for the last 24 hours
        sync_recent_news_articles(pg_conn, neo4j_driver)
        sync_recent_articles(pg_conn, neo4j_driver)
        sync_recent_council_articles(pg_conn, neo4j_driver)
        sync_recent_council_insights(pg_conn, neo4j_driver)
        sync_recent_council_videos(pg_conn, neo4j_driver)

        print("\nDaily sync completed successfully! 🎉")

    except Exception as e:
        print(f"Error during sync: {str(e)}")
        raise
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'neo4j_driver' in locals():
            neo4j_driver.close()

# Import the upsert functions
from sync_to_neo4j import (
    upsert_news_article,
    upsert_articles_table,
    upsert_council_article,
    upsert_council_insight,
    upsert_council_video
)

if __name__ == "__main__":
    main() 