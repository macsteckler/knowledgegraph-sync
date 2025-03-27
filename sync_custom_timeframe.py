import os
import psycopg2
from neo4j import GraphDatabase
from dotenv import load_dotenv
import sys
from decimal import Decimal
from datetime import datetime, timedelta
import argparse

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

def sync_news_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date):
    """Sync news articles from a specific time frame"""
    with pg_conn.cursor() as cur:
        print(f"\nSyncing news_articles from {start_date} to {end_date}...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM news_articles 
            WHERE date >= %s AND date <= %s
            """, (start_date, end_date))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 50
        processed = 0
        last_id = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM news_articles 
                WHERE date >= %s AND date <= %s AND id > %s
                ORDER BY id 
                LIMIT %s
                """, (start_date, end_date, last_id, batch_size))
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    last_id = data['id']
                    session.execute_write(upsert_news_article, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing news_articles")

def sync_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date):
    """Sync articles from a specific time frame"""
    with pg_conn.cursor() as cur:
        print(f"\nSyncing articles from {start_date} to {end_date}...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM articles 
            WHERE processed_at >= %s AND processed_at <= %s
            """, (start_date, end_date))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 50
        processed = 0
        last_id = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM articles 
                WHERE processed_at >= %s AND processed_at <= %s AND id > %s
                ORDER BY id 
                LIMIT %s
                """, (start_date, end_date, last_id, batch_size))
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    last_id = data['id']
                    session.execute_write(upsert_articles_table, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing articles")

def sync_council_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date):
    """Sync council meeting articles from a specific time frame"""
    with pg_conn.cursor() as cur:
        print(f"\nSyncing council_meeting_articles from {start_date} to {end_date}...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM council_meeting_articles 
            WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 50
        processed = 0
        last_id = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM council_meeting_articles 
                WHERE created_at >= %s AND created_at <= %s AND id > %s
                ORDER BY id 
                LIMIT %s
                """, (start_date, end_date, last_id, batch_size))
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    last_id = data['id']
                    session.execute_write(upsert_council_article, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing council_meeting_articles")

def sync_council_insights_timeframe(pg_conn, neo4j_driver, start_date, end_date):
    """Sync council meeting insights from a specific time frame"""
    with pg_conn.cursor() as cur:
        print(f"\nSyncing council_meeting_insights from {start_date} to {end_date}...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM council_meeting_insights 
            WHERE created_at >= %s AND created_at <= %s
            """, (start_date, end_date))
        total_rows = cur.fetchone()[0]
        print(f"Found {total_rows} records to sync")
        
        if total_rows == 0:
            return
        
        # Process in batches
        batch_size = 50
        processed = 0
        last_id = 0
        
        while processed < total_rows:
            cur.execute("""
                SELECT * 
                FROM council_meeting_insights 
                WHERE created_at >= %s AND created_at <= %s AND id > %s
                ORDER BY id 
                LIMIT %s
                """, (start_date, end_date, last_id, batch_size))
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    last_id = data['id']
                    session.execute_write(upsert_council_insight, data)
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed}/{total_rows} records processed ({(processed/total_rows*100):.1f}%)")
            
            print(f"Completed batch: {processed}/{total_rows} records")
    
    print("✓ Completed syncing council_meeting_insights")

def parse_args():
    parser = argparse.ArgumentParser(description='Sync data within a specific time frame')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=datetime.now().strftime('%Y-%m-%d'), 
                        help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--table', choices=['all', 'news_articles', 'articles', 
                                           'council_articles', 'council_insights'],
                       default='all', help='Which table to sync (default: all)')
    
    return parser.parse_args()

def main():
    try:
        # Parse command-line arguments
        args = parse_args()
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        end_date = end_date.replace(hour=23, minute=59, second=59)  # Set to end of day
        
        print(f"Syncing data from {start_date} to {end_date}")
        print(f"Table selection: {args.table}")
        
        # Get connections
        pg_conn = get_pg_connection()
        neo4j_driver = get_neo4j_driver()

        # Sync selected tables for the time frame
        if args.table in ['all', 'news_articles']:
            sync_news_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date)
            
        if args.table in ['all', 'articles']:
            sync_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date)
            
        if args.table in ['all', 'council_articles']:
            sync_council_articles_timeframe(pg_conn, neo4j_driver, start_date, end_date)
            
        if args.table in ['all', 'council_insights']:
            sync_council_insights_timeframe(pg_conn, neo4j_driver, start_date, end_date)

        print("\nTimeframe sync completed successfully! 🎉")

    except Exception as e:
        print(f"Error during sync: {str(e)}")
        raise
    finally:
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'neo4j_driver' in locals():
            neo4j_driver.close()

# Import the upsert functions from the main sync script
from sync_to_neo4j import (
    upsert_news_article,
    upsert_articles_table,
    upsert_council_article,
    upsert_council_insight
)

if __name__ == "__main__":
    main() 