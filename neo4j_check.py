from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")

def check_data(tx):
    # Check Article counts
    result = tx.run("""
    MATCH (a:Article)
    RETURN count(a) as article_count
    """)
    print("\nCurrent Article count:", result.single()['article_count'])

    # Check City nodes and their relationships
    result = tx.run("""
    MATCH (c:City)
    OPTIONAL MATCH (a:Article)-[:PUBLISHED_IN]->(c)
    RETURN c.name as city, c.state as state, COUNT(a) as article_count
    ORDER BY article_count DESC
    """)
    print("\nCurrent City nodes and their article counts:")
    for record in result:
        print(f"City: {record['city']}, State: {record['state']}, Articles: {record['article_count']}")

    # Check State nodes (if any exist)
    result = tx.run("""
    MATCH (s:State)
    OPTIONAL MATCH (c:City)-[:LOCATED_IN]->(s)
    RETURN s.name as state, COUNT(c) as city_count
    ORDER BY city_count DESC
    """)
    print("\nCurrent State nodes and their city counts:")
    for record in result:
        print(f"State: {record['state']}, Cities: {record['city_count']}")

    # Sample some articles to see their city relationships
    result = tx.run("""
    MATCH (a:Article)
    WHERE EXISTS((a)-[:PUBLISHED_IN]->())
    WITH a LIMIT 5
    MATCH (a)-[:PUBLISHED_IN]->(c:City)
    RETURN a.id as article_id, a.title as title, c.name as city, c.state as state
    """)
    print("\nSample Articles with City relationships:")
    for record in result:
        print(f"Article {record['article_id']}: {record['title']} -> {record['city']}, {record['state']}")

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        session.execute_write(check_data)
    driver.close()

if __name__ == "__main__":
    main() 