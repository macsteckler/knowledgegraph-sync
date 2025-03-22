import os
import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")

def generate_monthly_trends(driver, year_month_str=None):
    # If no year_month provided, use last month
    if year_month_str is None:
        today = datetime.date.today()
        first = today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)
        year_month_str = last_month.strftime("%Y-%m")

    # Derive date range from the input "YYYY-MM"
    start_year, start_month = map(int, year_month_str.split("-"))
    start_date = datetime.date(start_year, start_month, 1)

    # Calculate end date (first day of next month)
    if start_month == 12:
        end_date = datetime.date(start_year + 1, 1, 1)
    else:
        end_date = datetime.date(start_year, start_month + 1, 1)

    cypher = """
    MATCH (c:City)<-[:PUBLISHED_IN]-(a:Article)-[rel:HAS_TOPIC]->(i:Issue)
    WHERE rel.publishDate >= date($startDate)
      AND rel.publishDate < date($endDate)
    WITH c, i,
         count(a) AS mentionCount,
         avg(a.sentimentScore) AS avgSentiment
    MERGE (tt:TopicTrend {
      topicName: i.name,
      cityName: c.name,
      yearMonth: $ym
    })
    SET tt.mentionCount = mentionCount,
        tt.averageSentiment = avgSentiment,
        tt.lastUpdated = datetime()
    MERGE (c)-[:MONTHLY_TREND]->(tt)
    MERGE (tt)-[:TREND_OF]->(i)
    RETURN c.name AS city, i.name AS topic, 
           mentionCount, avgSentiment
    """

    print(f"Generating monthly trends for {year_month_str}")
    print(f"Date range: {start_date} to {end_date}")

    with driver.session() as session:
        result = session.run(cypher, {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "ym": year_month_str
        })
        
        trends = []
        for record in result:
            trend = {
                'city': record['city'],
                'topic': record['topic'],
                'count': record['mentionCount'],
                'sentiment': record['avgSentiment']
            }
            trends.append(trend)
            print(f"Updated monthly trend: City={trend['city']}, "
                  f"Topic={trend['topic']}, Count={trend['count']}, "
                  f"AvgSent={trend['sentiment']:.2f}")
        
        print(f"Updated {len(trends)} topic trends for {year_month_str}")
        return trends

def main():
    try:
        print("\nStarting monthly trend aggregation...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        # Generate trends for last month
        generate_monthly_trends(driver)
        
        driver.close()
        print("Monthly trend aggregation completed successfully!")
        
    except Exception as e:
        print(f"Error during monthly trend aggregation: {str(e)}")
        raise
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main() 