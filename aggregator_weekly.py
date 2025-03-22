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

def get_week_dates(year_week=None):
    if year_week is None:
        # Get last week's dates
        today = datetime.date.today()
        monday = today - datetime.timedelta(days=today.weekday())
        last_monday = monday - datetime.timedelta(weeks=1)
        year_week = last_monday.strftime("%Y-W%W")
    else:
        # Parse "YYYY-WXX" format
        year, week = year_week.split("-W")
        year = int(year)
        week = int(week)
        
        # Find the Monday of that week
        first_day = datetime.datetime.strptime(f"{year}-W{week:02d}-1", "%Y-W%W-%w").date()
        last_monday = first_day

    # Calculate the next Monday
    next_monday = last_monday + datetime.timedelta(weeks=1)
    
    return last_monday, next_monday, year_week

def generate_weekly_trends(driver, year_week=None):
    start_date, end_date, year_week = get_week_dates(year_week)
    
    cypher = """
    MATCH (c:City)<-[:PUBLISHED_IN]-(a:Article)-[rel:HAS_TOPIC]->(i:Issue)
    WHERE rel.publishDate >= date($startDate)
      AND rel.publishDate < date($endDate)
    WITH c, i,
         count(a) AS mentionCount,
         avg(a.sentimentScore) AS avgSentiment
    MERGE (tt:TopicTrendWeekly {
      topicName: i.name,
      cityName: c.name,
      yearWeek: $yw
    })
    SET tt.mentionCount = mentionCount,
        tt.averageSentiment = avgSentiment,
        tt.lastUpdated = datetime()
    MERGE (c)-[:WEEKLY_TREND]->(tt)
    MERGE (tt)-[:TREND_OF]->(i)
    RETURN c.name AS city, i.name AS topic, 
           mentionCount, avgSentiment
    """

    print(f"Generating weekly trends for {year_week}")
    print(f"Date range: {start_date} to {end_date}")

    with driver.session() as session:
        result = session.run(cypher, {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "yw": year_week
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
            print(f"Updated weekly trend: City={trend['city']}, "
                  f"Topic={trend['topic']}, Count={trend['count']}, "
                  f"AvgSent={trend['sentiment']:.2f}")
        
        print(f"Updated {len(trends)} topic trends for {year_week}")
        return trends

def main():
    try:
        print("\nStarting weekly trend aggregation...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        
        # Generate trends for last week
        generate_weekly_trends(driver)
        
        driver.close()
        print("Weekly trend aggregation completed successfully!")
        
    except Exception as e:
        print(f"Error during weekly trend aggregation: {str(e)}")
        raise
    finally:
        if 'driver' in locals():
            driver.close()

if __name__ == "__main__":
    main() 