import os
import psycopg2
from neo4j import GraphDatabase
from dotenv import load_dotenv
import sys
from decimal import Decimal

# 1) Load environment variables
load_dotenv()

# Postgres connection parameters
DATABASE_URL = "postgresql://postgres:8P9WzFGiVQEjencW@db.jcotpnywnrywfthrzvdk.supabase.co:5432/postgres"

print(f"Postgres connection URL (password hidden): postgresql://postgres:****@db.jcotpnywnrywfthrzvdk.supabase.co:5432/postgres")

# Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASS = os.getenv("NEO4J_PASSWORD")

print("\nNeo4j connection parameters:")
print(f"URI: {NEO4J_URI}")
print(f"Username: {NEO4J_USER}")
print("Password: ****")

# 2) Create Postgres & Neo4j connections
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
        # Verify connection works
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        print("Neo4j connection successful!")
        return driver
    except Exception as e:
        print(f"Error connecting to Neo4j: {str(e)}")
        raise

###################################################
#                   MERGE FUNCTIONS                 #
###################################################

def upsert_news_article(tx, row):
    """
    Ingest data from news_articles table => Create or update (:Article).
    """
    # Basic MERGE for the Article node
    tx.run(
        """
        MERGE (a:Article {id: $id})
        ON CREATE SET
            a.title = $title,
            a.summary = $summary,
            a.content = $full_content,
            a.publishDate = $date_posted,
            a.url = $url,
            a.sentimentScore = $sentiment
        ON MATCH SET
            a.summary = $summary,    // update fields if changed
            a.content = $full_content,
            a.sentimentScore = $sentiment
        """,
        id=row['id'],
        title=row['title'],
        summary=row['summary'],
        full_content=row['full_content'],
        date_posted=row['date_posted'],
        url=row['url'],
        sentiment=row['sentiment']
    )

    # City and State relationships
    if row.get('city_seo') and row.get('state_seo'):
        # Create City node with state property and link to Article
        tx.run(
            """
            MERGE (c:City {name: $city, state: $state})
            WITH c
            MERGE (a:Article {id: $article_id})
            MERGE (a)-[:PUBLISHED_IN]->(c)
            """,
            city=row['city_seo'],
            state=row['state_seo'],
            article_id=row['id']
        )
        
        # Create State node and relationship
        tx.run(
            """
            MERGE (s:State {name: $state})
            WITH s
            MERGE (c:City {name: $city, state: $state})
            MERGE (c)-[:LOCATED_IN]->(s)
            """,
            state=row['state_seo'],
            city=row['city_seo']
        )

    # Topics with enhanced relationships
    topics = []
    for topic_field in ['topic_1', 'topic_2', 'topic_3', 'main_topic']:
        if row.get(topic_field):
            topics.append(row[topic_field])
    
    if row.get('topic_keywords'):
        if isinstance(row['topic_keywords'], list):
            topics.extend(row['topic_keywords'])
        elif isinstance(row['topic_keywords'], str):
            topics.extend(row['topic_keywords'].split(','))

    # Process each topic and create all necessary relationships
    for topic in topics:
        if topic and row.get('city_seo') and row.get('state_seo'):
            tx.run(
                """
                // Create or match the Issue node
                MERGE (i:Issue {name: $topicName})
                WITH i
                
                // Match the Article and create HAS_TOPIC relationship
                MERGE (a:Article {id: $article_id})
                MERGE (a)-[ht:HAS_TOPIC]->(i)
                ON CREATE SET
                    ht.publishDate = $date_posted,
                    ht.createdAt = datetime()
                ON MATCH SET
                    ht.updatedAt = datetime()
                
                WITH i, $date_posted as date_posted
                
                // Match the City and create HAS_ISSUE relationship
                MERGE (c:City {name: $city, state: $state})
                MERGE (c)-[hi:HAS_ISSUE]->(i)
                ON CREATE SET
                    hi.firstMentioned = date_posted,
                    hi.mentionCount = 1,
                    hi.createdAt = datetime()
                ON MATCH SET
                    hi.lastMentioned = date_posted,
                    hi.mentionCount = hi.mentionCount + 1,
                    hi.updatedAt = datetime()
                """,
                topicName=topic.strip(),
                article_id=row['id'],
                city=row['city_seo'],
                state=row['state_seo'],
                date_posted=row['date_posted']
            )

    # Entities (entity_person)
    if row.get('entity_person'):
        persons = row['entity_person']
        if isinstance(persons, str):
            persons = [p.strip() for p in persons.split(',')]
        elif not isinstance(persons, list):
            persons = [str(persons)]

        for person_name in persons:
            if person_name:
                tx.run(
                    """
                    MERGE (p:Person {name: $personName})
                    WITH p
                    MERGE (a:Article {id: $articleId})
                    MERGE (a)-[:MENTIONS_PERSON]->(p)
                    """,
                    personName=person_name.strip(),
                    articleId=row['id']
                )

def upsert_articles_table(tx, row):
    """
    Ingest data from articles table => augment the same (:Article) node.
    """
    if row.get('news_article_id') is None:
        return

    tx.run(
        """
        MERGE (a:Article {id: $na_id})
        ON MATCH SET
            a.engagementScore = $engagement_score,
            a.viewCount = $view_count,
            a.shareCount = $share_count,
            a.commentCount = $comment_count,
            a.readingTime = $reading_time_minutes,
            a.complexityScore = $complexity_score,
            a.factDensity = $fact_density,
            a.isOpinion = $is_opinion,
            a.sentimentCategory = $sentiment_category
        """,
        na_id=row['news_article_id'],
        engagement_score=row.get('engagement_score'),
        view_count=row.get('view_count'),
        share_count=row.get('share_count'),
        comment_count=row.get('comment_count'),
        reading_time_minutes=row.get('reading_time_minutes'),
        complexity_score=row.get('complexity_score'),
        fact_density=row.get('fact_density'),
        is_opinion=row.get('is_opinion'),
        sentiment_category=row.get('sentiment_category')
    )

def convert_decimal(value):
    """Convert Decimal types to float for Neo4j compatibility."""
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, dict):
        return {k: convert_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_decimal(v) for v in value]
    return value

def upsert_council_article(tx, row):
    """
    Ingest data from council_meeting_articles.
    """
    # Convert any Decimal values to float
    data = convert_decimal(row)
    
    tx.run(
        """
        MERGE (a:Article {id: $id})
        ON CREATE SET
            a.title = $article_title,
            a.content = $article_content,
            a.summary = $summary,
            a.sourceType = "CouncilMeeting",
            a.importanceScore = $importance_score
        """,
        id=str(data['id']),
        article_title=data.get('article_title'),
        article_content=data.get('article_content'),
        summary=data.get('summary'),
        importance_score=data.get('importance_score')
    )

    # Link to Meeting
    if data.get('video_id'):
        tx.run(
            """
            MERGE (m:Meeting {id: $video_id})
            MERGE (a:Article {id: $article_id})
            MERGE (m)-[:HAS_ARTICLE]->(a)
            """,
            video_id=str(data['video_id']),
            article_id=str(data['id'])
        )

    # Related insights
    if data.get('related_insight_ids'):
        insights = data['related_insight_ids']
        if isinstance(insights, str):
            insights = [i.strip() for i in insights.split(',')]
        elif not isinstance(insights, list):
            insights = [str(insights)]

        for insight_id in insights:
            tx.run(
                """
                MERGE (i:Insight {id: $insight_id})
                MERGE (a:Article {id: $article_id})
                MERGE (a)-[:HAS_INSIGHT]->(i)
                """,
                insight_id=str(insight_id),
                article_id=str(data['id'])
            )

    # Topic tags
    if data.get('topic_tags'):
        topics = data['topic_tags']
        if isinstance(topics, str):
            topics = [t.strip() for t in topics.split(',')]
        elif not isinstance(topics, list):
            topics = [str(topics)]

        for topic in topics:
            tx.run(
                """
                MERGE (t:Issue {name: $topic})
                MERGE (a:Article {id: $article_id})
                MERGE (a)-[:HAS_TOPIC]->(t)
                """,
                topic=topic,
                article_id=str(data['id'])
            )

def upsert_council_insight(tx, row):
    """
    Ingest data from council_meeting_insights.
    """
    # Convert any Decimal values to float
    data = convert_decimal(row)
    
    # Create the Insight node with additional properties
    tx.run(
        """
        MERGE (i:Insight {id: $id})
        ON CREATE SET
            i.category = $category,
            i.title = $insight_title,
            i.description = $insight_description,
            i.voteResult = $vote_result,
            i.nextSteps = $next_steps,
            i.sentiment = $sentiment,
            i.importance = $importance,
            i.startTime = $start_time,
            i.endTime = $end_time,
            i.timestamp = $timestamp,
            i.city = $city,
            i.createdAt = $created_at
        ON MATCH SET
            i.category = $category,
            i.title = $insight_title,
            i.description = $insight_description,
            i.voteResult = $vote_result,
            i.nextSteps = $next_steps,
            i.sentiment = $sentiment,
            i.importance = $importance,
            i.city = $city
        """,
        id=str(data['id']),
        category=data.get('category'),
        insight_title=data.get('insight_title'),
        insight_description=data.get('insight_description'),
        vote_result=data.get('vote_result'),
        next_steps=data.get('next_steps'),
        sentiment=data.get('sentiment'),
        importance=data.get('importance'),
        start_time=data.get('start_time'),
        end_time=data.get('end_time'),
        timestamp=data.get('timestamp'),
        city=data.get('city'),
        created_at=data.get('created_at')
    )

    # Link to Meeting (Council Video)
    if data.get('video_id'):
        # First create the relationship without any properties
        tx.run(
            """
            MERGE (m:Meeting {id: $video_id})
            ON CREATE SET
                m.sourceType = "CouncilMeeting",
                m.city = $city
            MERGE (i:Insight {id: $insight_id})
            MERGE (m)-[:HAS_INSIGHT]->(i)
            """,
            video_id=str(data['video_id']),
            insight_id=str(data['id']),
            city=data.get('city')
        )
        
        # Then set properties on the relationship if they exist
        if data.get('start_time') or data.get('end_time') or data.get('timestamp'):
            props = {}
            if data.get('start_time') is not None:
                props['startTime'] = data.get('start_time')
            if data.get('end_time') is not None:
                props['endTime'] = data.get('end_time')
            if data.get('timestamp') is not None:
                props['timestamp'] = data.get('timestamp')
                
            # Only set properties if we have some
            if props:
                props_string = ", ".join(f"r.{key} = ${key}" for key in props.keys())
                query = f"""
                MATCH (m:Meeting {{id: $video_id}})-[r:HAS_INSIGHT]->(i:Insight {{id: $insight_id}})
                SET {props_string}
                """
                tx.run(query, video_id=str(data['video_id']), insight_id=str(data['id']), **props)
        
        # Link city to the insight
        tx.run(
            """
            MERGE (c:City {name: $city})
            MERGE (i:Insight {id: $insight_id})
            MERGE (i)-[:ABOUT_CITY]->(c)
            """,
            city=data.get('city'),
            insight_id=str(data['id'])
        )

    # Process Quotes - convert from JSONB to Python objects
    if data.get('quotes') and data['quotes']:
        quotes = data['quotes']
        if isinstance(quotes, str):
            try:
                import json
                quotes = json.loads(quotes)
            except:
                quotes = []
        
        # If it's a list of quotes
        if isinstance(quotes, list):
            for idx, quote in enumerate(quotes):
                # Handle different quote formats
                if isinstance(quote, dict):
                    quote_text = quote.get('quote') or quote.get('text')
                    speaker = quote.get('speaker')
                else:
                    quote_text = str(quote) if quote else None
                    speaker = None
                
                # Skip quotes with null text
                if not quote_text:
                    continue
                    
                # Create quote node
                tx.run(
                    """
                    MERGE (q:Quote {text: $text, insightId: $insight_id, index: $idx})
                    MERGE (i:Insight {id: $insight_id})
                    MERGE (i)-[:HAS_QUOTE]->(q)
                    """,
                    text=quote_text,
                    insight_id=str(data['id']),
                    idx=idx
                )
                
                # If speaker is identified, create person node
                if speaker:
                    tx.run(
                        """
                        MERGE (p:Person {name: $speaker})
                        MERGE (q:Quote {text: $text, insightId: $insight_id, index: $idx})
                        MERGE (p)-[:STATED]->(q)
                        """,
                        speaker=speaker,
                        text=quote_text,
                        insight_id=str(data['id']),
                        idx=idx
                    )
    
    # Process Entities - convert from JSONB to Python objects
    if data.get('entities') and data['entities']:
        entities = data['entities']
        if isinstance(entities, str):
            try:
                import json
                entities = json.loads(entities)
            except:
                entities = {}
        
        # Process entities by type (assuming structure like {organizations: [], persons: [], etc.})
        for entity_type, entity_list in entities.items():
            if isinstance(entity_list, list):
                for entity_name in entity_list:
                    # Create appropriate node type based on entity_type
                    if entity_type.lower() in ['person', 'persons', 'people']:
                        tx.run(
                            """
                            MERGE (p:Person {name: $name})
                            MERGE (i:Insight {id: $insight_id})
                            MERGE (i)-[:MENTIONS_ENTITY {type: 'person'}]->(p)
                            """,
                            name=entity_name,
                            insight_id=str(data['id'])
                        )
                    elif entity_type.lower() in ['organization', 'organizations', 'org', 'orgs']:
                        tx.run(
                            """
                            MERGE (o:Organization {name: $name})
                            MERGE (i:Insight {id: $insight_id})
                            MERGE (i)-[:MENTIONS_ENTITY {type: 'organization'}]->(o)
                            """,
                            name=entity_name,
                            insight_id=str(data['id'])
                        )
                    elif entity_type.lower() in ['location', 'locations', 'place', 'places']:
                        tx.run(
                            """
                            MERGE (l:Location {name: $name})
                            MERGE (i:Insight {id: $insight_id})
                            MERGE (i)-[:MENTIONS_ENTITY {type: 'location'}]->(l)
                            """,
                            name=entity_name,
                            insight_id=str(data['id'])
                        )
                    else:
                        # Generic entity
                        tx.run(
                            """
                            MERGE (e:Entity {name: $name, type: $type})
                            MERGE (i:Insight {id: $insight_id})
                            MERGE (i)-[:MENTIONS_ENTITY {type: $type}]->(e)
                            """,
                            name=entity_name,
                            type=entity_type.lower(),
                            insight_id=str(data['id'])
                        )

    # Key figures
    if data.get('key_figures'):
        figures = data['key_figures']
        if isinstance(figures, str):
            figures = [f.strip() for f in figures.split(',')]
        elif not isinstance(figures, list):
            figures = [str(figures)]

        for figure in figures:
            tx.run(
                """
                MERGE (p:Person {name: $figure})
                MERGE (i:Insight {id: $insight_id})
                MERGE (i)-[:MENTIONS_FIGURE]->(p)
                """,
                figure=figure,
                insight_id=str(data['id'])
            )

    # Related topics
    if data.get('related_topics'):
        topics = data['related_topics']
        if isinstance(topics, str):
            topics = [t.strip() for t in topics.split(',')]
        elif not isinstance(topics, list):
            topics = [str(topics)]

        for topic in topics:
            tx.run(
                """
                MERGE (t:Issue {name: $topic})
                MERGE (i:Insight {id: $insight_id})
                MERGE (i)-[:CONCERNS_TOPIC]->(t)
                WITH t, i
                MERGE (c:City {name: $city})
                MERGE (c)-[:HAS_ISSUE]->(t)
                """,
                topic=topic,
                insight_id=str(data['id']),
                city=data.get('city')
            )
                


def upsert_council_video(tx, row):
    """
    Ingest data from council_meeting_videos.
    """
    # Convert any Decimal values to float
    data = convert_decimal(row)
    
    # Create the Meeting node
    tx.run(
        """
        MERGE (m:Meeting {id: $id})
        ON CREATE SET
            m.youtubeUrl = $youtube_url,
            m.title = $meeting_title,
            m.meetingDate = $meeting_date,
            m.duration = $video_duration,
            m.sourceType = "CouncilMeeting",
            m.city = $city,
            m.youtubeVideoId = $youtube_video_id,
            m.createdAt = $created_at
        ON MATCH SET
            m.youtubeUrl = $youtube_url,
            m.title = $meeting_title,
            m.meetingDate = $meeting_date,
            m.duration = $video_duration,
            m.city = $city,
            m.youtubeVideoId = $youtube_video_id
        """,
        id=str(data['id']),
        youtube_url=data.get('youtube_url'),
        meeting_title=data.get('meeting_title'),
        meeting_date=data.get('meeting_date'),
        video_duration=data.get('video_duration'),
        city=data.get('city'),
        youtube_video_id=data.get('youtube_video_id'),
        created_at=data.get('created_at')
    )
    
    # Link city to the meeting
    tx.run(
        """
        MERGE (c:City {name: $city})
        MERGE (m:Meeting {id: $meeting_id})
        MERGE (c)-[:HAS_MEETING]->(m)
        """,
        city=data.get('city'),
        meeting_id=str(data['id'])
    )


def sync_council_meeting_videos(pg_conn, neo4j_driver):
    """Fetch from council_meeting_videos."""
    with pg_conn.cursor() as cur:
        print("\nSyncing council_meeting_videos table...")
        cur.execute("SELECT COUNT(*) FROM council_meeting_videos;")
        total_rows = cur.fetchone()[0]
        
        # Get last processed ID - for UUIDs we start with empty string
        last_id = ""
        checkpoint_file = 'council_videos_checkpoint.txt'
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    last_id = f.read().strip()
                print(f"Resuming from UUID: {last_id}")
            except:
                last_id = ""
            
        print(f"Found {total_rows} total records")
        
        # Process in smaller batches
        batch_size = 50  # Reduced from 1000 to 50
        processed = 0
        
        while True:
            cur.execute(
                """
                SELECT * FROM council_meeting_videos 
                WHERE id::text > %s 
                ORDER BY id::text 
                LIMIT %s;
                """,
                (last_id, batch_size)
            )
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            # Process records in Neo4j batch
            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_video, data)
                    last_id = str(data['id'])  # Convert UUID to string
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed} records processed. Current UUID: {last_id}")
                        # Save UUID checkpoint
                        with open(checkpoint_file, 'w') as f:
                            f.write(str(last_id))
            
            # Show batch completion and save checkpoint
            print(f"Completed batch. Last UUID: {last_id}")
            # Save UUID checkpoint
            with open(checkpoint_file, 'w') as f:
                f.write(str(last_id))
    
    print("✓ Completed syncing council_meeting_videos")

###################################################
#               MAIN SYNC FUNCTIONS                #
###################################################

def get_last_processed_id(table_name):
    """Get the last processed ID from a checkpoint file."""
    try:
        with open(f'{table_name}_checkpoint.txt', 'r') as f:
            return int(f.read().strip())
    except:
        return 0

def save_checkpoint(table_name, last_id):
    """Save the last processed ID to a checkpoint file."""
    with open(f'{table_name}_checkpoint.txt', 'w') as f:
        f.write(str(last_id))

def sync_news_articles(pg_conn, neo4j_driver):
    """Fetch rows from news_articles, upsert into Neo4j."""
    with pg_conn.cursor() as cur:
        print("\nSyncing news_articles table...")
        cur.execute("SELECT COUNT(*) FROM news_articles;")
        total_rows = cur.fetchone()[0]
        
        # Get last processed ID
        last_id = get_last_processed_id('news_articles')
        if last_id > 0:
            print(f"Resuming from ID: {last_id}")
        
        print(f"Found {total_rows} total records")
        
        # Process in smaller batches
        batch_size = 50  # Reduced from 1000 to 50
        processed = 0
        
        while True:
            cur.execute(
                """
                SELECT * FROM news_articles 
                WHERE id > %s 
                ORDER BY id 
                LIMIT %s;
                """,
                (last_id, batch_size)
            )
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            # Process records in Neo4j batch
            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_news_article, data)
                    last_id = data['id']
                    processed += 1
                    if processed % 10 == 0:  # Show progress more frequently
                        print(f"Progress: {processed} records processed. Current ID: {last_id}")
                        save_checkpoint('news_articles', last_id)
            
            # Show batch completion and save checkpoint
            print(f"Completed batch. Last ID: {last_id}")
            save_checkpoint('news_articles', last_id)
    
    print("✓ Completed syncing news_articles")

def sync_articles_table(pg_conn, neo4j_driver):
    """Fetch rows from articles, augment existing Article nodes."""
    with pg_conn.cursor() as cur:
        print("\nSyncing articles table...")
        cur.execute("SELECT COUNT(*) FROM articles;")
        total_rows = cur.fetchone()[0]
        
        # Get last processed ID
        last_id = get_last_processed_id('articles')
        if last_id > 0:
            print(f"Resuming from ID: {last_id}")
            
        print(f"Found {total_rows} total records")
        
        # Process in smaller batches
        batch_size = 50  # Reduced from 1000 to 50
        processed = 0
        
        while True:
            cur.execute(
                """
                SELECT * FROM articles 
                WHERE id > %s 
                ORDER BY id 
                LIMIT %s;
                """,
                (last_id, batch_size)
            )
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            # Process records in Neo4j batch
            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_articles_table, data)
                    last_id = data['id']
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed} records processed. Current ID: {last_id}")
                        save_checkpoint('articles', last_id)
            
            # Show batch completion and save checkpoint
            print(f"Completed batch. Last ID: {last_id}")
            save_checkpoint('articles', last_id)
    
    print("✓ Completed syncing articles")

def sync_council_meeting_articles(pg_conn, neo4j_driver):
    """Fetch from council_meeting_articles."""
    with pg_conn.cursor() as cur:
        print("\nSyncing council_meeting_articles table...")
        cur.execute("SELECT COUNT(*) FROM council_meeting_articles;")
        total_rows = cur.fetchone()[0]
        
        # Get last processed ID - for UUIDs we start with empty string
        last_id = ""
        checkpoint_file = 'council_articles_checkpoint.txt'
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    last_id = f.read().strip()
                print(f"Resuming from UUID: {last_id}")
            except:
                last_id = ""
            
        print(f"Found {total_rows} total records")
        
        # Process in smaller batches
        batch_size = 50  # Reduced from 1000 to 50
        processed = 0
        
        while True:
            cur.execute(
                """
                SELECT * FROM council_meeting_articles 
                WHERE id::text > %s 
                ORDER BY id::text 
                LIMIT %s;
                """,
                (last_id, batch_size)
            )
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            # Process records in Neo4j batch
            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_article, data)
                    last_id = str(data['id'])  # Convert UUID to string
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed} records processed. Current UUID: {last_id}")
                        # Save UUID checkpoint
                        with open(checkpoint_file, 'w') as f:
                            f.write(str(last_id))
            
            # Show batch completion and save checkpoint
            print(f"Completed batch. Last UUID: {last_id}")
            # Save UUID checkpoint
            with open(checkpoint_file, 'w') as f:
                f.write(str(last_id))
    
    print("✓ Completed syncing council_meeting_articles")

def sync_council_meeting_insights(pg_conn, neo4j_driver):
    """Fetch from council_meeting_insights."""
    with pg_conn.cursor() as cur:
        print("\nSyncing council_meeting_insights table...")
        cur.execute("SELECT COUNT(*) FROM council_meeting_insights;")
        total_rows = cur.fetchone()[0]
        
        # Get last processed ID - for UUIDs we start with empty string
        last_id = ""
        checkpoint_file = 'council_insights_checkpoint.txt'
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    last_id = f.read().strip()
                print(f"Resuming from UUID: {last_id}")
            except:
                last_id = ""
            
        print(f"Found {total_rows} total records")
        
        # Process in smaller batches
        batch_size = 50  # Reduced from 1000 to 50
        processed = 0
        
        while True:
            cur.execute(
                """
                SELECT * FROM council_meeting_insights 
                WHERE id::text > %s 
                ORDER BY id::text 
                LIMIT %s;
                """,
                (last_id, batch_size)
            )
            rows = cur.fetchall()
            if not rows:
                break
                
            colnames = [desc[0] for desc in cur.description]

            # Process records in Neo4j batch
            with neo4j_driver.session() as session:
                for row in rows:
                    data = dict(zip(colnames, row))
                    session.execute_write(upsert_council_insight, data)
                    last_id = str(data['id'])  # Convert UUID to string
                    processed += 1
                    if processed % 10 == 0:
                        print(f"Progress: {processed} records processed. Current UUID: {last_id}")
                        # Save UUID checkpoint
                        with open(checkpoint_file, 'w') as f:
                            f.write(str(last_id))
            
            # Show batch completion and save checkpoint
            print(f"Completed batch. Last UUID: {last_id}")
            # Save UUID checkpoint
            with open(checkpoint_file, 'w') as f:
                f.write(str(last_id))
    
    print("✓ Completed syncing council_meeting_insights")

def main():
    try:
        # Get connections
        pg_conn = get_pg_connection()
        neo4j_driver = get_neo4j_driver()

        # Ask which tables to sync
        print("\nWhich tables would you like to sync?")
        print("1. news_articles")
        print("2. articles")
        print("3. council_meeting_articles")
        print("4. council_meeting_insights")
        print("5. council_meeting_videos")
        print("Enter numbers separated by commas (e.g., '3,4' to only sync council tables)")
        print("Or press Enter to sync all tables")
        
        choice = input("Tables to sync: ").strip()
        
        if choice:
            table_numbers = [int(n.strip()) for n in choice.split(',')]
        else:
            table_numbers = [1, 2, 3, 4, 5]
            
        # Sync selected tables
        if 1 in table_numbers:
            sync_news_articles(pg_conn, neo4j_driver)
        if 2 in table_numbers:
            sync_articles_table(pg_conn, neo4j_driver)
        if 3 in table_numbers:
            sync_council_meeting_articles(pg_conn, neo4j_driver)
        if 4 in table_numbers:
            sync_council_meeting_insights(pg_conn, neo4j_driver)
        if 5 in table_numbers:
            sync_council_meeting_videos(pg_conn, neo4j_driver)

        print("\nSync completed successfully! 🎉")

    except Exception as e:
        print(f"Error during sync: {str(e)}")
        raise
    finally:
        # Clean up connections
        if 'pg_conn' in locals():
            pg_conn.close()
        if 'neo4j_driver' in locals():
            neo4j_driver.close()

if __name__ == "__main__":
    main() 