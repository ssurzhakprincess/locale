import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

def create_tables():
    with engine.connect() as conn:
        
        # Venues table - Overture + Yelp joined data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS venues (
                id SERIAL PRIMARY KEY,
                overture_id TEXT UNIQUE,
                name TEXT NOT NULL,
                category TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                yelp_id TEXT,
                yelp_rating DOUBLE PRECISION,
                yelp_review_count INTEGER,
                match_confidence DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # Yelp reviews table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                yelp_id TEXT,
                venue_id INTEGER REFERENCES venues(id),
                review_text TEXT,
                star_rating INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # Venue sentiment scores table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS venue_sentiment (
                id SERIAL PRIMARY KEY,
                venue_id INTEGER REFERENCES venues(id),
                food_quality DOUBLE PRECISION,
                atmosphere DOUBLE PRECISION,
                value_for_money DOUBLE PRECISION,
                crowd_type DOUBLE PRECISION,
                service DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # User profiles table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                budget_range TEXT,
                priorities TEXT[],
                dietary_preferences TEXT[],
                household_type TEXT,
                free_text_preferences TEXT,
                preference_vector JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # User diary table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS diary (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                venue_id INTEGER REFERENCES venues(id),
                match_score DOUBLE PRECISION,
                explanation TEXT,
                photo_path TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        # Identification logs table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS identification_logs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                identified_name TEXT,
                identified_category TEXT,
                confidence_score DOUBLE PRECISION,
                fallback_used BOOLEAN,
                venue_id INTEGER REFERENCES venues(id),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))

        conn.commit()
        print("All tables created successfully.")

if __name__ == "__main__":
    create_tables()