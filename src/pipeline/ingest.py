import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

# ── 1. Load Overture ──────────────────────────────────────────────────────────

def load_overture(path="data/raw/overture/sb_places.geojson"):
    print("Loading Overture data...")
    records = []
    with open(path) as f:
        next(f)  # skip FeatureCollection header
        for line in f:
            line = line.strip().rstrip(",")
            if not line or line in ("{", "}", "]", "[", "]}"):
                continue
            try:
                feature = json.loads(line)
                props = feature.get("properties", {})
                coords = feature.get("geometry", {}).get("coordinates", [])
                if not coords or len(coords) < 2:
                    continue
                name = props.get("names", {})
                if isinstance(name, dict):
                    name = name.get("primary", "")
                elif not isinstance(name, str):
                    name = ""
                if not name:
                    continue
                records.append({
                    "overture_id": props.get("id"),
                    "name": name,
                    "category": props.get("categories", {}).get("primary"),
                    "longitude": coords[0],
                    "latitude": coords[1],
                })
            except Exception:
                continue
    df = pd.DataFrame(records)
    print(f"Loaded {len(df)} Overture venues")
    return df


# ── 2. Load Yelp businesses (Santa Barbara only) ──────────────────────────────

def load_yelp_businesses(path="data/raw/yelp/yelp_academic_dataset_business.json"):
    print("Loading Yelp businesses...")
    records = []
    with open(path) as f:
        for line in f:
            try:
                biz = json.loads(line)
                if biz.get("city") == "Santa Barbara" and biz.get("state") == "CA":
                    records.append({
                        "yelp_id": biz.get("business_id"),
                        "name": biz.get("name"),
                        "category": biz.get("categories"),
                        "latitude": biz.get("latitude"),
                        "longitude": biz.get("longitude"),
                        "yelp_rating": biz.get("stars"),
                        "yelp_review_count": biz.get("review_count"),
                        "city": biz.get("city"),
                        "is_open": biz.get("is_open"),
                    })
            except json.JSONDecodeError:
                continue
    df = pd.DataFrame(records)
    print(f"Loaded {len(df)} Yelp Santa Barbara businesses")
    return df


# ── 3. Save to PostgreSQL ─────────────────────────────────────────────────────

def save_overture(df):
    print("Saving Overture venues to database...")
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO venues (overture_id, name, category, latitude, longitude)
                VALUES (:overture_id, :name, :category, :latitude, :longitude)
                ON CONFLICT (overture_id) DO NOTHING
            """), row.to_dict())
        conn.commit()
    print(f"Saved {len(df)} Overture venues")


def save_yelp(df):
    print("Saving Yelp businesses to CSV for fuzzy matching...")
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/yelp_sb_businesses.csv", index=False)
    print(f"Saved {len(df)} Yelp businesses to data/processed/yelp_sb_businesses.csv")


# ── 4. Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    overture_df = load_overture()
    yelp_df = load_yelp_businesses()

    print(f"\nOverture sample:\n{overture_df.head(3)}")
    print(f"\nYelp sample:\n{yelp_df.head(3)}")

    save_overture(overture_df)
    save_yelp(yelp_df)

    print("\nIngestion complete.")