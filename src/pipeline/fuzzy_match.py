import os
import pandas as pd
from sqlalchemy import create_engine, text
from rapidfuzz import fuzz
from dotenv import load_dotenv
import mlflow

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

# ── Config ────────────────────────────────────────────────────────────────────
NAME_THRESHOLD = 80      # minimum name similarity score (0-100)
DISTANCE_THRESHOLD = 0.1  # max distance in degrees (~100 metres)

# ── Load data ─────────────────────────────────────────────────────────────────

def load_overture_from_db():
    print("Loading Overture venues from database...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, overture_id, name, latitude, longitude, category FROM venues"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    print(f"Loaded {len(df)} Overture venues")
    return df

def load_yelp_from_csv(path="data/processed/yelp_sb_businesses.csv"):
    print("Loading Yelp businesses from CSV...")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} Yelp businesses")
    return df

# ── Matching logic ────────────────────────────────────────────────────────────

def is_within_distance(lat1, lon1, lat2, lon2, threshold):
    return abs(lat1 - lat2) < threshold and abs(lon1 - lon2) < threshold

def find_best_match(overture_row, yelp_df):
    candidates = yelp_df[
        yelp_df.apply(lambda y: is_within_distance(
            overture_row["latitude"], overture_row["longitude"],
            y["latitude"], y["longitude"],
            DISTANCE_THRESHOLD
        ), axis=1)
    ]
    if candidates.empty:
        return None, 0

    best_score = 0
    best_match = None
    for _, yelp_row in candidates.iterrows():
        score = fuzz.token_sort_ratio(
            str(overture_row["name"]).lower(),
            str(yelp_row["name"]).lower()
        )
        if score > best_score:
            best_score = score
            best_match = yelp_row

    if best_score >= NAME_THRESHOLD:
        return best_match, best_score
    return None, 0

# ── Run matching ──────────────────────────────────────────────────────────────

def run_matching(overture_df, yelp_df):
    print("Running fuzzy matching...")
    matched = []
    unmatched = 0

    for _, overture_row in overture_df.iterrows():
        best_match, score = find_best_match(overture_row, yelp_df)
        if best_match is not None:
            matched.append({
                "venue_id": overture_row["id"],
                "overture_name": overture_row["name"],
                "yelp_name": best_match["name"],
                "yelp_id": best_match["yelp_id"],
                "yelp_rating": best_match["yelp_rating"],
                "yelp_review_count": best_match["yelp_review_count"],
                "match_confidence": score,
            })
        else:
            unmatched += 1

    matched_df = pd.DataFrame(matched)
    print(f"Matched: {len(matched_df)} venues")
    print(f"Unmatched: {unmatched} venues")
    return matched_df

# ── Save matches to database ──────────────────────────────────────────────────

def save_matches(matched_df):
    print("Saving matches to database...")
    with engine.connect() as conn:
        for _, row in matched_df.iterrows():
            conn.execute(text("""
                UPDATE venues
                SET yelp_id = :yelp_id,
                    yelp_rating = :yelp_rating,
                    yelp_review_count = :yelp_review_count,
                    match_confidence = :match_confidence
                WHERE id = :venue_id
            """), row.to_dict())
        conn.commit()
    print(f"Saved {len(matched_df)} matches to database")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mlflow.set_experiment("fuzzy_matching")

    with mlflow.start_run():
        mlflow.log_param("name_threshold", NAME_THRESHOLD)
        mlflow.log_param("distance_threshold", DISTANCE_THRESHOLD)

        overture_df = load_overture_from_db()
        yelp_df = load_yelp_from_csv()

        matched_df = run_matching(overture_df, yelp_df)

        # Save results
        save_matches(matched_df)
        matched_df.to_csv("data/processed/matched_venues.csv", index=False)

        # Log metrics to MLflow
        match_rate = len(matched_df) / len(overture_df) * 100
        avg_confidence = matched_df["match_confidence"].mean()

        mlflow.log_metric("total_overture_venues", len(overture_df))
        mlflow.log_metric("total_yelp_businesses", len(yelp_df))
        mlflow.log_metric("matched_venues", len(matched_df))
        mlflow.log_metric("match_rate_pct", match_rate)
        mlflow.log_metric("avg_confidence_score", avg_confidence)

        print(f"\nMatch rate: {match_rate:.1f}%")
        print(f"Average confidence score: {avg_confidence:.1f}")
        print("\nSample matches:")
        print(matched_df[["overture_name", "yelp_name", "match_confidence"]].head(10))