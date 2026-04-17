"""
Full Bronze → Silver → Gold ETL pipeline — runs 100% locally.
No AWS account needed. Uses pandas + local parquet files.

Run: python pipeline/local_etl.py
"""

import os
import pandas as pd
from datetime import datetime

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
BRONZE = os.path.join(BASE, "bronze")
SILVER = os.path.join(BASE, "silver")
GOLD   = os.path.join(BASE, "gold")

for p in [BRONZE, SILVER, GOLD]:
    os.makedirs(p, exist_ok=True)


# ── INGEST: raw CSV → Bronze ──────────────────────────────────────────────────
def ingest_bronze():
    print("\n[1/3] Bronze — ingesting raw CSV...")
    src = os.path.join(BASE, "sample_transactions.csv")
    df  = pd.read_csv(src)
    df["_ingested_at"] = datetime.utcnow().isoformat()
    out = os.path.join(BRONZE, "transactions.parquet")
    df.to_parquet(out, index=False)
    print(f"  ✓ {len(df)} rows written → {out}")
    return df


# ── TRANSFORM: Bronze → Silver ────────────────────────────────────────────────
def transform_silver(df: pd.DataFrame):
    print("\n[2/3] Silver — cleaning & validating...")
    before = len(df)

    df = df.drop_duplicates(subset=["id"])
    df = df.dropna(subset=["id", "user_id", "event_time"])
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["amount"]     = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"].notna()]

    # Flag anomalies (amount > 3 std devs from mean)
    mean, std = df["amount"].mean(), df["amount"].std()
    df["is_anomaly"] = (df["amount"] - mean).abs() > 3 * std

    df["event_date"] = df["event_time"].dt.date
    df["_processed_at"] = datetime.utcnow().isoformat()

    out = os.path.join(SILVER, "transactions.parquet")
    df.to_parquet(out, index=False)
    print(f"  ✓ {before - len(df)} rows dropped, {len(df)} clean rows → {out}")
    print(f"  ✓ {df['is_anomaly'].sum()} anomalies flagged")
    return df


# ── AGGREGATE: Silver → Gold ──────────────────────────────────────────────────
def aggregate_gold(df: pd.DataFrame):
    print("\n[3/3] Gold — building analytics tables...")

    # Daily aggregates
    daily = (
        df[df["amount"] > 0]
        .groupby(["event_date", "region", "category"])
        .agg(
            total_events  = ("id", "count"),
            total_revenue = ("amount", "sum"),
            avg_order     = ("amount", "mean"),
            unique_users  = ("user_id", "nunique"),
        )
        .reset_index()
    )
    daily.to_parquet(os.path.join(GOLD, "daily_aggregates.parquet"), index=False)
    print(f"  ✓ daily_aggregates: {len(daily)} rows")

    # User summary
    users = df.groupby("user_id").agg(
        total_orders      = ("id", "count"),
        lifetime_value    = ("amount", "sum"),
        avg_order_value   = ("amount", "mean"),
        first_seen        = ("event_time", "min"),
        last_seen         = ("event_time", "max"),
    ).reset_index()
    users["days_active"] = (users["last_seen"] - users["first_seen"]).dt.days
    users.to_parquet(os.path.join(GOLD, "user_summary.parquet"), index=False)
    print(f"  ✓ user_summary: {len(users)} users")

    return daily, users


# ── REPORT ────────────────────────────────────────────────────────────────────
def print_report(daily, users):
    print("\n" + "="*50)
    print("  PIPELINE COMPLETE — GOLD LAYER SUMMARY")
    print("="*50)
    print(f"\n  Top regions by revenue:")
    top = daily.groupby("region")["total_revenue"].sum().sort_values(ascending=False)
    for region, rev in top.items():
        print(f"    {region:<15} ${rev:,.2f}")
    print(f"\n  Top users by lifetime value:")
    top_users = users.nlargest(3, "lifetime_value")[["user_id","lifetime_value","total_orders"]]
    print(top_users.to_string(index=False))
    print()


if __name__ == "__main__":
    print("=" * 50)
    print("  AWS DATA LAKEHOUSE — LOCAL ETL DEMO")
    print("=" * 50)
    bronze_df = ingest_bronze()
    silver_df = transform_silver(bronze_df)
    daily, users = aggregate_gold(silver_df)
    print_report(daily, users)
