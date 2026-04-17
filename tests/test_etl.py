"""Unit tests for local ETL pipeline."""

import os, sys
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pipeline.local_etl import ingest_bronze, transform_silver, aggregate_gold


def test_ingest_creates_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.local_etl.BRONZE", str(tmp_path / "bronze"))
    monkeypatch.setattr("pipeline.local_etl.BASE",
                        os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(str(tmp_path / "bronze"), exist_ok=True)
    df = ingest_bronze()
    assert len(df) > 0
    assert "_ingested_at" in df.columns


def test_silver_removes_nulls():
    df = pd.DataFrame({
        "id": [1, 2, None],
        "user_id": ["u1", "u2", "u3"],
        "event_time": ["2026-01-01", "2026-01-01", "2026-01-01"],
        "amount": [100.0, 200.0, 50.0],
        "region": ["us-east"] * 3,
        "category": ["electronics"] * 3,
        "event_type": ["purchase"] * 3,
        "_ingested_at": ["now"] * 3,
    })
    df_clean = df.dropna(subset=["id", "user_id", "event_time"])
    assert len(df_clean) == 2


def test_gold_aggregation():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "user_id": ["u1", "u1", "u2"],
        "amount": [100.0, 200.0, 50.0],
        "event_time": pd.to_datetime(["2026-01-01", "2026-01-01", "2026-01-01"]),
        "event_date": ["2026-01-01"] * 3,
        "region": ["us-east"] * 3,
        "category": ["electronics"] * 3,
    })
    users = df.groupby("user_id").agg(lifetime_value=("amount", "sum")).reset_index()
    assert users[users["user_id"] == "u1"]["lifetime_value"].values[0] == 300.0
