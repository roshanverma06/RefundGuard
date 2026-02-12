import pandas as pd
from refund_guard.db import get_engine
from refund_guard.mapping import load_column_map, standardize_columns

TAMPER_KEYWORDS = [
    "tamper", "tempered", "opened", "seal broken",
    "used", "scratched", "replaced", "swapped",
    "wrong item", "not as described", "counterfeit",
    "missing parts", "empty box"
]


def _contains_any(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any(k in t for k in keywords)


def transform_curated(database_url: str, column_map_path: str) -> None:
    """
    Builds curated analytics tables from the staging table.
    Output tables:
      - fact_returns
      - product_risk_rollup
      - customer_behavior_rollup
    """
    engine = get_engine(database_url)

    # Load raw staged data from the database
    with engine.begin() as conn:
        raw = pd.read_sql("SELECT * FROM stg_returns", conn)

    # Rename dataset-specific columns to a canonical schema used by the pipeline
    col_map = load_column_map(column_map_path)
    df = standardize_columns(raw, col_map)

    # Product_ID is modeled as the tracked entity for risk in this dataset
    df = df.rename(columns={"seller_id": "product_id"})

    # Basic type normalization
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce").fillna(0)

    # In this dataset, the mapped "return_date" field is a numeric days-to-return value
    df["days_to_return"] = pd.to_numeric(df["return_date"], errors="coerce")

    # Build an approximate return_date to keep downstream logic consistent
    df["return_date"] = df["order_date"] + pd.to_timedelta(df["days_to_return"].fillna(0), unit="D")

    # Normalize return status into a simple 0/1 flag
    df["is_returned"] = df["return_status"].astype(str).str.lower().isin(
        ["returned", "yes", "true", "1"]
    ).astype(int)

    # Rule-based signals derived from return reason and timing
    df["tamper_reason_flag"] = df["return_reason"].astype(str).apply(
        lambda x: int(_contains_any(x, TAMPER_KEYWORDS))
    )
    df["fast_return_flag"] = (df["days_to_return"].fillna(999) <= 2).astype(int)

    # High value is defined as top 10% of order amount within the dataset
    q90 = df["order_amount"].quantile(0.90) if len(df) else 0
    df["high_value_flag"] = (df["order_amount"] >= q90).astype(int)

    # Simple 0–100 score based on explainable signals
    df["risk_score"] = (
        40 * df["tamper_reason_flag"] +
        25 * df["fast_return_flag"] +
        20 * df["high_value_flag"] +
        15 * df["is_returned"]
    ).clip(0, 100)

    # Row-level table used by downstream analysis and Tableau
    fact_cols = [
        "order_id", "customer_id", "product_id",
        "order_date", "return_date",
        "order_amount", "return_reason", "return_status",
        "days_to_return", "is_returned",
        "tamper_reason_flag", "fast_return_flag", "high_value_flag",
        "risk_score"
    ]
    fact = df[fact_cols].copy()

    # Product-level risk rollup (product_id acts as the entity key)
    product_risk = (
        fact.groupby("product_id", as_index=False)
        .agg(
            order_count=("order_id", "nunique"),
            returned_count=("is_returned", "sum"),
            tamper_flags=("tamper_reason_flag", "sum"),
            avg_risk_score=("risk_score", "mean"),
            avg_order_amount=("order_amount", "mean"),
            avg_days_to_return=("days_to_return", "mean"),
            loss_exposure=("order_amount", lambda s: float(s[fact.loc[s.index, "is_returned"].eq(1)].sum()))
        )
        .fillna(0)
    )
    product_risk["return_rate"] = product_risk["returned_count"] / product_risk["order_count"]
    product_risk["tamper_rate"] = product_risk["tamper_flags"] / product_risk["order_count"]

    # Customer behavior rollup (user-level patterns)
    customer_behavior = (
        fact.groupby("customer_id", as_index=False)
        .agg(
            order_count=("order_id", "nunique"),
            returned_count=("is_returned", "sum"),
            tamper_flags=("tamper_reason_flag", "sum"),
            avg_risk_score=("risk_score", "mean"),
            loss_exposure=("order_amount", lambda s: float(s[fact.loc[s.index, "is_returned"].eq(1)].sum()))
        )
        .fillna(0)
    )
    customer_behavior["return_rate"] = customer_behavior["returned_count"] / customer_behavior["order_count"]
    customer_behavior["tamper_claim_rate"] = customer_behavior["tamper_flags"] / customer_behavior["order_count"]

    # Persist curated tables back to SQLite
    fact.to_sql("fact_returns", engine, if_exists="replace", index=False)
    product_risk.to_sql("product_risk_rollup", engine, if_exists="replace", index=False)
    customer_behavior.to_sql("customer_behavior_rollup", engine, if_exists="replace", index=False)
