import pandas as pd
from refund_guard.db import get_engine
from refund_guard.mapping import load_column_map, standardize_columns

# Keywords for "tampered / used / swapped item"
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
    Reads stg_returns from SQLite, standardizes columns, then builds:
      - fact_returns: row-level analytics with risk flags + risk_score
      - seller_risk_rollup: (actually product-level risk in your dataset)
      - customer_behavior_rollup: user-level behavior patterns
    """
    engine = get_engine(database_url)

    # 1) Read staging table
    with engine.begin() as conn:
        raw = pd.read_sql("SELECT * FROM stg_returns", conn)

    # 2) Standardize columns using column_map.json
    col_map = load_column_map(column_map_path)
    df = standardize_columns(raw, col_map)

    # 3) Parse types
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    #  "return_date" is actually Days_to_Return (a number)
    df["days_to_return"] = pd.to_numeric(df["return_date"], errors="coerce")

    # Build an approximate return_date 
    df["return_date"] = df["order_date"] + pd.to_timedelta(df["days_to_return"].fillna(0), unit="D")

    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce").fillna(0)

    # 4) Returned flag (Return_Status likely contains values like Returned/Not Returned)
    df["is_returned"] = df["return_status"].astype(str).str.lower().isin(
        ["returned", "yes", "true", "1"]
    ).astype(int)

    # 5) Risk signals (rule-based, explainable)
    df["tamper_reason_flag"] = df["return_reason"].astype(str).apply(lambda x: int(_contains_any(x, TAMPER_KEYWORDS)))
    df["fast_return_flag"] = (df["days_to_return"].fillna(999) <= 2).astype(int)

    # High value threshold = top 10% of order_amount
    q90 = df["order_amount"].quantile(0.90) if len(df) else 0
    df["high_value_flag"] = (df["order_amount"] >= q90).astype(int)

    # 6) Risk score (0–100)
    df["risk_score"] = (
        40 * df["tamper_reason_flag"] +
        25 * df["fast_return_flag"] +
        20 * df["high_value_flag"] +
        15 * df["is_returned"]
    ).clip(0, 100)

    # 7) Keep additional sustainability + profit columns if present
    optional_cols = []
    for c in ["Profit_Loss", "CO2_Emissions", "Packaging_Waste", "CO2_Saved", "Waste_Avoided", "Product_Category"]:
        if c in raw.columns:
            optional_cols.append(c)

    # Add optional cols to fact table (if they exist)
    fact_cols = [
        "order_id", "customer_id", "seller_id",
        "order_date", "return_date",
        "order_amount", "return_reason", "return_status",
        "days_to_return", "is_returned",
        "tamper_reason_flag", "fast_return_flag", "high_value_flag",
        "risk_score"
    ]

    # Map optional raw columns, so attach from raw by joining on order id if possible
    fact = df[fact_cols].copy()

    # 8) “Seller” rollup
    seller_risk = (
        fact.groupby("seller_id", as_index=False)
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
    seller_risk["return_rate"] = seller_risk["returned_count"] / seller_risk["order_count"]
    seller_risk["tamper_rate"] = seller_risk["tamper_flags"] / seller_risk["order_count"]

    # 9) Customer rollup (user-level return/tamper behavior)
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

    # 10) Write curated tables to SQLite
    fact.to_sql("fact_returns", engine, if_exists="replace", index=False)
    seller_risk.to_sql("seller_risk_rollup", engine, if_exists="replace", index=False)
    customer_behavior.to_sql("customer_behavior_rollup", engine, if_exists="replace", index=False)
