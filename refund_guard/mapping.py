import json
import pandas as pd

REQUIRED_CANONICAL_COLUMNS = [
    "order_id",
    "customer_id",
    "seller_id",
    "order_date",
    "return_date",
    "order_amount",
    "return_reason",
    "return_status",
]

def load_column_map(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def standardize_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    renamed = df.rename(columns=column_map)

    missing = [c for c in REQUIRED_CANONICAL_COLUMNS if c not in renamed.columns]
    if missing:
        raise ValueError(
            "Missing required columns AFTER mapping: "
            + ", ".join(missing)
            + "\nFix refund_guard/column_map.json using the exact dataset headers."
        )

    return renamed
