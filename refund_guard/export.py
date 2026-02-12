import os
import pandas as pd
from refund_guard.db import get_engine

EXPORT_TABLES = [
    "fact_returns",
    "product_risk_rollup",
    "customer_behavior_rollup",
]


def export_tables(database_url: str, exports_dir: str) -> list[str]:
    os.makedirs(exports_dir, exist_ok=True)
    engine = get_engine(database_url)

    paths: list[str] = []
    with engine.begin() as conn:
        for t in EXPORT_TABLES:
            df = pd.read_sql(f"SELECT * FROM {t};", conn)
            out_path = os.path.join(exports_dir, f"{t}.csv")
            df.to_csv(out_path, index=False)
            paths.append(out_path)

    return paths
