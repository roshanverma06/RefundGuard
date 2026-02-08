import os
import pandas as pd
from refund_guard.db import get_engine

def stage_raw_returns(raw_dir: str, raw_csv_name: str, database_url: str) -> None:
    path = os.path.join(raw_dir, raw_csv_name)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing dataset CSV at: {path}\n"
            f"Download it using scripts/get_dataset.md"
        )

    df = pd.read_csv(path)
    engine = get_engine(database_url)
    df.to_sql("stg_returns", engine, if_exists="replace", index=False)
