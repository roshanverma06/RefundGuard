import os
import pandas as pd

def inspect_csv(raw_dir: str, raw_csv_name: str, n: int = 5) -> None:
    path = os.path.join(raw_dir, raw_csv_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found at: {path}")

    df = pd.read_csv(path)

    print("\n=== CSV FOUND ===")
    print(path)

    print("\n=== COLUMNS ===")
    for c in df.columns.tolist():
        print(c)

    print(f"\n=== FIRST {n} ROWS ===")
    print(df.head(n).to_string(index=False))
