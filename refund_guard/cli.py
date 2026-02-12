import os
import typer

from refund_guard.config import get_settings
from refund_guard.inspect import inspect_csv
from refund_guard.stage import stage_raw_returns
from refund_guard.transform import transform_curated
from refund_guard.quality import run_checks
from refund_guard.export import export_tables


app = typer.Typer(help="RefundGuard ETL CLI")


@app.command()
def show_config():
    s = get_settings()
    print("Raw dir:", s.raw_dir)
    print("CSV name:", s.raw_csv_name)
    print("DB URL:", s.database_url)


@app.command()
def inspect(n: int = 5):
    s = get_settings()
    inspect_csv(s.raw_dir, s.raw_csv_name, n=n)


@app.command()
def run_pipeline():
    s = get_settings()
    os.makedirs(s.raw_dir, exist_ok=True)

    print("Staging...")
    stage_raw_returns(s.raw_dir, s.raw_csv_name, s.database_url)

    print("Transforming...")
    transform_curated(s.database_url, "refund_guard/column_map.json")

    print("Validating...")
    issues = run_checks(db_path="refund_guard.db")
    if issues:
        print("Validation failed:", issues)
        raise typer.Exit(code=1)

    print("Done.")

@app.command()
def export():
    s = get_settings()
    paths = export_tables(s.database_url, s.exports_dir)
    for p in paths:
        print(p)



if __name__ == "__main__":
    app()
