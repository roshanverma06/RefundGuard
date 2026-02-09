import typer
from refund_guard.config import get_settings
from refund_guard.inspect import inspect_csv

app = typer.Typer(help="RefundGuard ETL CLI")

@app.command()
def show_config():
    """Print config values so we know CLI works."""
    s = get_settings()
    print("CLI is working")
    print("Raw dir:", s.raw_dir)
    print("CSV name:", s.raw_csv_name)
    print("DB URL:", s.database_url)

@app.command()
def inspect(n: int = 5):
    """Print dataset columns + preview rows."""
    s = get_settings()
    inspect_csv(s.raw_dir, s.raw_csv_name, n=n)

if __name__ == "__main__":
    app()
