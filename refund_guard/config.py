from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///refund_guard.db")

    raw_dir: str = os.getenv("RAW_DIR", "data/raw")
    exports_dir: str = os.getenv("EXPORTS_DIR", "data/exports")
    reports_dir: str = os.getenv("REPORTS_DIR", "reports")

    raw_csv_name: str = os.getenv("RAW_CSV_NAME", "returns_sustainability_dataset.csv")

def get_settings() -> Settings:
    return Settings()
