from sqlalchemy import create_engine

def get_engine(database_url: str):
    return create_engine(database_url)
