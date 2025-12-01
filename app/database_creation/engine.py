from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg://airflow:airflow@localhost:5432/postgres"

engine = create_engine(
    DATABASE_URL,
    echo=True,
    future=True,
)