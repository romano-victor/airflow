from sqlalchemy import text
from app.database_creation.engine import engine
from app.database_creation.base import small_business_data_base

SCHEMA_NAME = "small_business_data"

def run_db_builder():
        with engine.connect() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}";'))
                conn.commit()

        small_business_data_base.metadata.create_all(engine)

if __name__ == "__main__":
        run_db_builder()