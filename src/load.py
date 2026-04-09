import os
import pandas as pd
from sqlalchemy import create_engine, text

def load_to_postgres(file_path: str, table_name: str = "stg_jobs") -> str:
    df = pd.read_csv(file_path)

    columns_to_load = [
        "job_id",
        "title",
        "company",
        "location",
        "date_posted",
        "min_salary",
        "max_salary",
        "remote_flag",
        "skill_python",
        "skill_sql",
        "skill_tableau",
        "skill_airflow",
    ]

    df = df[columns_to_load]

    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "airflow")

    conn_string = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(conn_string)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))

    df.to_sql(table_name, engine, if_exists="append", index=False)

    return f"Loaded {len(df)} rows into {table_name}"