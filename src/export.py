import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

def export_daily_metrics(output_path: str = "data/exports/mart_jobs_daily.csv") -> str:
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "airflow")

    conn_string = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(conn_string)

    df = pd.read_sql("SELECT * FROM mart_jobs_daily ORDER BY job_date", engine)

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)

    return str(output_file)