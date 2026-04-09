from datetime import datetime, timedelta
from pathlib import Path
import psycopg2

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.extract import extract_jobs
from src.validate import validate_jobs
from src.transform import transform_jobs
from src.load import load_to_postgres
from src.export import export_daily_metrics

@dag(
    dag_id="job_market_pipeline",
    start_date=datetime(2026, 4, 7),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "justin",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["portfolio", "etl", "jobs"],
)
def job_market_pipeline():

    @task
    def create_tables_task():
        sql_path = Path("/opt/airflow/sql/create_tables.sql")
        sql_text = sql_path.read_text(encoding="utf-8")

        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            port=5432,
        )
        cur = conn.cursor()
        cur.execute(sql_text)
        conn.commit()
        cur.close()
        conn.close()

        return "Tables created"

    @task
    def extract_task():
        return extract_jobs("data/raw/jobs_2026_04_07.csv")

    @task
    def validate_task(file_path: str):
        return validate_jobs(file_path)

    @task
    def transform_task(file_path: str):
        return transform_jobs(file_path)

    @task
    def load_task(file_path: str):
        return load_to_postgres(file_path)
    
    @task
    def export_task():
        return export_daily_metrics()
    
    @task
    def build_marts_task():
        sql_path = Path("/opt/airflow/sql/marts.sql")
        sql_text = sql_path.read_text(encoding="utf-8")

        conn = psycopg2.connect(
            host="postgres",
            dbname="airflow",
            user="airflow",
            password="airflow",
            port=5432,
        )
        cur = conn.cursor()
        cur.execute(sql_text)
        conn.commit()
        cur.close()
        conn.close()

        return "Marts built"

    created = create_tables_task()
    extracted = extract_task()
    validated = validate_task(extracted)
    transformed = transform_task(validated)
    loaded = load_task(transformed)
    marts = build_marts_task()
    exported = export_task()

    created >> extracted
    loaded >> marts >> exported


job_market_pipeline()