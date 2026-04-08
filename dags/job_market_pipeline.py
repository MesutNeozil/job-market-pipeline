from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from src.extract import extract_jobs
from src.validate import validate_jobs
from src.transform import transform_jobs
from src.load import load_to_postgres


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

        hook = PostgresHook(
            postgres_conn_id=None,
            host="postgres",
            schema="airflow",
            login="airflow",
            password="airflow",
            port=5432,
        )
        conn = hook.get_conn()
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

    created = create_tables_task()
    extracted = extract_task()
    validated = validate_task(extracted)
    transformed = transform_task(validated)
    created >> extracted
    load_task(transformed)


job_market_pipeline()