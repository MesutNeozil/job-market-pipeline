from datetime import datetime, timedelta
from airflow.decorators import dag, task

from src.extract import extract_jobs
from src.validate import validate_jobs
from src.transform import transform_jobs


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
    def extract_task():
        return extract_jobs("data/raw/jobs_2026_04_07.csv")

    @task
    def validate_task(file_path: str):
        return validate_jobs(file_path)

    @task
    def transform_task(file_path: str):
        return transform_jobs(file_path)

    extracted = extract_task()
    validated = validate_task(extracted)
    transform_task(validated)


job_market_pipeline()