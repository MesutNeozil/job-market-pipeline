from pathlib import Path
import random
import sys
from datetime import datetime, timedelta

import pandas as pd


TITLES = [
    "Data Analyst",
    "Data Engineer",
    "Business Analyst",
    "Analytics Engineer",
    "BI Analyst",
    "Machine Learning Engineer",
    "Data Scientist",
    "Reporting Analyst",
]

COMPANIES = [
    "ABC Tech",
    "DataWorks",
    "Insight Pte Ltd",
    "Metric Labs",
    "Vision Analytics",
    "CloudNova",
    "ByteBridge",
    "Quantive Solutions",
]

LOCATIONS = [
    "Singapore",
    "Remote",
    "Hybrid",
    "Jurong East",
    "One-North",
    "Tampines",
    "CBD",
]

SKILL_PHRASES = [
    "SQL, Python, Tableau required",
    "Airflow, Python, ETL, PostgreSQL",
    "Excel, SQL, dashboarding",
    "Python, SQL, Airflow, dbt",
    "Tableau, Excel, SQL",
    "Python, machine learning, SQL",
    "Power BI, Excel, SQL",
    "ETL, data warehousing, Airflow",
]

SALARY_RANGES = [
    "SGD 3800-4800",
    "SGD 4000-5000",
    "SGD 4200-5200",
    "SGD 4500-5500",
    "SGD 5000-6500",
    "SGD 5500-7000",
    "SGD 6000-8000",
]


def generate_job_data(
    num_rows: int = 20,
    output_file: str | None = None,
    seed: int = 42,
) -> str:
    random.seed(seed)

    if output_file is None:
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        output_file = f"data/raw/jobs_{timestamp}.csv"

    start_date = datetime(2026, 4, 1)
    rows = []

    for job_id in range(1, num_rows + 1):
        date_posted = start_date + timedelta(days=random.randint(0, 14))

        rows.append(
            {
                "job_id": job_id,
                "title": random.choice(TITLES),
                "company": random.choice(COMPANIES),
                "location": random.choice(LOCATIONS),
                "date_posted": date_posted.strftime("%Y-%m-%d"),
                "salary_text": random.choice(SALARY_RANGES),
                "description": random.choice(SKILL_PHRASES),
            }
        )

    df = pd.DataFrame(rows)

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return str(output_path)


if __name__ == "__main__":
    num_rows = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    path = generate_job_data(num_rows=num_rows, output_file=None, seed=42)
    print(f"Generated file: {path}")