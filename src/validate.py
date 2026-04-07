import pandas as pd

REQUIRED_COLUMNS = [
    "job_id",
    "title",
    "company",
    "location",
    "date_posted",
    "salary_text",
    "description",
]


def validate_jobs(file_path: str) -> str:
    df = pd.read_csv(file_path)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df["job_id"].isna().any():
        raise ValueError("Found null job_id values")

    df = df.drop_duplicates(subset=["job_id"])

    output_path = "data/processed/validated_jobs.csv"
    df.to_csv(output_path, index=False)
    return output_path