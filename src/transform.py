import re
import pandas as pd

def parse_salary(salary_text: str):
    if pd.isna(salary_text):
        return None, None

    nums = re.findall(r"\d+", str(salary_text).replace(",", ""))
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    if len(nums) == 1:
        value = float(nums[0])
        return value, value
    return None, None


def transform_jobs(file_path: str) -> str:
    df = pd.read_csv(file_path)

    salary_values = df["salary_text"].apply(parse_salary)
    df["min_salary"] = salary_values.apply(lambda x: x[0])
    df["max_salary"] = salary_values.apply(lambda x: x[1])

    df["remote_flag"] = df["location"].str.lower().eq("remote")

    desc = df["description"].fillna("").str.lower()
    df["skill_python"] = desc.str.contains("python")
    df["skill_sql"] = desc.str.contains("sql")
    df["skill_tableau"] = desc.str.contains("tableau")
    df["skill_airflow"] = desc.str.contains("airflow")

    output_path = "data/processed/transformed_jobs.csv"
    df.to_csv(output_path, index=False)
    return output_path