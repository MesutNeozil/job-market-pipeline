from pathlib import Path
import pandas as pd

def extract_jobs(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("Input file is empty")

    output_path = Path("data/processed/extracted_jobs.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return str(output_path)