CREATE TABLE IF NOT EXISTS raw_jobs (
    job_id TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    date_posted DATE,
    salary_text TEXT,
    description TEXT,
    source_file TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_jobs (
    job_id TEXT PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    date_posted DATE,
    min_salary NUMERIC,
    max_salary NUMERIC,
    remote_flag BOOLEAN,
    skill_python BOOLEAN,
    skill_sql BOOLEAN,
    skill_tableau BOOLEAN,
    skill_airflow BOOLEAN,
    source_file TEXT,
    ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);