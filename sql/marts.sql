DROP TABLE IF EXISTS mart_jobs_by_skill;
CREATE TABLE mart_jobs_by_skill AS
SELECT date_posted AS job_date, 'python' AS skill_name, COUNT(*) AS jobs_count
FROM stg_jobs
WHERE skill_python = TRUE
GROUP BY date_posted

UNION ALL

SELECT date_posted AS job_date, 'sql' AS skill_name, COUNT(*) AS jobs_count
FROM stg_jobs
WHERE skill_sql = TRUE
GROUP BY date_posted

UNION ALL

SELECT date_posted AS job_date, 'tableau' AS skill_name, COUNT(*) AS jobs_count
FROM stg_jobs
WHERE skill_tableau = TRUE
GROUP BY date_posted

UNION ALL

SELECT date_posted AS job_date, 'airflow' AS skill_name, COUNT(*) AS jobs_count
FROM stg_jobs
WHERE skill_airflow = TRUE
GROUP BY date_posted;

DROP TABLE IF EXISTS mart_jobs_daily;
CREATE TABLE mart_jobs_daily AS
SELECT
    date_posted AS job_date,
    COUNT(*) AS total_jobs,
    SUM(CASE WHEN remote_flag THEN 1 ELSE 0 END) AS remote_jobs,
    AVG(min_salary) AS avg_min_salary,
    AVG(max_salary) AS avg_max_salary
FROM stg_jobs
GROUP BY date_posted;