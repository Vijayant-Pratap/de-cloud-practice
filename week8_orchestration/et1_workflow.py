import subprocess
import json
import pandas as pd
from prefect import task, flow

# TASK 1: Extract (with automatic self-healing retries!)
@task(retries=2, retry_delay_seconds=3)
def extract_repo_data(repo_name: str):
    print(f"Fetching live data for {repo_name}...")
    raw = subprocess.check_output(["gh", "api", f"repos/{repo_name}"])
    return json.loads(raw)

# TASK 2: Transform (only runs AFTER Task 1 succeeds)
@task
def transform_data(raw_dict: dict):
    print("Transforming JSON into a clean Pandas table...")
    clean_row = {
        "repository": [raw_dict["full_name"]],
        "stars": [raw_dict["stargazers_count"]],
        "open_issues": [raw_dict["open_issues_count"]]
    }
    return pd.DataFrame(clean_row)

# TASK 3: Load (saves output cleanly)
@task
def load_data(df: pd.DataFrame):
    print("Saving transformed table to CSV...")
    df.to_csv("airflow_stats.csv", index=False)
    print(df)

# THE FLOW (The "Air Traffic Controller" / DAG)
@flow(name="GitHub-ETL-Pipeline")
def main_pipeline():
    raw_json = extract_repo_data("apache/airflow")
    clean_df = transform_data(raw_json)
    load_data(clean_df)

if __name__ == "__main__":
    main_pipeline()
