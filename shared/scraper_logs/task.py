import os
from typing import List
from datetime import datetime

from prefect import task

from revi_toolbox.adapters import PostgreAdapter

from shared.macro import get_query, parse_job_id

QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")


@task(retries=0)
def load_log(db: PostgreAdapter, service: str, name: str, runtime_job_id: str):
    """
    Task: Loading Log to Database

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - service: str. Job service name
        - name: str. Job name
        - runtime_job_id: str. Job ID of scraper job
    """
    query = get_query(QUERY_DIR, "load_scraper_logs.sql")
    _job_at = parse_job_id(runtime_job_id).strftime("%Y-%m-%d %H:%M:%S")
    data = [{
        "job_service": service,
        "job_name": name,
        "job_id": runtime_job_id,
        "job_at": _job_at
    }]

    _ = db.run_query(query, data)


@task(retries=0)
def fetch_new_job_id(db: PostgreAdapter, service: str) -> List[str]:
    """
    Task: Fetch unprocessed (new) Job ID based on service

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - service: str. Job service name
    
    return:
        List of unprocessed Job ID
    """
    # Prepare Query and Params
    query = get_query(QUERY_DIR, "fetch_new_job_id.sql")
    params = {"job_service": service}

    # Run Query
    job_ids = [r["job_id"] for r in db.run_query(query, params)]
    return job_ids


@task(retries=0)
def mark_job_id(db: PostgreAdapter, service: str, runtime_job_id: List[str], processed_at: datetime):
    """
    Task: Mark Job ID to Processed

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - service: str. Job service name
        - runtime_job_id: List[str]. List of processed Job ID
        - processed_at: datetime. Data processing start time
    """
    # Prepare Query and Params
    query = get_query(QUERY_DIR, "update_scraper_logs_processed.sql")
    _processed_at = processed_at.strftime("%Y-%m-%d %H:%M:%S")
    
    params = {
        "job_service": service,
        "job_id": tuple(runtime_job_id),
        "processed_at": _processed_at
    }

    # Run Query
    _ = db.run_query(query, params)
