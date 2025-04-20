import os
from typing import List
from datetime import datetime

import pytz
from prefect.runtime import flow_run


def gen_job_id() -> str:
    """
    Generate Job ID

    return:
        String format of datetime (year to seconds)
    """
    st = flow_run.get_scheduled_start_time().replace(tzinfo=pytz.timezone("UTC")).astimezone(tz=pytz.timezone("Asia/Jakarta"))
    return st.strftime("%Y%m%d%H%M%S")


def get_query(query_dir: str, query_file: str) -> str:
    """
    Helper function to read query from file

    params:
        - query_dir: str. Directory path where query saved
        - query_file: str. Query filename (need to specify extensions)
    
    return:
        String representation of query
    """
    query_path = os.path.join(query_dir, query_file)
    with open(query_path, "r") as file:
        query = file.read()
    
    return query


def parse_job_id(job_id: str) -> datetime:
    """
    Parsing Job ID to Datetime object

    params:
        - job_id: str. Job ID to be parsed
    
    return:
        Datetime object from Job ID
    """    
    __job_id = datetime.strptime(job_id, "%Y%m%d%H%M%S")
    return __job_id


def remove_duplicate(data: List[dict], unique_key: List[str], order_by: str = "job_id", desc: bool = True) -> List[dict]:
    """
    Helper function to remove duplicate from list of data

    params:
        - data: List[dict]. List of data
        - unique_key: List[str]. List of unique key constraint
        - order_by: str. (default: job_id). Key for determine data selection
        - desc: bool. (default: True). Sorting descending when determine data selection
    
    return:
        List of data with removed duplicates
    """
    unique_key_combi = set([
        ";".join([d.get(uk, "-") for uk in unique_key])
        for d in data
    ])
    
    grouped_data = {
        uk: list(filter(lambda d: ";".join([d.get(_uk, "-") for _uk in unique_key]) == uk, data))
        for uk in unique_key_combi
    }

    cleaned_data = [sorted(d, key=lambda d: d.get(order_by, "-"), reverse=desc)[0] for d in grouped_data.values()]
    return cleaned_data