import os
from typing import List

from revi_toolbox.adapters import PostgreAdapter
from revi_toolbox.scraper.manga import MangabatsScraperRunner

from prefect import flow, task
from prefect.variables import Variable
from prefect.blocks.system import Secret

from shared.macro import gen_job_id, get_query, parse_job_id
from shared.scraper_logs import load_log

from flows.manga.schemas import RawManga


QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")


# Flow
@flow(
    name = "manga_mangabats_scraper_overviews",
    log_prints = True
)
def main(slug_list: list[str]):
    """
    Flow: Running Scraper for MangaBats Manga information

    params:
        - slug_list: list[str]. List of slug (or Code) for Manga
    """
    # Init
    db_auth = Secret.load("scraper-db-auth").get()
    db_conn = Variable.get("scraper_db_conn")
    db = PostgreAdapter(**db_auth, **db_conn)

    job_id = gen_job_id()

    # Task: scraping_manga
    overviews = scraping_manga_overview.map(slug_list).result()
    
    # Task: load_mangas
    mangas = load_mangas(db, overviews, job_id)

    # Task: log_scraper
    load_log(db, "scraper-overviews", "mangabats_manga_scraper_overview", job_id, wait_for=[mangas])


# Tasks
@task(retries=0)
def scraping_manga_overview(slug: str) -> RawManga:
    """
    Task: Scraping Manga per slug

    params:
        - slug: str. Slug (or Code) for Overview
    
    return:
        Manga scraping results
    """
    # Scraping
    scraper = MangabatsScraperRunner()
    overview = scraper.scrape_overview(slug)

    # Re-formatting
    author_list = ";".join(sorted(overview.authors))
    genre_list = ";".join(sorted(overview.genres))
    is_completed = "TRUE" if (overview.is_completed) else "FALSE"

    m = RawManga.model_validate({
        "author_list": author_list,
        "genre_list": genre_list,
        "is_completed": is_completed,
        **overview.model_dump(include=["code", "title"])
    })
    return m


@task(retries=0)
def load_mangas(db: PostgreAdapter, mangas: List[RawManga], job_id: str):
    """
    Task: Loading Manga Chapters

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - mangas: list[Manga]. List of Manga data object
        - job_id: str. Job generated ID
    """
    query = get_query(QUERY_DIR, "load_mangas.sql")
    data = [{"job_id": job_id, **m.model_dump()} for m in mangas]
    _ = db.run_query(query, data)


# Runtime
if __name__ == "__main__":
    main(
        slug_list = [
            "juujika-no-rokunin",
            "blue-lock"
        ]
    )
