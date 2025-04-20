import os
import itertools
from typing import List

from revi_toolbox.adapters import PostgreAdapter
from revi_toolbox.scraper.manga import MangabatsScraperRunner

from prefect import flow, task

from prefect.variables import Variable
from prefect.blocks.system import Secret

from shared.scraper_logs import load_log
from shared.macro import gen_job_id, get_query

from flows.manga.schemas import RawMangaChapter


QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")


# Flow
@flow(
    name = "manga_mangabats_scraper_chapters",
    log_prints = True
)
def main(slug_list: list[str]):
    """
    Flow: Running Scraper for MangaBats Manga Chapters

    params:
        - slug_list: list[str]. List of slug (or Code) for Manga
    """
    # Init
    db_auth = Secret.load("scraper-db-auth").get()
    db_conn = Variable.get("scraper_db_conn")
    db = PostgreAdapter(**db_auth, **db_conn)

    job_id = gen_job_id()

    # Task: scraping_manga
    chapters = scraping_manga_chapters.map(slug_list).result()
    flt_chapters = list(itertools.chain(*chapters))
    
    # Task: load_mangas
    manga_chapters = load_manga_chapters(db, flt_chapters, job_id)

    # Task: log_scraper_runtime
    load_log(db, "scraper-chapters", "mangabats_manga_scraper_chapters", job_id, wait_for=[manga_chapters])


# Tasks
@task(retries=0)
def scraping_manga_chapters(slug: str) -> List[RawMangaChapter]:
    """
    Task: Scraping Manga Chapters per slug

    params:
        - slug: str. Slug (or Code) for Manga
    
    return:
        Manga scraping results
    """
    # Scraping
    scraper = MangabatsScraperRunner()
    chapters = scraper.scrape_chapters(slug)

    # Re-formatting
    ch_list = []
    for chapter in chapters:
        chapter_updated_at = chapter.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        ch  = RawMangaChapter.model_validate({
            "chapter_updated_at": chapter_updated_at,
            **chapter.model_dump(include=["code", "chapter_title", "chapter_url"])
        })
        ch_list.append(ch)

    return ch_list


@task
def load_manga_chapters(db: PostgreAdapter, manga_chapters: List[RawMangaChapter], job_id: str):
    """
    Task: Loading Manga Chapters

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - manga_chapters: list[RawMangaChapter]. List of manga chapters data object
        - job_id: str. Job generated ID
    """
    # Prepare Query and Params
    query = get_query(QUERY_DIR, "load_manga_chapters.sql")
    data = [{"job_id": job_id, **m.model_dump()} for m in manga_chapters]
    
    # Run Query
    _ = db.run_query(query, data)


# Runtime
if __name__ == "__main__":
    main(
        slug_list = [
            "juujika-no-rokunin",
            "blue-lock"
        ]
    )
