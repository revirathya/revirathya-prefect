import os
from typing import List

from revi_toolbox.adapters import PostgreAdapter

from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from shared.macro import gen_job_id, get_query, parse_job_id, remove_duplicate
from shared.scraper_logs import fetch_new_job_id, mark_job_id

from flows.manga.schemas import Manga, MangaChapter, RawMangaChapter

QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")


# Flow
@flow(
    name = "manga_sync_chapters",
    log_prints = True
)
def main(scraper_job_id: List[str]):
    """
    Flow: Running Sync task to Update/Insert Manga Chapters information

    params:
        - scraper_job_id: list[int]. List of Scraper Job ID to be processed
    """
    # Init
    db_auth = Secret.load("scraper-db-auth").get()
    db_conn = Variable.get("scraper_db_conn")
    db = PostgreAdapter(**db_auth, **db_conn)

    job_id = gen_job_id()

    # Task: fetch_new_job_id
    scraper_job_id = scraper_job_id if (scraper_job_id) else fetch_new_job_id(db, "scraper-chapters")
    if not (scraper_job_id):
        return
    
    # Task: fetch_raw_chapters
    raw_chapters = fetch_raw_chapters(db, scraper_job_id)
    if (not raw_chapters):
        return
    
    # Task: fetch_mangas
    mangas = fetch_mangas_by_code(db, raw_chapters)

    # Task: sync_manga_chapters
    manga_chapters = sync_manga_chapters(db, raw_chapters, mangas, job_id)

    # Task: mark_is_complete
    processed_at = parse_job_id(job_id)
    mark_job_id(db, "scraper-chapters", scraper_job_id, processed_at, wait_for=[manga_chapters])


# Tasks
@task(retries=0)
def fetch_raw_chapters(db: PostgreAdapter, scraper_job_id: List[str]) -> List[RawMangaChapter]:
    """
    Task: Fetch Raw Manga Chapter scraping results

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - scraper_job_id: list[str]. List of Scraper Job ID to be processed.
    
    return:
        List of RawMangaChapter objects
    """
    print(f"Fetch Chapters data with job_id: [{', '.join(scraper_job_id)}]")

    # Prepare Query and Params
    query = get_query(QUERY_DIR, "fetch_raw_chapters.sql")
    params = {"job_id": tuple(scraper_job_id)}

    # Fetch Results
    chapters = [RawMangaChapter.model_validate(r) for r in db.run_query(query, params)]
    print(f"Collected {len(chapters)} records")
    return chapters


@task(retries=0)
def fetch_mangas_by_code(db: PostgreAdapter, chapters: List[RawMangaChapter]) -> List[Manga]:
    """
    Task: Fetch Mangas based on chapters Manga code

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - chapters: list[RawMangaChapter]. List of raw Manga Chapter object
    
    return:
        List of Manga object
    """
    print("Fetch Mangas")

    # Prepare Query and Params
    query = get_query(QUERY_DIR, "fetch_mangas_by_code.sql")
    params = {"code": tuple(map(lambda c: c.code, chapters))}

    # Fetch Results
    mangas = [Manga.model_validate(r) for r in db.run_query(query, params)]
    print(f"Collected {len(mangas)} Manga records")
    return mangas


@task(retries=0)
def sync_manga_chapters(db: PostgreAdapter, chapters: List[RawMangaChapter], mangas: List[Manga], job_id: str) -> List[MangaChapter]:
    """
    Task: Sync Manga Chapters data (manga.manga_chapters) with new chapters data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - chapters: list[RawMangaChapter]. List of raw Manga Chapter object
        - mangas: list[Manga]. List of Manga object
        - job_id: str. Data processing Job ID

    returns:
        Updated/Loaded Manga Chapter data (unloaded data have manga_id = None)
    """
    print("Sync Manga Chapters data")

    # Mapping Chapters with Manga 
    mapped_raw_chapters = []
    for ch in chapters:
        fltr_manga_id = list(filter(lambda m: m.code == ch.code, mangas))
        manga_id = fltr_manga_id[0].id if (fltr_manga_id) else None
        __mrc = {
            "manga_id": manga_id,
            "job_id": job_id,
            **ch.model_dump()
        }
        mapped_raw_chapters.append(__mrc)

    mapped_raw_chapters = remove_duplicate(mapped_raw_chapters, unique_key=["manga_code", "chapter_url"])

    # Upsert Manga Chapters (defined)
    def_manga_chapters = list(filter(lambda m: m["manga_id"] is not None, mapped_raw_chapters))
    ups_def_query = get_query(QUERY_DIR, "upsert_manga_chapters.sql")
    def_manga_chapters_upt = [MangaChapter.model_validate(r) for r in db.run_query(ups_def_query, def_manga_chapters)]

    # Upsert Manga Chapter (undefined)
    udf_manga_chapters_upt = []
    udf_manga_chapters = list(filter(lambda m: m["manga_id"] is None, mapped_raw_chapters))
    if (udf_manga_chapters):
        print("Sync Undefined Manga Chapters")
        ups_udf_query = get_query(QUERY_DIR, "upsert_undefined_manga_chapters.sql")
        udf_manga_chapters_upt = [MangaChapter.model_validate(r) for r in db.run_query(ups_udf_query, udf_manga_chapters)]

    manga_chapters = def_manga_chapters_upt + udf_manga_chapters_upt
    print(f"Finish Sync {len(manga_chapters)} record(s) ({len(udf_manga_chapters_upt)} record(s) with undefined Manga)")
    return manga_chapters
