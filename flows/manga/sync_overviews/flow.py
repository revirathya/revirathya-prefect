import os
from typing import Dict, List

from revi_toolbox.adapters import PostgreAdapter

from prefect import flow, task
from prefect.variables import Variable
from prefect.blocks.system import Secret

from shared.macro import gen_job_id, get_query, parse_job_id, remove_duplicate
from shared.scraper_logs import fetch_new_job_id, mark_job_id

from flows.manga.schemas import Author, Genre, Manga, MangaAuthor, MangaGenre, RawManga


QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")


# Flow
@flow(
    name = "manga_sync_overviews",
    log_prints = True
)
def main(scraper_job_id: List[str]):
    """
    Flow: Running Sync task to Update/Insert Manga Overviews information

    params:
        - scraper_job_id: list[int]. List of Scraper Job ID to be processed
    """
    # Init
    db_auth = Secret.load("scraper-db-auth").get()
    db_conn = Variable.get("scraper_db_conn")
    db = PostgreAdapter(**db_auth, **db_conn)

    job_id = gen_job_id()

    # Task: fetch_new_job_id
    scraper_job_id = scraper_job_id if (scraper_job_id) else fetch_new_job_id(db, "scraper-overviews")
    if (not scraper_job_id):
        return
    
    # Task: fetch_raw_overviews
    raw_overviews = fetch_raw_overviews(db, scraper_job_id)
    if (not raw_overviews):
        return
    
    # Task: sync_mangas
    mangas = sync_mangas(db, raw_overviews, job_id)
    
    # Task: sync_authors
    authors = sync_authors(db, raw_overviews, job_id)

    # Task: sync_manga_authors
    manga_authors = sync_manga_authors(db, raw_overviews, mangas, authors, job_id)

    # Task: sync_genres
    genres = sync_genres(db, raw_overviews, job_id)

    # Task: sync_manga_genres
    manga_genres = sync_manga_genres(db, raw_overviews, mangas, genres, job_id)

    # Task: mark_scraper_log
    processed_at = parse_job_id(job_id)
    mark_job_id(db, "scraper-overviews", scraper_job_id, processed_at, wait_for=[manga_authors, manga_genres])


# Tasks
@task(retries=0)
def fetch_raw_overviews(db: PostgreAdapter, scraper_job_id: List[str]) -> List[RawManga]:
    """
    Task: Fetch Raw Manga Overview scraping results

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - scraper_job_id: list[str]. List of Scraper Job ID to be processed.
    
    return:
        List of RawManga objects
    """
    print(f"Fetch Overviews data with job_id: [{', '.join(scraper_job_id)}]")
    
    query = get_query(QUERY_DIR, "fetch_raw_overviews.sql")
    params = {
        "job_id": tuple(scraper_job_id)
    }
    mangas = [RawManga.model_validate(r) for r in db.run_query(query, params)]
    
    print(f"Collected {len(mangas)} records")
    return mangas


@task(retries=0)
def sync_mangas(db: PostgreAdapter, overviews: List[RawManga], job_id: str) -> List[Manga]:
    """
    Task: Sync Manga data (manga.mangas) with new overview data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - overview: list[RawManga]. List of Manga overview object
        - job_id: str. Data processing Job ID
    
    return:
        Updated/loaded Mangas
    """
    print("Sync Mangas data")
    
    query = get_query(QUERY_DIR, "upsert_mangas.sql")
    data = remove_duplicate(
        [{"job_id": job_id, **o.model_dump()} for o in overviews],
        unique_key = ["code"]
    )
    mangas = [Manga.model_validate(r) for r in db.run_query(query, data)]
    
    print(f"Finish Sync {len(mangas)} Manga record(s)")
    return mangas


@task(retries=0)
def sync_authors(db: PostgreAdapter, overviews: List[RawManga], job_id: str) -> List[Author]:
    """
    Task: Sync Authors data (manga.authors) with new overview data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - overviews: list[RawManga]. List of Manga overview object
        - job_id: str. Data processing Job ID

    return:
        Updated/loaded Authors
    """
    print("Sync Authors data")

    query = get_query(QUERY_DIR, "upsert_authors.sql")
    data = remove_duplicate(
        [
            {"name": n, "job_id": job_id}
            for m in overviews
            for n in m.author_list.split(";")
        ],
        unique_key = ["name"]
    )
    authors = [Author.model_validate(r) for r in db.run_query(query, data)]
    
    print(f"Finish Sync {len(authors)} Author record(s)")
    return authors


@task(retries=0)
def sync_manga_authors(
    db: PostgreAdapter,
    overviews: List[RawManga],
    mangas: List[Manga],
    authors: List[Author],
    job_id: str
):
    """
    Task: Sync Manga Authors mapping data (manga.manga_authors) with new overview data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - overviews: list[RawManga]. List of Manga overview object
        - mangas: list[Manga]. List of Manga object
        - authors: list[Author]. List of Author object
        - job_id: str. Data processing Job ID
    """
    print("Sync Manga Authors data")

    # Mapping Manga and Authors
    raw_manga_authors: List[Dict[str, str]] = remove_duplicate(
        [
            {"manga_code": o.code, "author_name": a}
            for o in overviews
            for a in o.author_list.split(";")
        ],
        unique_key = ["manga_code", "author_name"]
    )
    mapped_manga_authors = [
        {
            "manga_id": list(filter(lambda m: m.code == ma["manga_code"], mangas))[0].id,
            "author_id": list(filter(lambda a: a.name == ma["author_name"], authors))[0].id,
            "job_id": job_id
        }
        for ma in raw_manga_authors
    ]

    # Delete existing Manga mapping
    manga_ids = {"manga_id": tuple(set(map(lambda d: d["manga_id"], mapped_manga_authors)))}
    del_query = get_query(QUERY_DIR, "delete_manga_authors.sql")
    _ = db.run_query(del_query, manga_ids)

    # Insert Manga Author Mapping
    ins_query = get_query(QUERY_DIR, "insert_manga_authors.sql")
    manga_authors = [MangaAuthor.model_validate(r) for r in db.run_query(ins_query, mapped_manga_authors)]
    
    print(f"Finish Sync {len(manga_authors)} Manga Author record(s)")


@task(retries=0)
def sync_genres(db: PostgreAdapter, overviews: List[RawManga], job_id: str) -> List[Genre]:
    """
    Task: Sync Genres data (manga.genres) with new overview data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - overviews: list[RawManga]. List of Manga overview object
        - job_id: str. Data processing Job ID
    """
    print("Sync Genres data")
    
    query = get_query(QUERY_DIR, "upsert_genres.sql")
    data = remove_duplicate(
        [
            {"name": n, "job_id": job_id}
            for m in overviews
            for n in m.genre_list.split(";")
        ],
        unique_key = ["name"]
    )
    genres = [Genre.model_validate(r) for r in db.run_query(query, data)]
    
    print(f"Finish Sync {len(genres)} Genre record(s)")
    return genres


@task(retries=0)
def sync_manga_genres(
    db: PostgreAdapter,
    overviews: List[RawManga],
    mangas: List[Manga],
    genres: List[Genre],
    job_id: str
):
    """
    Task: Sync Manga Genre mapping data (manga.manga_genres) with new overview data

    params:
        - db: PostgreAdapter. Adapter for interacting with Postgre DB
        - overviews: list[RawManga]. List of Manga overview object
        - mangas: list[Manga]. List of Manga object
        - genres: list[Genre]. List of Genre object
        - job_id: str. Data processing Job ID
    """
    print("Sync Manga Genre data")

    # Mapping Manga and Genres
    raw_manga_genres: List[Dict[str, str]] = remove_duplicate(
        [
            {"manga_code": o.code, "genre_name": g}
            for o in overviews
            for g in o.genre_list.split(";")
        ],
        unique_key = {"manga_code", "genre_name"}
    )
    mapped_manga_genres = [
        {
            "manga_id": list(filter(lambda m: m.code == ma["manga_code"], mangas))[0].id,
            "genre_id": list(filter(lambda a: a.name == ma["genre_name"], genres))[0].id,
            "job_id": job_id
        }
        for ma in raw_manga_genres
    ]

    # Delete existing Manga mapping
    manga_ids = {"manga_id": tuple(set(map(lambda d: d["manga_id"], mapped_manga_genres)))}
    del_query = get_query(QUERY_DIR, "delete_manga_genres.sql")
    _ = db.run_query(del_query, manga_ids)

    # Insert Manga Author Mapping
    ins_query = get_query(QUERY_DIR, "insert_manga_genres.sql")
    manga_genres = [MangaGenre.model_validate(r) for r in db.run_query(ins_query, mapped_manga_genres)]
    
    print(f"Finish Sync {len(manga_genres)} Manga Genre record(s)")


# Runtime
if __name__ == "__main__":
    main(
        scraper_job_id = []
    )