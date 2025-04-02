from revi_toolbox.scraper.manga import MangabatsScraperRunner
from prefect import flow, task


# Flow
@flow(log_prints=True)
def main(slug_list: list[str]):
    """
    -- Flow --
    Running Scraper for MangaBats

    params:
        - slug_list: list[str]. Kist of slug (or Code) for Manga
    """
    for slug in slug_list:
        # Task: scraping_manga
        results = scraping_manga(slug)
        print(results)


# Tasks
@task
def scraping_manga(slug: str) -> dict:
    """
    --- Task ---
    Scraping Manga per slug

    params:
        - slug: str. Slug (or Code) for Manga
    
    return:
        Manga scraping results
    """
    # Scraping Chapters
    scraper = MangabatsScraperRunner()
    chapters = [ch.model_dump_json() for ch in scraper.scrape_chapters(slug)]

    return chapters


# Deployment
if __name__ == "__main__":
    from prefect.docker import DockerImage

    main.deploy(
        name = "manga_scraper_mangabats",
        work_pool_name = "docker-pool",
        parameters = {
            "slug_list": [
                "juujika-no-rokunin",
                "blue-lock",
                "shuumatsu-no-valkyrie",
                "smiley"
            ]
        },
        image = DockerImage(
            name = "avidito/revirathya-prefect",
            tag = "3-0.1.0",
            dockerfile = "Dockerfile"
        ),
        build = False,
        push = False
    )
