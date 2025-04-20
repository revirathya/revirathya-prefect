INSERT INTO manga_src.log_scrapers (
    job_service,
    job_name,
    job_id,
    job_at
)
VALUES (%(job_service)s, %(job_name)s, %(job_id)s, %(job_at)s);
