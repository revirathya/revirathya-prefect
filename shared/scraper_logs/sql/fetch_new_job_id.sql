SELECT job_id
FROM manga_src.log_scrapers
WHERE is_processed IS FALSE
  AND job_service = %(job_service)s;
