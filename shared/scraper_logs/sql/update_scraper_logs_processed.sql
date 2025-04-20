UPDATE manga_src.log_scrapers
SET
  is_processed = TRUE,
  processed_at = %(processed_at)s
WHERE job_service = %(job_service)s
  AND job_id IN %(job_id)s;
