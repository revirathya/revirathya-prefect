INSERT INTO manga.mangas (
  code,
  title,
  is_completed,
  created_at,
  modified_at,
  job_id
)
VALUES (%(code)s, %(title)s, %(is_completed)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s)
ON CONFLICT(code) DO
  UPDATE SET
    title = EXCLUDED.title,
    is_completed = EXCLUDED.is_completed,
    modified_at = EXCLUDED.modified_at,
    job_id = EXCLUDED.job_id
RETURNING
  id, code, title, is_completed;
