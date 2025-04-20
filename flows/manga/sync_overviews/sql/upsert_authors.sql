INSERT INTO manga.authors (
  "name",
  created_at,
  modified_at,
  job_id
)
VALUES (%(name)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s)
ON CONFLICT("name") DO
  UPDATE SET
    "name" = EXCLUDED."name",
    modified_at = EXCLUDED.modified_at,
    job_id = EXCLUDED.job_id
RETURNING
  id, "name";
