INSERT INTO manga.manga_chapters (
  manga_id,
  chapter_title,
  chapter_url,
  chapter_updated_at,
  created_at,
  modified_at,
  job_id
)
VALUES (%(manga_id)s, %(chapter_title)s, %(chapter_url)s, %(chapter_updated_at)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s)
ON CONFLICT (manga_id, chapter_url) DO
  UPDATE SET
    chapter_title = EXCLUDED.chapter_title,
    chapter_updated_at = EXCLUDED.chapter_updated_at,
    modified_at = EXCLUDED.modified_at,
    job_id = EXCLUDED.job_id
RETURNING
  id,
  manga_id,
  chapter_title,
  chapter_urL,
  chapter_updated_at;
