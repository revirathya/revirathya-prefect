INSERT INTO manga_src.raw_manga_chapters (
  code,
  chapter_title,
  chapter_url,
  chapter_updated_at,
  created_at,
  modified_at,
  job_id
)
VALUES (%(code)s, %(chapter_title)s, %(chapter_url)s, %(chapter_updated_at)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s);
