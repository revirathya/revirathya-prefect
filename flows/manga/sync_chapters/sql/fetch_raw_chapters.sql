SELECT
  code,
  chapter_title,
  chapter_url,
  chapter_updated_at
FROM manga_src.raw_manga_chapters
WHERE job_id IN %(job_id)s;
