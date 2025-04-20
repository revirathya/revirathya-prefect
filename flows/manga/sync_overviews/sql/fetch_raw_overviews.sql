SELECT
  id,
  code,
  title,
  author_list,
  genre_list,
  is_completed
FROM manga_src.raw_mangas
WHERE job_id IN %(job_id)s;
