INSERT INTO manga_src.raw_mangas (
  code,
  title,
  author_list,
  genre_list,
  is_completed,
  created_at,
  modified_at,
  job_id
)
VALUES (%(code)s, %(title)s, %(author_list)s, %(genre_list)s, %(is_completed)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s);