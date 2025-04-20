SELECT
  id,
  code,
  title,
  is_completed
FROM manga.mangas
WHERE code IN %(code)s;
