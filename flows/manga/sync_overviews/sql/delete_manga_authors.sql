DELETE FROM manga.manga_authors
WHERE manga_id IN %(manga_id)s;
