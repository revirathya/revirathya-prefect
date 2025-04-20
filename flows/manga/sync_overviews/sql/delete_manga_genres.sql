DELETE FROM manga.manga_genres
WHERE manga_id IN %(manga_id)s;
