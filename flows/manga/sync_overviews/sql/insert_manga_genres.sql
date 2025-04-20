INSERT INTO manga.manga_genres (
    manga_id,
    genre_id,
    created_at,
    modified_at,
    job_id
)
VALUES (%(manga_id)s, %(genre_id)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s)
RETURNING
    manga_id, genre_id;
