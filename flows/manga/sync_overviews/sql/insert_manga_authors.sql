INSERT INTO manga.manga_authors (
    manga_id,
    author_id,
    created_at,
    modified_at,
    job_id
)
VALUES (%(manga_id)s, %(author_id)s, CURRENT_TIMESTAMP AT TIME ZONE 'WAST', CURRENT_TIMESTAMP AT TIME ZONE 'WAST', %(job_id)s)
RETURNING
    manga_id, author_id;
