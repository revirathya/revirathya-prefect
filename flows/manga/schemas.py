from typing import Optional
from datetime import datetime

from pydantic import BaseModel, ConfigDict


# SRC
class RawManga(BaseModel):
    code: str
    title: str
    author_list: str
    genre_list: str
    is_completed: str


class RawMangaChapter(BaseModel):
    code: str
    chapter_title: str
    chapter_url: str
    chapter_updated_at: str


# DB
class Manga(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Optional[int] = None
    code: str
    title: str
    is_completed: bool


class Author(BaseModel):
    id: Optional[int] = None
    name: str


class MangaAuthor(BaseModel):
    manga_id: int
    author_id: int


class Genre(BaseModel):
    id: Optional[int] = None
    name: str


class MangaGenre(BaseModel):
    manga_id: int
    genre_id: int


class MangaChapter(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    id: Optional[int] = None
    manga_id: Optional[int] = None
    chapter_title: str
    chapter_url: str
    chapter_updated_at: datetime
