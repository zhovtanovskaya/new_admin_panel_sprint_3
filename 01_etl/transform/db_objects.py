"""Python-представление данных о фильмах."""

from typing import Optional
import uuid
# from dataclasses import dataclass, field, fields
# from datetime import datetime

# from dateutil.parser import parse
from pydantic import BaseModel


class GenreBase(BaseModel):
    """Базовое представление строк таблицы genre."""

    id: uuid.UUID
    name: str


class Genre(GenreBase):
    """Объектное представление строк таблицы genre."""

    description: Optional[str] = ''

    film_ids: list[str] = []


class PersonBase(BaseModel):
    """Базовое представление строк таблицы genre."""

    id: uuid.UUID
    name: str


class FilmWork(BaseModel):
    """Объектное представление строк таблицы film_work."""

    id: uuid.UUID
    title: str
    imdb_rating: Optional[float] = 0
    description: Optional[str] = ''

    genres_names: list[str] = []
    actors_names: list[str] = []
    directors_names: list[str] = []
    writers_names: list[str] = []

    genres: list[GenreBase] = []
    actors: list[PersonBase] = []
    directors: list[PersonBase] = []
    writers: list[PersonBase] = []
