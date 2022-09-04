"""Python-представление данных о фильмах."""

from typing import Optional
import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime

from dateutil.parser import parse
from pydantic import BaseModel


class DBData:
    """Базовый класс для объектного представления таблиц БД."""

    def __post_init__(self) -> None:
        """Сделать изменения после инициализации."""
        # Привести к типу `datetime` строковые значения
        # полей с датами.  Указания типа поля `datetime`
        # в dataclass'е недостаточно.
        for own_field in fields(type(self)):
            if own_field.type == datetime:
                value = getattr(self, own_field.name)
                if isinstance(value, str):
                    setattr(self, own_field.name, parse(value))


@dataclass
class FilmWork(DBData):
    """Объектное представление строк таблицы film_work."""

    title: str
    description: str
    type: str
    genres: list[str]
    persons: list[dict]
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self) -> None:
        """Проинициализировать атрибуты фильма, нужные для ElasticSearch."""
        super().__post_init__()
        # Разделить self.persons на списки имен и объектов по ролям персон.
        role_to_names_map = {
            'director': 'director',
            'actor': 'actors_names',
            'writer': 'writers_names',
        }
        role_to_objs_map = {
            'actor': 'actors',
            'writer': 'writers',
        }
        # Заполнить списки.
        for person in self.persons:
            role = person['role']
            # Заполнить списки имен.
            name_attr = role_to_names_map[role]
            self._append(name_attr, person['name'])
            # Заполнить списки вложенных объектов.
            objs_attr = role_to_objs_map.get(role)
            if objs_attr:
                obj = {
                    'id': person['id'],
                    'name': person['name'],
                }
                self._append(objs_attr, obj)

    def _append(self, list_attr: str, obj) -> None:
        """Пополнить или проинициализировать атрибут-список."""
        if not hasattr(self, list_attr):
            setattr(self, list_attr, [])
        getattr(self, list_attr).append(obj)

    def as_document(self) -> dict:
        """Получить фильм как документ для индекса ElasticSearch.

        Returns:
            Документ ElasticSearch в виде dict.
        """
        doc_mapping = {
            'id': 'id',
            'imdb_rating': 'rating',
            'genre': 'genres',
            'title': 'title',
            'description': 'description',
            'director': 'director',
            'actors_names': 'actors_names',
            'writers_names': 'writers_names',
            'actors': 'actors',
            'writers': 'writers',
        }
        doc = {}
        for doc_attr, obj_attr in doc_mapping.items():
            doc[doc_attr] = getattr(self, obj_attr, [])
        return doc


class Genre(BaseModel):
    """Объектное представление строк таблицы genre."""

    id: uuid.UUID
    name: str
    description: Optional[str] = ''

    film_ids: list[str] = []

    def as_document(self) -> dict:
        """Костыль, чтобы использовать параллельно с dataclsses."""
        return self.dict()


class Person(BaseModel):
    """Объектное представление строк таблицы genre."""

    id: uuid.UUID
    full_name: str

    role: list[str] = []
    film_ids: list[str] = []

    def as_document(self) -> dict:
        """Костыль, чтобы использовать параллельно с dataclsses."""
        return {
            'id': self.id,
            'name': self.full_name,
            'role': self.role,
            'film_ids': self.film_ids,
        }
