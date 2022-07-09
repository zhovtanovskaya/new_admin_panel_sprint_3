"""Python-представление данных о фильмах."""

import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime

from dateutil.parser import parse


class DBData:
    """Базовый класс для объектного представления таблиц БД."""

    def __post_init__(self):
        """Сделать изменения после инициализации."""
        # Привести к типу `datetime` строковые значения
        # полей с датами.  Указания типа поля `datetime`
        # в dataclass'е не достаточно.
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
    creation_date: str
    genres: list[str]
    persons: list[dict]
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)
    file_path: str = ''
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)