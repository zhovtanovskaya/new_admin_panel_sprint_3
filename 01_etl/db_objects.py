"""Python-представление данных о фильмах."""

import uuid
from dataclasses import dataclass, field, fields
from datetime import datetime
from itertools import chain

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
    genres: list[str]
    persons: list[dict]
    created: datetime = field(default_factory=datetime.now)
    modified: datetime = field(default_factory=datetime.now)
    file_path: str = ''
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __post_init__(self):
        super().__post_init__()
        # Разделить self.persons на списки имен и объектов по ролям персон.
        role_to_names_map = {
            'director': 'director',
            'actor': 'actors_names',
            'writer': 'writers_names'
        }
        role_to_objs_map = {
            'actor': 'actors',
            'writer': 'writers'
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

    def _append(self, list_attr, obj):
        if not hasattr(self, list_attr):
            setattr(self, list_attr, [])
        getattr(self, list_attr).append(obj)

