"""Подключение и чтение из PostgreSQL."""

from typing import Generator
from psycopg2.extras import RealDictRow

from transform.db_objects import Person

from .postgres_loader import EPOCH, PostgresLoader


class StateKeys:
    """Ключи, которые содержат состояния загрузки данных в ElasticSearch."""

    FILM_WORK = 'person_film_work_since'
    PERSON = 'person_person_since'


class PostgresPersonLoader(PostgresLoader):
    """Класс, загружающий персон из PostgreSQL."""

    es_index = 'persons'
    validator = Person

    def load_all(self) -> Generator[RealDictRow, None, None]:
        """Получить все обновленные и новые персоны.

        Yields:
            Строка базы данных с информацией о персоне.
        """
        film_work_since = self.state.get_state(StateKeys.FILM_WORK) or EPOCH
        for ids, film_work_since in self.ids_for_film_work_since(film_work_since):
            yield from self.get_persons(ids)
            self.state.set_state(StateKeys.FILM_WORK, film_work_since)

        person_since = self.state.get_state(StateKeys.PERSON) or EPOCH
        for ids, person_since in self.ids_for_person_since(person_since):
            yield from self.get_persons(ids)
            self.state.set_state(StateKeys.PERSON, person_since)

    def ids_for_person_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID персон, отредактированных с указанного момента.

        Args:
            since: Получить персоны, измененные после since.

        Yields:
            Список ID персон и самое раннее время правки этих персон.
        """
        sql = """
            SELECT
                person.id,
                person.modified
            FROM person
            WHERE person.modified >= %s
            ORDER BY person.modified, person.id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_film_work_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID персон у изменённых фильмов.

        Args:
            since: Получить персон по фильмам, изменённым после since.

        Yields:
            Список ID персон и самое раннее время правки фильма этих персон.
        """
        sql = """
            SELECT
                pfw.person_id,
                min(fw.modified) min_modified
            FROM film_work fw
            INNER JOIN person_film_work pfw ON fw.id = pfw.film_work_id
            WHERE fw.modified >= %s
            GROUP BY pfw.person_id
            ORDER BY min_modified, pfw.person_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def get_persons(self, ids: tuple[str]) -> Generator[RealDictRow, None, None]:
        """Получить персон с указанными ID.

        Args:
            ids: Список ID персон.

        Yields:
            Информация о персоне в виде строки БД.
        """
        sql = """
            SELECT
                person.id,
                person.full_name,
                COALESCE(
                    JSON_AGG(DISTINCT pfw.role),
                    '{}'
                ) as role,
                COALESCE(
                    JSON_AGG(DISTINCT fw.id),
                    '{}'
                ) as film_ids
            FROM person
            LEFT JOIN person_film_work pfw ON pfw.person_id = person.id
            LEFT JOIN film_work fw ON fw.id = pfw.film_work_id
            WHERE person.id IN %s
            GROUP BY person.id
            ORDER BY person.modified;
        """
        values = (tuple(ids),)
        rows = self._execute_sql(sql, values)
        yield from rows
