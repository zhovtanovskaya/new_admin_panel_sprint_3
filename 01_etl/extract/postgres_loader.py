"""Подключение и чтение из PostgreSQL."""

from collections.abc import Iterable
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import RealDictCursor, RealDictRow
from storage import State


class StateKeys:
    """Ключи, которые содержат состояния загрузки данных в ElasticSearch."""

    FILM_WORK = 'film_work_since'
    PERSON = 'person_work_since'
    GENRE = 'genre_since'


def create_connection(dsl: dict) -> pg_connection:
    """Создать подключение к базе PostgreSQL.

    Args:
        dsl: Настройки подключения к базе данных.

    Returns:
        Подключение к PostgreSQL.
    """
    connection = psycopg2.connect(**dsl, cursor_factory=RealDictCursor)
    connection.set_session(autocommit=True)
    return connection


@contextmanager
def postgres_connection(dsl: dict) -> pg_connection:
    """Создает подключение к PostgreSQL, которое закроет на выходе.

    Args:
        dsl: Настройки подключения к базе данных.

    Yields:
        Подключение к PostgreSQL.
    """
    connection = create_connection(dsl)
    yield connection
    connection.close()


class PostgresLoader:
    """Класс, загружающий фильмы из PostgreSQL."""

    EPOCH = '1970-01-01'

    def __init__(self, connection: pg_connection, state: State):
        """Проинициализировать соединение и состояние.

        Args:
            connection: Подключение к PostgreSQL.
            state: Хранилище, для сохранения состояния импорта фильмов.
        """
        self.connection = connection
        self.state = state

    def load_all(self) -> RealDictRow:
        """Получить все обновленные и новые фильмы.

        Yields:
            Строка базы данных с полной информацией о фильме.
        """
        genre_since = self.state.get_state(StateKeys.GENRE) or self.EPOCH
        for ids, genre_since in self.ids_for_genre_since(genre_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.GENRE, genre_since)
        person_since = self.state.get_state(StateKeys.PERSON) or self.EPOCH
        for ids, person_since in self.ids_for_person_since(person_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.PERSON, person_since)
        film_work_since = self.state.get_state(StateKeys.FILM_WORK) or self.EPOCH
        for ids, film_work_since in self.ids_for_film_work_since(film_work_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.FILM_WORK, film_work_since)

    def ids_for_film_work_since(self, since: str = EPOCH) -> (list[str], str):
        """Получить ID фильмов, отредактированных с указанного момента.

        Args:
            since: Получить фильмы, измененные после since.

        Yields:
            Список ID фильмов и самое раннее время правки этих фильмов.
        """
        sql = """
            SELECT
                fw.id,
                fw.modified
            FROM film_work fw
            WHERE fw.modified >= %s
            ORDER BY fw.modified, fw.id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_genre_since(self, since: str = EPOCH) -> (list[str], str):
        """Получить ID фильмов, у которых изменился жанр.

        Args:
            since: Получить фильмы, жанры которых изменены после since.

        Yields:
            Список ID фильмов и самое раннее время правки жанра этих фильмов.
        """
        sql = """
            SELECT
                gfw.film_work_id,
                min(g.modified) min_modified
            FROM genre g
            INNER JOIN genre_film_work gfw ON g.id = gfw.genre_id
            WHERE g.modified >= %s
            GROUP BY gfw.film_work_id
            ORDER BY min_modified, gfw.film_work_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_person_since(self, since: str = EPOCH) -> (list[str], str):
        """Получить ID фильмов, у которых изменились персоны.

        Args:
            since: Получить фильмы, персоны которых изменены после since.

        Yields:
            Список ID фильмов и самое раннее время правки персон этих фильмов.
        """
        sql = """
            SELECT
                pfw.film_work_id,
                min(p.modified) min_modified
            FROM person p
            INNER JOIN person_film_work pfw ON p.id = pfw.person_id
            WHERE p.modified >= %s
            GROUP BY pfw.film_work_id
            ORDER BY min_modified, pfw.film_work_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def get_film_works(self, ids: list[str]) -> RealDictRow:
        """Получить фильмы с указанными ID.

        Args:
            ids: Список ID фильмов.

        Yields:
            Полная информация о фильме в виде строки БД.
        """
        sql = """
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'role', pfw.role,
                           'id', p.id,
                           'name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
                ) as persons,
                json_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s
            GROUP BY fw.id
            ORDER BY fw.modified;
        """
        values = (tuple(ids),)
        rows = self._execute_sql(sql, values)
        yield from rows

    def _execute_sql(
            self, sql: str, values: tuple, fetch_size: int = 1,
            ) -> RealDictRow:
        """Запустить SQL.

        Args:
            sql: SQL-выражение.
            values: Значения для вставки в SQL-выражение.
            fetch_size: По сколько фильмов выбирать из SQL-запроса за раз.

        Yields:
            Строка результата SQL.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, values)
            while rows := cursor.fetchmany(fetch_size):
                for row in rows:
                    yield row

    def _bunchify(
            self, rows: Iterable[RealDictRow], bunch_size: int = 100,
            ) -> list[RealDictRow]:
        """Связать строки списка в подсписки указанного размера.

        Args:
            rows: Итератор из строк.
            bunch_size: Размер возвращаемого списка.

        Yields:
            Подсписок строк.
        """
        bunch = []
        for row in rows:
            bunch.append(row)
            if len(bunch) >= bunch_size:
                yield bunch
                bunch = []
        if bunch:
            yield bunch

    def _split_bunch(self, bunches: Iterable[list]) -> (list[str], str):
        """Выделить ID и дату модификации из связок строк БД.

        Args:
            bunches: Iterable связки строк БД.

        Yields:
            Список ID фильмов и минимальная дата модификации для них.
        """
        for bunch in bunches:
            fw_ids, modified_dates = zip(*(row.values() for row in bunch))
            since = modified_dates[0]
            yield fw_ids, since
