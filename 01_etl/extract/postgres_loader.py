"""Подключение и чтение из PostgreSQL."""

from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import RealDictCursor, RealDictRow


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

    def __init__(self, connection: pg_connection, state):
        """Проинициализировать соединение.

        Args:
            connection: Подключение к PostgreSQL.
        """
        self.connection = connection
        self.state = state

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

    def load(self, since: str = EPOCH) -> RealDictRow:
        """Получить фильм из PostgreSQL один за другим.

        Args:
            since: Получить строки, у которых дата правки больше строго since.

        Yields:
            Строка, представляющая фильм с жанрами и персонами.
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
            WHERE fw.modified > %s
            GROUP BY fw.id
            ORDER BY fw.modified;
        """
        values = (since or self.EPOCH,)
        rows = self._execute_sql(sql, values)
        yield from rows

    def load_all(self):
        genre_since = self.state.get_state('genre_since') or self.EPOCH
        for ids, genre_since in self.ids_for_genre_since(genre_since):
            yield from self.get_film_works(ids)
            self.state.set_state('genre_since', genre_since)
            print('genre_since', genre_since)
        person_since = self.state.get_state('person_since') or self.EPOCH
        for ids, person_since in self.ids_for_person_since(person_since):
            yield from self.get_film_works(ids)
            self.state.set_state('person_since', person_since)
            print('person_since', person_since)
        film_work_since = self.state.get_state('film_work_since') or self.EPOCH
        for ids, film_work_since in self.ids_for_film_work_since(film_work_since):
            yield from self.get_film_works(ids)
            self.state.set_state('film_work_since', film_work_since)
            print('film_since', film_work_since)

    def _rows_for_film_work_since(
        self, since: str = EPOCH, bunch_size: int = 100,
    ) -> list[RealDictRow]:
        sql = """
            SELECT
                fw.id,
                fw.modified
            FROM film_work fw
            WHERE fw.modified >= %s
            ORDER BY fw.modified, fw.id;
        """
        values = (since,)
        yield from self._bunchify(self._execute_sql(sql, values))

    def _rows_for_genre_since(
        self, since: str = EPOCH, bunch_size: int = 100,
    ) -> list[RealDictRow]:
        """
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
        yield from self._bunchify(self._execute_sql(sql, values))

    def _rows_for_person_since(
        self, since: str = EPOCH,
    ) -> list[RealDictRow]:
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
        yield from self._bunchify(self._execute_sql(sql, values))

    def ids_for_genre_since(self, since):
        gen = self._rows_for_genre_since(since)
        yield from self._split_rows(gen)

    def ids_for_person_since(self, since):
        gen = self._rows_for_person_since(since)
        yield from self._split_rows(gen)

    def ids_for_film_work_since(self, since):
        gen = self._rows_for_film_work_since(since)
        yield from self._split_rows(gen)

    def _split_rows(self, bunches):
        for bunch in bunches:
            fw_ids, modified_dates = zip(*(row.values() for row in bunch))
            genre_since = modified_dates[0]
            yield fw_ids, genre_since

    def _bunchify(self, gen, bunch_size: int = 100):
        bunch = []
        for row in gen:
            bunch.append(row)
            if len(bunch) >= bunch_size:
                yield bunch
                bunch = []
        if bunch:
            yield bunch

    def get_film_works(self, ids):
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


