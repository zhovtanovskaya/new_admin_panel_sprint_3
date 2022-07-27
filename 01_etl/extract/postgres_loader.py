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
def postgres_connection(dsl: dict):
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

    def __init__(self, connection: pg_connection):
        """Проинициализировать соединение.

        Args:
            connection: Подключение к PostgreSQL.
        """
        self.connection = connection

    def _execute_sql(
            self, sql: str, values: tuple, fetch_size: int = 50,
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
        """Получить фильм из PostgreSQL.

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
        for row in rows:
            yield row