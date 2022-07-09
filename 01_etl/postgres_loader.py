"""Подключение и чтение из PostgreSQL."""

from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import RealDictCursor


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

    def __init__(self, connection: pg_connection):
        self.connection = connection

    def _execute_sql(self, sql: str, values: tuple) -> None:
        """Запустить SQL.

        Args:
            sql: SQL-выражение.
            values: Значения для вставки в SQL-выражение.

        Raises:
            psycopg2.Error: В случае ошибки при SQL-вызове.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql, values)

    def load(self, fetch_size=50):
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
            GROUP BY fw.id
            ORDER BY fw.modified
            LIMIT 100;
        """
        rows = self._execute_sql(sql, ())
        with self.connection.cursor() as curs:
            curs.execute(sql)
            while data := curs.fetchmany(fetch_size):
                for row in data:
                    yield row
    
