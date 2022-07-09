"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import settings
from db_objects import FilmWork
from postgres_loader import PostgresLoader, postgres_connection
from psycopg2.extensions import connection as pg_connection


def load(pg_conn: pg_connection, es_conn=None) -> None:
    """Основной метод загрузки данных из SQLite в Postgres.

    Args:
        sqlite_conn: подключение к базе источкику данных SQLite.
        pg_conn: подключение к базе приемнику данных в PostgreSQL.
    """
    loader = PostgresLoader(pg_conn)
    for row in loader.load():
        obj = FilmWork(**row)


if __name__ == '__main__':
    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
    ):
            load(pg_conn)
