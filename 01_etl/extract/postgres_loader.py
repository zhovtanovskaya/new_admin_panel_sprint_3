"""Подключение и чтение из PostgreSQL."""

import logging
from collections.abc import Iterable
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import RealDictCursor, RealDictRow
from storage import State

EPOCH = '1970-01-01'


def create_connection(dsl: dict) -> pg_connection:
    """Создать подключение к базе PostgreSQL.

    Args:
        dsl: Настройки подключения к базе данных.

    Returns:
        Подключение к PostgreSQL.
    """
    connection = psycopg2.connect(**dsl, cursor_factory=RealDictCursor)
    logging.info('Connected to postgresql.')
    connection.set_session(autocommit=True)
    return connection


@contextmanager
def postgres_connection(dsl: dict) -> Generator[pg_connection, None, None]:
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
    """Класс, загружающий объекты из PostgreSQL."""

    def __init__(self, connection: pg_connection, state: State):
        """Проинициализировать соединение и состояние.

        Args:
            connection: Подключение к PostgreSQL.
            state: Хранилище, для сохранения состояния импорта объектов.
        """
        self.connection = connection
        self.state = state

    def load_all(self) -> Generator[RealDictRow, None, None]:
        pass

    def _execute_sql(
            self, sql: str, values: tuple, fetch_size: int = 1,
            ) -> Generator[RealDictRow, None, None]:
        """Запустить SQL.

        Args:
            sql: SQL-выражение.
            values: Значения для вставки в SQL-выражение.
            fetch_size: По сколько объектов выбирать из SQL-запроса за раз.

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
            ) -> Generator[list[RealDictRow], None, None]:
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

    def _split_bunch(
            self,
            bunches: Generator[list[RealDictRow], None, None]
            ) -> Generator[tuple[tuple[str], str], None, None]:
        """Выделить ID и дату модификации из связок строк БД.

        Args:
            bunches: Iterable связки строк БД.

        Yields:
            Список ID и минимальная дата модификации для них.
        """
        for bunch in bunches:
            ids, modified_dates = zip(*(row.values() for row in bunch))
            since = modified_dates[0]
            yield ids, since
