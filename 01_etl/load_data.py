"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import time
from elasticsearch.exceptions import ConnectionError
from psycopg2 import OperationalError

import settings
from availability.backoff import backoff
from extract.postgres_loader import PostgresLoader, postgres_connection
from load.elastic_search_saver import (ElasticSearchSaver,
                                       elastic_search_connection)
from storage import JsonFileStorage, State
from transform.db_objects import FilmWork


def load(
        loader: PostgresLoader, saver: ElasticSearchSaver,
        ) -> None:
    """Для каждой строки фильма из PostgreSQL создать документ в ElasticSearch.

    Args:
        loader: загрузчик фильмов из PostgreSQL.
        saver: загрузчик фильмов в ElasticSearch.
    """
    for row in loader.load_all():
        obj = FilmWork(**row)
        saver.save(obj.as_document())
        if saver.is_batch_ready():
            saver.flush()
    saver.flush()


@backoff((OperationalError,))
@backoff((ConnectionError,))
def etl() -> None:
    """Инициировать загрузку фильмов из PostgreSQL в ElasticSearch."""
    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
        elastic_search_connection(settings.ELASTIC_HOST) as es_client,
    ):
        state = State(JsonFileStorage(settings.STATE_FILE))
        loader = PostgresLoader(pg_conn, state)
        saver = ElasticSearchSaver(es_client)
        load(loader, saver)


if __name__ == '__main__':
    while True:
        etl()
        time.sleep(settings.ETL_TIMEOUT)
