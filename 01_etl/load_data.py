"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

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
        loader: PostgresLoader, saver: ElasticSearchSaver, state: State,
        ) -> None:
    """Для каждой строки фильма из PostgreSQL создать документ в ElasticSearch.

    Args:
        loader: загрузчик фильмов из PostgreSQL.
        saver: загрузчик фильмов в ElasticSearch.
        state: файл для хранения состояния синхронизации ElasticSearch.
    """
    SINCE_KEY = 'since'
    for row in loader.load(since=state.get_state(SINCE_KEY)):
        obj = FilmWork(**row)
        saver.save(obj.as_document())
        if saver.is_batch_ready():
            saver.flush()
            state.set_state(SINCE_KEY, obj.modified)
    saver.flush()


@backoff((OperationalError,))
@backoff((ConnectionError,))
def etl() -> None:
    """Инициировать загрузку фильмов из PostgreSQL в ElasticSearch."""
    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
        elastic_search_connection(settings.ELASTIC_HOST) as es_client,
    ):
        loader = PostgresLoader(pg_conn)
        saver = ElasticSearchSaver(es_client)
        storage = JsonFileStorage(settings.STATE_FILE)
        load(loader, saver, State(storage))


if __name__ == '__main__':
    etl()
