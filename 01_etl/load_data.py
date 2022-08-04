"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import settings
from availability.backoff import backoff
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from extract.postgres_loader import PostgresLoader, postgres_connection
from load.elastic_search_saver import (ElasticSearchSaver,
                                       elastic_search_connection)
from psycopg2 import OperationalError
from psycopg2.extensions import connection as pg_connection
from storage import JsonFileStorage, State
from transform.db_objects import FilmWork


def load(
        pg_conn: pg_connection, es_client: Elasticsearch, state: State,
        ) -> None:
    """Для каждой строки фильма из PostgreSQL создать документ в ElasticSearch.

    Args:
        pg_conn: подключение к базе приемнику данных в PostgreSQL.
        es_client: подключение к ElasticSearch.
        state: файл для хранения состояния синхронизации ElasticSearch.
    """
    SINCE_KEY = 'since'
    loader = PostgresLoader(pg_conn)
    saver = ElasticSearchSaver(es_client)
    for row in loader.load(since=state.get_state(SINCE_KEY)):
        obj = FilmWork(**row)
        doc = obj.as_document()
        saver.save(doc)
        state.set_state(SINCE_KEY, obj.modified)


@backoff((OperationalError,))
@backoff((ConnectionError,))
def etl() -> None:
    """Инициировать загрузку фильмов из PostgreSQL в ElasticSearch."""
    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
        elastic_search_connection(settings.ELASTIC_HOST) as es_client,
    ):
        storage = JsonFileStorage(settings.STATE_FILE)
        load(pg_conn, es_client, State(storage))


if __name__ == '__main__':
    etl()
