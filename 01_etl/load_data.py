"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import settings
from db_objects import FilmWork
from elastic_search_saver import ElasticSearchSaver, elastic_search_connection
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from postgres_loader import PostgresLoader, postgres_connection
from psycopg2.extensions import connection as pg_connection
from storage import JsonFileStorage, State


def load(
        pg_conn: pg_connection, es_client: Elasticsearch, state: State,
        ) -> None:
    """Основной метод загрузки данных из SQLite в Postgres.

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
        try:
            saver.save(doc)
        except (RequestError, KeyboardInterrupt) as e:
            print(e)
            import pprint
            pprint.pprint(doc)
            break
        else:
            state.set_state(SINCE_KEY, obj.modified)
        break


if __name__ == '__main__':
    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
        elastic_search_connection(settings.ELASTIC_HOST) as es_client,
    ):
        storage = JsonFileStorage(settings.STATE_FILE)
        load(pg_conn, es_client, State(storage))
