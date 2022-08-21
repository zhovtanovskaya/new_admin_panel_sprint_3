"""Основной модуль для импорта кино из PostgreSQL в ElasticSearch."""

import logging
import time

from elasticsearch.exceptions import ConnectionError
from psycopg2 import OperationalError

import settings
from availability.backoff import backoff
from extract.postgres_genre_loader import PostgresGenreLoader
from extract.postgres_loader import PostgresLoader, postgres_connection
from extract.postgres_movie_loader import PostgresMovieLoader
from extract.postgres_person_loader import PostgresPersonLoader
from load.elastic_search_saver import (ElasticSearchSaver,
                                       elastic_search_connection)
from logger import logger
from storage import JsonFileStorage, State

logging.basicConfig(**logger.settings)

def load(
        loader: PostgresLoader, saver: ElasticSearchSaver,
        ) -> None:
    """Для каждой строки фильма из PostgreSQL создать документ в ElasticSearch.

    Args:
        loader: загрузчик фильмов из PostgreSQL.
        saver: загрузчик фильмов в ElasticSearch.
    """
    validator = loader.validator
    for row in loader.load_all():
        obj = validator(**row)
        saver.save(obj.as_document())
        if saver.is_batch_ready():
            saver.flush()
    saver.flush()


@backoff((OperationalError,))
@backoff((ConnectionError,))
def etl() -> None:
    """Инициировать загрузку фильмов из PostgreSQL в ElasticSearch."""
    logging.info(f'Initializing postgresql and elasticsearch connection.')

    with (
        postgres_connection(settings.POSTGRES_DB) as pg_conn,
        elastic_search_connection(settings.ELASTIC_HOST) as es_client,
    ):
        index_loaders = (
            PostgresMovieLoader,
            PostgresGenreLoader,
            PostgresPersonLoader,
        )

        state = State(JsonFileStorage(settings.STATE_FILE))

        for index_loader in index_loaders:
            loader = index_loader(pg_conn, state)
            logging.info(f'Started {loader.es_index} extraction.')
            saver = ElasticSearchSaver(es_client, loader.es_index)
            logging.info(f'Started {loader.es_index} loading.')
            load(loader, saver)


if __name__ == '__main__':
    while True:
        etl()
        time.sleep(settings.ETL_TIMEOUT)
