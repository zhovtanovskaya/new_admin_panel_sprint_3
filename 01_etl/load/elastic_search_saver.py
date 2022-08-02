"""Загрузка фильмов в Elastic Search."""

import uuid
from contextlib import contextmanager
from typing import Union

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError


def create_connection(host: dict) -> Elasticsearch:
    """Создать подключение к ElasticSearch.

    Args:
        host: Настройки подключения к базе данных.

    Returns:
        Подключение к ElasticSearch.
    """
    es = Elasticsearch([host])
    return es


@contextmanager
def elastic_search_connection(host: dict) -> Elasticsearch:
    """Создает подключение к ElasticSearch, которое закроет на выходе.

    Args:
        host: Настройки подключения.

    Yields:
        Подключение к ElasticSearch.
    """
    es = create_connection(host)
    yield es
    es.close()


class ElasticSearchSaver:
    """Загрузчик фильмов в индекс ElasticSearch."""

    def __init__(self, es_client: Elasticsearch):
        """Инициализация атрибутов класса.

        Args:
            es_client: Подключение к ElasticSearch.
        """
        self.client = es_client
        self.index = 'movies'

    def save(self, document: dict) -> None:
        """Создать или обновить документ.

        Args:
            document: Документ для Elastic Search.
        """
        self.client.update(
            index=self.index,
            id=document['id'],
            doc=document,
            params={'doc_as_upsert': 'true'},
        )

    def get(self, id: uuid.UUID) -> Union[dict, None]:
        """Получить документ по id.

        Args:
            id: Идентификатор документа.

        Returns:
            Документ Elastic Search.  Или None, если он не существует.
        """
        try:
            return self.client.get(index=self.index, id=id)
        except NotFoundError:
            return
