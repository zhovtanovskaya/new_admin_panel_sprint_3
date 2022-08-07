"""Загрузка фильмов в Elastic Search."""

import uuid
from contextlib import contextmanager
from typing import Union

from elasticsearch import Elasticsearch, helpers
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

    def __init__(self, es_client: Elasticsearch, batch_size=100):
        """Инициализация атрибутов класса.

        Args:
            es_client: Подключение к ElasticSearch.
            batch_size: Размер буфера для сохраняемых объектов.
        """
        self.client = es_client
        self.index = 'movies'
        self._documents = []
        self._batch_size = batch_size

    def save(self, document: dict) -> None:
        """Создать или обновить документ.

        Args:
            document: Документ для Elastic Search.
        """
        self._documents.append(document)

    def is_batch_ready(self) -> bool:
        """Проверить заполнен ли буфер.

        Returns:
            Истина, если в буфере больше self._batch_size объектов.
        """
        return len(self._documents) >= self._batch_size

    def flush(self) -> None:
        """Сохранить все объекты из буфера в ElasticSearch."""
        def get_actions(documents: list[dict]) -> dict:
            for document in documents:
                action = {
                    '_index': self.index,
                    '_op_type': 'index',
                    '_id': document['id'],
                    '_source': document,
                }
                yield action
        helpers.bulk(self.client, get_actions(self._documents))
        self._documents = []

    def get(self, id: uuid.UUID) -> Union[dict, None]:
        """Получить документ по id.

        Args:
            id: Идентификатор документа.

        Returns:
            Документ Elastic Search.  Или None, если он не существует.
        """
        try:
            return self.client.get(index=self.index, id=str(id))
        except NotFoundError:
            return
