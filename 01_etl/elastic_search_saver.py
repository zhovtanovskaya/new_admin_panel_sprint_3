from contextlib import contextmanager

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
def postgres_connection(host: dict) -> Elasticsearch:
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
    def __init__(self, es_client: Elasticsearch):
        self.client = es_client

    def save(self, document):
        status = self.client.create(index="movies", id=document['id'], document=document)
        assert status['result'] == 'created'

    def get(self, id):
        try:
            return self.client.get(index="movies", id=id)
        except NotFoundError:
            return
