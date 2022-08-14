"""Хранение состояния загрузки данных в ElasticSearch."""

import abc
import json
from typing import Any, Optional


class BaseStorage:
    """Интерфейс хранилища состояния."""

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище.

        Args:
            state: Состояние для записи в хранилище.
        """
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища.

        Returns:
            Состояние из хранилища.
        """
        return {}


class JsonFileStorage(BaseStorage):
    """Хранилище состояния в виде JSON в файле."""

    def __init__(self, file_path: Optional[str] = None):
        """Проинициализировать путь к файлу.

        Args:
            file_path: Путь к файлу для хранения состояния.
        """
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Записать JSON-состояние в файл.

        Args:
            state: состояние в виде JSON.
        """
        with open(self.file_path, 'w') as f:
            f.write(json.dumps(state, default=str))

    def retrieve_state(self) -> dict:
        """Получить JSON-состояние из файла.

        Returns:
            JSON-состояние из файла.
        """
        try:
            with open(self.file_path) as f:
                state = f.read()
        except FileNotFoundError:
            return {}
        try:
            return json.loads(state)
        except json.decoder.JSONDecodeError:
            return {}


class State:
    """Класс для хранения состояния при работе с данными.

    Предназначен для хранения актуального состояния процесса
    обработки данных.  Дает возможность после перезапуска
    процесса продолжить не с начала, а с позиции, на которой
    остановился.
    """

    def __init__(self, storage: BaseStorage):
        """Проинициализировать состояние и хранилище состояния.

        Args:
            storage: Постоянное хранилище состояния.
        """
        self.storage = storage
        self._state = {}

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа.

        Args:
            key: Ключ состояния.
            value: Значение ключа.
        """
        self._state[key] = value
        self.storage.save_state(self._state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу.

        Args:
            key: Один из ключей состояния.

        Returns:
            Значение ключа.
        """
        self._state = self.storage.retrieve_state()
        return self._state.get(key)
