"""Сбор информации о жанрах из базы данных Postgresql."""

from typing import Generator
from psycopg2.extras import RealDictRow

from transform.db_objects import Genre

from .postgres_loader import EPOCH, PostgresLoader


class StateKeys:
    """Ключи, которые содержат состояния загрузки данных в ElasticSearch."""

    FILM_WORK = 'genre_film_work_since'
    GENRE = 'genre_genre_since'


class PostgresGenreLoader(PostgresLoader):
    """Класс, загружающий жанры из PostgreSQL."""

    es_index = 'genres'
    validator = Genre

    def load_all(self) -> Generator[RealDictRow, None, None]:
        """Получить все обновленные и новые жанры.

        Yields:
            Строка базы данных с информацией о жанре.
        """
        # если у жанра меняется состав фильмов, в которых он прикреплён,
        # то эти изменения мы отслеживаем в объекте жанра, потому что так
        # отрабатывает orm django.
        film_work_since = self.state.get_state(StateKeys.FILM_WORK) or EPOCH
        for ids, film_work_since in self.ids_for_film_work_since(film_work_since):
            yield from self.get_genres(ids)
            self.state.set_state(StateKeys.FILM_WORK, film_work_since)

        genre_since = self.state.get_state(StateKeys.GENRE) or EPOCH
        for ids, genre_since in self.ids_for_genre_since(genre_since):
            yield from self.get_genres(ids)
            self.state.set_state(StateKeys.GENRE, genre_since)

    def ids_for_genre_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID жанров, отредактированных с указанного момента.

        Args:
            since: Получить жанры, измененные после since.

        Yields:
            Список ID жанров и самое раннее время правки этих жанров.
        """
        sql = """
            SELECT
                genre.id,
                genre.modified
            FROM genre
            WHERE genre.modified >= %s
            ORDER BY genre.modified, genre.id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_film_work_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID жанров у изменённых фильмов.

        Args:
            since: Получить жанры по фильмам, изменённым после since.

        Yields:
            Список ID жанров и самое раннее время правки фильма этих жанров.
        """
        sql = """
            SELECT
                gfw.genre_id,
                min(fw.modified) min_modified
            FROM film_work fw
            INNER JOIN genre_film_work gfw ON fw.id = gfw.film_work_id
            WHERE fw.modified >= %s
            GROUP BY gfw.genre_id
            ORDER BY min_modified, gfw.genre_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def get_genres(self, ids: tuple[str]) -> Generator[RealDictRow, None, None]:
        """Получить жанры с указанными ID.

        Args:
            ids: Список ID жанров.

        Yields:
            Информация о жанре в виде строки БД.
        """
        sql = """
            SELECT
                genre.id,
                genre.name,
                genre.description,
                COALESCE(
                    JSON_AGG(DISTINCT gfw.film_work_id),
                    '{}'
                ) as film_ids
            FROM genre
            LEFT JOIN genre_film_work gfw ON gfw.genre_id = genre.id
            WHERE genre.id IN %s
            GROUP BY genre.id
            ORDER BY genre.modified;
        """
        values = (tuple(ids),)
        rows = self._execute_sql(sql, values)
        yield from rows
