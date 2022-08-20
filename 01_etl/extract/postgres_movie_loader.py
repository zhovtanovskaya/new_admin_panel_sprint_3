"""Подключение и чтение из PostgreSQL."""

from typing import Generator
from psycopg2.extras import RealDictRow

from transform.db_objects import FilmWork

from .postgres_loader import EPOCH, PostgresLoader


class StateKeys:
    """Ключи, которые содержат состояния загрузки данных в ElasticSearch."""

    FILM_WORK = 'movie_film_work_since'
    PERSON = 'movie_person_work_since'
    GENRE = 'movie_genre_since'


class PostgresMovieLoader(PostgresLoader):
    """Класс, загружающий фильмы из PostgreSQL."""

    es_index = 'movies'
    validator = FilmWork

    def load_all(self) -> Generator[RealDictRow, None, None]:
        """Получить все обновленные и новые фильмы.

        Yields:
            Строка базы данных с полной информацией о фильме.
        """
        genre_since = self.state.get_state(StateKeys.GENRE) or EPOCH
        for ids, genre_since in self.ids_for_genre_since(genre_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.GENRE, genre_since)

        person_since = self.state.get_state(StateKeys.PERSON) or EPOCH
        for ids, person_since in self.ids_for_person_since(person_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.PERSON, person_since)

        film_work_since = self.state.get_state(StateKeys.FILM_WORK) or EPOCH
        for ids, film_work_since in self.ids_for_film_work_since(film_work_since):
            yield from self.get_film_works(ids)
            self.state.set_state(StateKeys.FILM_WORK, film_work_since)

    def ids_for_film_work_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID фильмов, отредактированных с указанного момента.

        Args:
            since: Получить фильмы, измененные после since.

        Yields:
            Список ID фильмов и самое раннее время правки этих фильмов.
        """
        sql = """
            SELECT
                fw.id,
                fw.modified
            FROM film_work fw
            WHERE fw.modified >= %s
            ORDER BY fw.modified, fw.id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_genre_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID фильмов, у которых изменился жанр.

        Args:
            since: Получить фильмы, жанры которых изменены после since.

        Yields:
            Список ID фильмов и самое раннее время правки жанра этих фильмов.
        """
        sql = """
            SELECT
                gfw.film_work_id,
                min(g.modified) min_modified
            FROM genre g
            INNER JOIN genre_film_work gfw ON g.id = gfw.genre_id
            WHERE g.modified >= %s
            GROUP BY gfw.film_work_id
            ORDER BY min_modified, gfw.film_work_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def ids_for_person_since(self, since: str = EPOCH) -> Generator[tuple[tuple[str], str], None, None]:
        """Получить ID фильмов, у которых изменились персоны.

        Args:
            since: Получить фильмы, персоны которых изменены после since.

        Yields:
            Список ID фильмов и самое раннее время правки персон этих фильмов.
        """
        sql = """
            SELECT
                pfw.film_work_id,
                min(p.modified) min_modified
            FROM person p
            INNER JOIN person_film_work pfw ON p.id = pfw.person_id
            WHERE p.modified >= %s
            GROUP BY pfw.film_work_id
            ORDER BY min_modified, pfw.film_work_id;
        """
        values = (since,)
        bunches = self._bunchify(self._execute_sql(sql, values))
        yield from self._split_bunch(bunches)

    def get_film_works(self, ids: tuple[str]) -> Generator[RealDictRow, None, None]:
        """Получить фильмы с указанными ID.

        Args:
            ids: Список ID фильмов.

        Yields:
            Полная информация о фильме в виде строки БД.
        """
        sql = """
            SELECT
                fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                COALESCE (
                   json_agg(
                       DISTINCT jsonb_build_object(
                           'role', pfw.role,
                           'id', p.id,
                           'name', p.full_name
                       )
                   ) FILTER (WHERE p.id is not null),
                   '[]'
                ) as persons,
                json_agg(DISTINCT g.name) as genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s
            GROUP BY fw.id
            ORDER BY fw.modified;
        """
        values = (tuple(ids),)
        rows = self._execute_sql(sql, values)
        yield from rows
