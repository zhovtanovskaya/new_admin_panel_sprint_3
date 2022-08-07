from itertools import chain
from unittest import TestCase, main

from extract.postgres_loader import PostgresLoader, create_connection
from settings import POSTGRES_DB


class TestPostgresLoader(TestCase):
    def setUp(self):
        connection = create_connection(POSTGRES_DB)
        self.loader = PostgresLoader(connection)

    def test_load_top(self):
        row = next(self.loader.load())
        self.assertEqual('Star Wars: Episode IV - A New Hope', row['title'])

    def test_load_since(self):
        row = next(self.loader.load(since='2021-06-16 20:14:09.222232+00'))
        self.assertEqual('Star Trek', row['title'])


class TestPostgresLoaderRowsForGenreSince(TestCase):
    def setUp(self):
        connection = create_connection(POSTGRES_DB)
        self.loader = PostgresLoader(connection)

    def test_top_ids(self):
        ids = self.loader._rows_for_genre_since(bunch_size=2)
        two_ids = next(ids)
        ids = self.loader._rows_for_genre_since(bunch_size=1)
        one_by_one_ids = list(chain(next(ids), next(ids)))
        self.assertEqual(two_ids, one_by_one_ids)

    def test_since_ids(self):
        since = '2021-06-16 20:14:09.310212+00:00'
        ids = self.loader._rows_for_genre_since(since=since, bunch_size=1)
        since_ids = next(ids)
        ids = self.loader._rows_for_genre_since(bunch_size=1)
        top_ids = next(ids)
        self.assertNotEqual(since_ids, top_ids)


class TestPostgresLoaderIDsForGenreSince(TestCase):
    def setUp(self):
        connection = create_connection(POSTGRES_DB)
        self.loader = PostgresLoader(connection)

    def test_load(self):
        self.loader.ids_for_genre_since()

if __name__ == '__main__':
    main()
