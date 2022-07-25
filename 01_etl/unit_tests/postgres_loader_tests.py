from unittest import TestCase, main

from postgres_loader import PostgresLoader, create_connection
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


if __name__ == '__main__':
    main()
