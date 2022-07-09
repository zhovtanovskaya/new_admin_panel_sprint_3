from unittest import TestCase, main

from postgres_loader import PostgresLoader, create_connection
from settings import POSTGRES_DB


class Test(TestCase):
    def setUp(self):
        connection = create_connection(POSTGRES_DB)
        self.loader = PostgresLoader(connection)

    def test(self):
        row = next(self.loader.load())
        from pprint import PrettyPrinter
        pp = PrettyPrinter(indent=4)
        pp.pprint(dict(**row))


if __name__ == '__main__':
    main()
