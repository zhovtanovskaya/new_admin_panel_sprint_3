from enum import Enum, auto
from unittest import TestCase, main

from availability.keep_alive import MaxRetriesExceeded, keep_alive


class Status(Enum):
    OK = auto()


class Connection:
    pass


class TestKeepAlive(TestCase):
    def setUp(self):
        connect = lambda: Connection()
        self.keep_alive = keep_alive(connect, (ConnectionError,), 2)

    def test_exceeds_retries(self):
        def func(connection):
            raise ConnectionError()
        self.assertRaises(MaxRetriesExceeded, self.keep_alive(func))

    def test_returns_decorated_function_result(self):
        func = lambda connection: Status.OK
        self.assertEqual(Status.OK, self.keep_alive(func)())

    def test_reconnects_on_failure(self):
        def func(connection):
            self.assertIsInstance(connection, Connection)
            raise ConnectionError()
        self.assertRaises(MaxRetriesExceeded, self.keep_alive(func))


if __name__ == '__main__':
    main()
