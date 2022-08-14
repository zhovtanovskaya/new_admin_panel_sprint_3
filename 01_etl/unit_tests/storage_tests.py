from unittest import TestCase, main

from storage import JsonFileStorage, State


class FileStorageTestCase(TestCase):
    TEST_STORAGE_FILE = 'test_storage.json'

    def setUp(self):
        self.storage = JsonFileStorage(self.TEST_STORAGE_FILE)


class TestJsonFileStorage(FileStorageTestCase):

    def test(self):
        state = {'key': 'value'}
        self.storage.save_state(state)
        self.assertEqual(state, self.storage.retrieve_state())


class Test(FileStorageTestCase):

    def setUp(self):
        super().setUp()
        self.state = State(self.storage)

    def test_sets_one_key(self):
        key = 'key'
        value = 'value'
        self.state.set_state(key, value)
        self.assertEqual(value, self.state.get_state(key))

    def test_sets_two_keys(self):
        dict_state = {
            'a': 'value_a', 
            'b': 'value_b',
        }
        for k, v in dict_state.items():
            self.state.set_state(k, v)
        self.assertEqual(dict_state['a'], self.state.get_state('a'))

    def test_returns_none(self):
        self.assertIsNone(self.state.get_state('no_such_key'))


if __name__ == '__main__':
    main()
