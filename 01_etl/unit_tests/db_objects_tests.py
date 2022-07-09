from datetime import datetime
from unittest import TestCase, main

from db_objects import FilmWork


class TestFilmWork(TestCase):

    def setUp(self):
        self.film_work = FilmWork(
            title=' Star Wars: Episode IV - A New Hope',
            description='The Imperial Forces...',
            type='movie',
            creation_date='2021-06-16',
            created='2021-06-16 20:14:09.313086+00',
            modified='2021-06-16 20:14:09.313086+00',
            genres=['Action', 'Adventure', 'Fantasy', 'Sci-Fi'],
            persons=[   
                {
                    'id': '26e83050-29ef-4163-a99d-b546cac208f8',
                    'name': 'Mark Hamill',
                    'role': 'actor',
                },
                {
                    'id': 'a5a8f573-3cee-4ccc-8a2b-91cb9f55250a',
                    'name': 'George Lucas',
                    'role': 'director'
                },
                {
                    'id': '139e8f31-b198-4127-a4a0-958e35c328bc',
                    'name': 'Minta Durfee',
                    'role': 'writer'
                },
            ],

        )

    def test_created_at_is_datetime(self):
        self.assertEqual(datetime, type(self.film_work.created))

    def test_updated_at_is_datetime(self):
        self.assertEqual(datetime, type(self.film_work.modified))

    def test_one_director(self):
        self.assertEqual(['George Lucas'], self.film_work.director)

    def test_one_actor_name(self):
        self.assertEqual(['Mark Hamill'], self.film_work.actors_names)

    def test_one_writer_name(self):
        self.assertEqual(['Minta Durfee'], self.film_work.writers_names)


if __name__ == '__main__':
    main()
