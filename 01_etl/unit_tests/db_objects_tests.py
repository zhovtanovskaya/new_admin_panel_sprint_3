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
                    'person_id': '26e83050-29ef-4163-a99d-b546cac208f8',
                    'person_name': 'Mark Hamill',
                    'person_role': 'actor',
                },
                {
                    'person_id': 'a5a8f573-3cee-4ccc-8a2b-91cb9f55250a',
                    'person_name': 'George Lucas',
                    'person_role': 'director'
                },
                {
                    'person_id': '139e8f31-b198-4127-a4a0-958e35c328bc',
                    'person_name': 'Minta Durfee',
                    'person_role': 'writer'
                },
            ],

        )

    def test_created_at_is_datetime(self):
        self.assertEqual(datetime, type(self.film_work.created))

    def test_updated_at_is_datetime(self):
        self.assertEqual(datetime, type(self.film_work.modified))


if __name__ == '__main__':
    main()
