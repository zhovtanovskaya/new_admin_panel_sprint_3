from unittest import TestCase, main

from elastic_search_saver import ElasticSearchSaver, create_connection

ELASTIC_SEARCH_TEST_HOST = {
    'host': 'localhost', 
    'port': 9200,
}


class Test(TestCase):

    def setUp(self):
        self.client = create_connection(ELASTIC_SEARCH_TEST_HOST)
        self.ess = ElasticSearchSaver(self.client)

    def tearDown(self):
        self.client.close()

    def test_creates_document(self):
        doc = {
            'id': '24eafcd7-1018-4951-9e17-583e2554ef0a'
        }
        self.assertIsNone(self.ess.get(doc['id']))
        self.ess.save(doc)
        self.assertIsNotNone(self.ess.get(doc['id']))


if __name__ == '__main__':
    main()
