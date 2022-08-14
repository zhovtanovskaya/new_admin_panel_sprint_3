from unittest import TestCase, main

import settings
from load.elastic_search_saver import ElasticSearchSaver, create_connection


class Test(TestCase):

    def setUp(self):
        self.client = create_connection(settings.ELASTIC_TEST_HOST)
        self.ess = ElasticSearchSaver(self.client)

    def tearDown(self):
        self.client.close()

    def test_caches_document(self):
        doc = {
            'id': '24eafcd7-1018-4951-9e17-583e2554ef0a'
        }
        self.ess.save(doc)
        self.assertIsNone(self.ess.get(doc['id']))

    def test_creates_document(self):
        doc = {
            'id': '24eafcd7-1018-4951-9e17-583e2554ef0a'
        }
        self.ess.save(doc)
        self.ess.flush()
        self.assertIsNotNone(self.ess.get(doc['id']))

    def test_updates_document(self):
        doc = {
            'id': '24eafcd7-1018-4951-9e17-583e2554ef0a'
        }
        self.ess.save(doc)
        self.ess.save(doc)


if __name__ == '__main__':
    main()
