import json
import requests
from estester import MultipleIndexesQueryTestCase, ElasticSearchQueryTestCase


class IndexManagementSingleIndexTestCase(ElasticSearchQueryTestCase):

    timeout = None
    mappings = {
        "moscow": {
            "properties": {
                "name": {
                    "type": "string"
                },
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }
    settings = {
        "index": {
            "number_of_replicas": "4",
            "number_of_shards": "7"
        }
    }
    index = 'indexovisky'

    def test_created_index_with_proper_mapping_and_settings(self):
        url = "{0}{1}".format(self.host, self.index)
        response = requests.head(url)
        self.assertEqual(response.status_code, 200)

        response = requests.get(url + '/_mapping')
        self.assertEqual(response.status_code, 200)
        expected = {
            "indexovisky": {
                "mappings": self.mappings
            }
        }
        self.assertDictEqual(json.loads(response.text), expected)

        response = requests.get(url + '/_settings')
        self.assertEqual(response.status_code, 200)
        found = json.loads(response.text)['indexovisky']['settings']
        self.assertEqual(found['index']['number_of_replicas'], '4')
        self.assertEqual(found['index']['number_of_shards'], '7')


class IndexManagementMultipleIndexesTestCase(MultipleIndexesQueryTestCase):

    timeout = None
    mappings = {
        "moscow": {
            "properties": {
                "name": {
                    "type": "string"
                },
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }
    settings = {
        "index": {
            "number_of_replicas": "4",
            "number_of_shards": "7"
        }
    }

    def setUp(self):
        self.new_index = 'indexovisky'
        self.url = '{0}{1}'.format(self.host, self.new_index)

    def tearDown(self):
        requests.delete(self.url)

    def test_create_an_index_with_default_mappings_and_settings(self):
        self.create_index(self.new_index)
        response = requests.head(self.url)
        self.assertEqual(response.status_code, 200)

        response = requests.get(self.url + '/_mapping')
        self.assertEqual(response.status_code, 200)
        expected = {
            "indexovisky": {
                "mappings": self.mappings
            }
        }
        self.assertDictEqual(json.loads(response.text), expected)

        response = requests.get(self.url + '/_settings')
        self.assertEqual(response.status_code, 200)
        found = json.loads(response.text)['indexovisky']['settings']
        self.assertEqual(found['index']['number_of_replicas'], '4')
        self.assertEqual(found['index']['number_of_shards'], '7')

    def test_create_an_index_with_custom_mappings(self):
        mappings = {
            "mother_russia_people": {
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "age": {
                        "type": "integer"
                    }
                }
            }
        }
        self.create_index(self.new_index, mappings=mappings)
        response = requests.head(self.url)
        self.assertEqual(response.status_code, 200)
        response = requests.get(self.url + '/_mapping')
        self.assertEqual(response.status_code, 200)
        expected = {
            "indexovisky": {
                "mappings": mappings
            }
        }
        self.assertDictEqual(json.loads(response.text), expected)

    def test_create_an_index_with_custom_settings(self):
        settings = {
            "index": {
                "number_of_replicas": 2,
                "number_of_shards": 3
            }
        }
        self.create_index(self.new_index, settings=settings)
        response = requests.head(self.url)
        self.assertEqual(response.status_code, 200)
        response = requests.get(self.url + '/_settings')
        self.assertEqual(response.status_code, 200)
        found = json.loads(response.text)['indexovisky']['settings']
        self.assertEqual(found['index']['number_of_replicas'], '2')
        self.assertEqual(found['index']['number_of_shards'], '3')

    def test_delete_index(self):
        requests.put(self.url)
        self.delete_index(self.new_index)
        response = requests.head(self.url)
        self.assertEqual(response.status_code, 404)
