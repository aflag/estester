import json
from operator import itemgetter
from mock import patch
from estester import MultipleIndexesQueryTestCase, ElasticSearchException


class AliasMultipleIndexesTestCase(MultipleIndexesQueryTestCase):

    data = {
        "beatles": {
            "aliases": [
                "band"
            ],
            "fixtures": [
                {
                    "type": "member",
                    "id": "lenon",
                    "body": {
                        "name": "John Lenon",
                        "role": "singer"
                    }
                },
                {
                    "type": "member",
                    "id": "mccartney",
                    "body": {
                        "name": "Paul McCartney",
                        "role": "guitar"
                    }
                },
                {
                    "type": "member",
                    "id": "harrison",
                    "body": {
                        "name": "George Harrison",
                        "role": "bass"
                    }
                }
            ]
        },
        "thepolice": {
            "aliases": [
                "band",
                "single-man-band"
            ],
            "fixtures": [
                {
                    "type": "member",
                    "id": "sting",
                    "body": {
                        "name": "Gordon Matthew Thomas Sumner",
                        "role": "singer"
                    }
                }
            ]
        }
    }
    timeout = None

    def test_finds_aliases_for_all_indices(self):
        self.assertEqual(set(self.get_aliases("thepolice")),
                         {"band", "single-man-band"})
        self.assertEqual(self.get_aliases("beatles"), ["band"])

    def test_create_alias_for_an_index(self):
        self.create_aliases('thepolice', ['stingband'])
        self.assertEqual(set(self.get_aliases('thepolice')),
                         {'band', 'single-man-band', 'stingband'})

    def test_get_aliases_on_missing_index_must_return_empty(self):
        self.assertEqual(self.get_aliases('hoodoogurus'), [])

    def test_get_aliases_on_index_with_no_aliases_must_return_empty(self):
        self.create_index('metallica')
        self.assertEqual(self.get_aliases('metallica'), [])

    # I could not make elasticsearch's _aliases to return an error
    @patch('requests.get')
    def test_failure_in_getting_alias(self, get):
        attrs = {
            "text": 'Error',
            "status_code": 400
        }
        get.return_value.configure_mock(**attrs)
        with self.assertRaises(ElasticSearchException) as cm:
            self.get_aliases('hoodoogurus')
        self.assertEqual(cm.exception.message, 'Error')

    def test_creating_alias_for_missing_index_must_fail(self):
        with self.assertRaises(ElasticSearchException) as cm:
            self.create_aliases('hoodoogurus', ['stingband'])
        expected = {
            "error": "IndexMissingException[[hoodoogurus] missing]",
            "status": 404
        }
        self.assertDictEqual(json.loads(cm.exception.message), expected)

    def test_search_alias_for_sting(self):
        query = {
            "query": {
                "match": {
                    "name": "Gordon"
                }
            }
        }
        response = self.search_in_index("single-man-band", query)
        self.assertEqual(response["hits"]["total"], 1)
        self.assertEqual(response['hits']['hits'][0]['_id'], 'sting')

    def test_single_index_alias_must_return_only_one_singer(self):
        query = {
            "query": {
                "match": {
                    "role": "singer"
                }
            }
        }
        response = self.search_in_index('single-man-band', query)
        self.assertEqual(response["hits"]["total"], 1)
        self.assertEqual(response["hits"]["hits"][0]['_id'], 'sting')

    def test_search_bands_for_singer(self):
        query = {
            "query": {
                "match": {
                    "role": "singer"
                }
            }
        }
        response = self.search_in_index('band', query)
        self.assertEqual(response["hits"]["total"], 2)
        ids = map(itemgetter('_id'), response["hits"]["hits"])
        self.assertIn('sting', ids)
        self.assertIn('lenon', ids)
