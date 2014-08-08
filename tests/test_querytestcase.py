import unittest
import json
import time
import requests
from operator import itemgetter
from mock import patch
from estester import ElasticSearchQueryTestCase, ExtendedTestCase,\
    MultipleIndexesQueryTestCase, ElasticSearchException


SIMPLE_QUERY = {
    "query": {
        "query_string": {
            "fields": [
                "name"
            ],
            "query": "nina"
        }
    }
}
SINGLE_INDEX_FIXTURE = [
    {
        "type": "dog",
        "id": "1",
        "body": {"name": "Nina Fox"}
    },
    {
        "type": "dog",
        "id": "2",
        "body": {"name": "Charles M."}
    },
    {
        "type": "internet/dog",
        "id": "http://dog.com",
        "body": {"name": "It bytes"}
    }
]
MULTIPLE_INDEXES_FIXTURE = {
    "personal": {
        "fixtures": [
            {
                "type": "contact",
                "id": "1",
                "body": {"name": "Dmitriy"}
            },
            {
                "type": "contact",
                "id": "2",
                "body": {"name": "Agnessa"}
            }
        ]
    },
    "professional": {
        "fixtures": [
            {
                "type": "contact",
                "id": "1",
                "body": {"name": "Nikolay"}
            }
        ]
    },
    "magical": {
        "fixtures": [
            {
                "type": "wizard/mage",
                "id": "http://middleearth.com/gandalf",
                "body": {"name": "Gandalf the Grey"}
            }
        ]
    }
}


def raise_interruption(self):
    raise KeyboardInterrupt


class TestExtendedTestCase(ExtendedTestCase):

    value = 1

    def _pre_setup(self):
        self.value = self.value * 2

    def setUp(self):
        self.value = self.value ** 3

    def test_pre_setup(self):
        self.assertEqual(self.value, 8)


class DefaultValuesTestCase(unittest.TestCase):

    def test_default_values(self):
        ESQTC = ElasticSearchQueryTestCase
        self.assertEqual(ESQTC.index, "sample.test")
        self.assertEqual(ESQTC.reset_index, True)
        self.assertEqual(ESQTC.host, "http://0.0.0.0:9200/")
        self.assertEqual(ESQTC.fixtures, [])
        self.assertEqual(ESQTC.timeout, 5)
        self.assertEqual(ESQTC.proxies, {})


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

    # I could not  make elasticsearch's _aliases to return an error
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


class MultipleIndexesFixtureLoadingTestCase(MultipleIndexesQueryTestCase):

    data = MULTIPLE_INDEXES_FIXTURE
    timeout = None

    # Avoid that load_fixtures is called before every test
    def _pre_setup(self):
        for index_name, index in self.data.items():
            if self.reset_index:
                self.delete_index(index_name)
            settings = index.get("settings", {})
            mappings = index.get("mappings", {})
            self.create_index(index_name, settings, mappings)

    def test_create_index(self):
        index = "indexovisky"
        url = '{0}{1}'.format(self.host, index)
        try:
            self.create_index(index)
            response = requests.head(url)
            self.assertEqual(response.status_code, 200)
        finally:
            requests.delete(url)

    def test_delete_index(self):
        index = "indexovisky"
        url = '{0}{1}'.format(self.host, index)
        try:
            requests.put(url)
            self.delete_index(index)
            response = requests.head(url)
            self.assertEqual(response.status_code, 404)
        finally:
            requests.delete(url)

    def test_load_fixtures_sets_all_documents_in_place(self):
        loaded_fixtures = 0
        for index_name, index in self.data.items():
            self.load_fixtures(index_name, index['fixtures'])
            response = self.search()
            loaded_fixtures += len(index['fixtures'])
            self.assertEqual(response["hits"]["total"], loaded_fixtures)

    @patch('time.sleep')
    def test_assert_that_timeout_is_being_waited_by_load_fixtures(self, sleep):
        old_timeout = self.timeout
        try:
            self.timeout = 5
            index_name, index = self.data.items()[0]
            self.load_fixtures(index_name, index['fixtures'])
            sleep.assert_called_once_with(5)
        finally:
            self.timeout = old_timeout


class SingleIndexFixtureLoadingTestCase(ElasticSearchQueryTestCase):

    fixtures = SINGLE_INDEX_FIXTURE
    timeout = None

    # Avoid that load_fixtures is called before every test
    def _pre_setup(self):
        if self.reset_index:
            self.delete_index()
        self.create_index()

    def test_load_fixtures_sets_all_documents_in_place(self):
        self.load_fixtures()
        response = self.search()
        self.assertEqual(response["hits"]["total"], len(self.fixtures))

    @patch('time.sleep')
    def test_assert_that_timeout_is_being_waited_by_load_fixtures(self, sleep):
        old_timeout = self.timeout
        try:
            self.timeout = 5
            self.load_fixtures()
            sleep.assert_called_once_with(5)
        finally:
            self.timeout = old_timeout


class SimpleMultipleIndexesQueryTestCase(MultipleIndexesQueryTestCase):

    data = MULTIPLE_INDEXES_FIXTURE
    timeout = None

    def test_search_all_indexes(self):
        response = self.search()
        expected = [
            {
                u'_score': 1.0,
                u'_type': u'contact',
                u'_id': u'1',
                u'_source': {u'name': u'Dmitriy'},
                u'_index': u'personal'
            },
            {
                u'_score': 1.0,
                u'_type': u'contact',
                u'_id': u'2',
                u'_source': {u'name': u'Agnessa'},
                u'_index': u'personal'
            },
            {
                u'_score': 1.0,
                u'_type': u'contact',
                u'_id': u'1',
                u'_source': {u'name': u'Nikolay'},
                u'_index': u'professional'
            },
            {
                u'_score': 1.0,
                u'_type': u'wizard/mage',
                u'_index': u'magical',
                u'_id': u'http://middleearth.com/gandalf',
                u'_source': {u'name': u'Gandalf the Grey'}
            }
        ]
        self.assertEqual(response["hits"]["total"], 4)
        self.assertEqual(sorted(response["hits"]["hits"]), sorted(expected))

    def test_search_one_index_that_has_item(self):
        query = {
            "query": {
                "match": {
                    "name": "Agnessa"
                }
            }
        }
        response = self.search_in_index("personal", query)
        self.assertEqual(response["hits"]["total"], 1)
        expected = {u'name': u'Agnessa'}
        self.assertEqual(response["hits"]["hits"][0]["_source"], expected)

    def test_search_one_index_that_doesnt_have_item(self):
        query = {
            "query": {
                "match": {
                    "name": "Agnessa"
                }
            }
        }
        response = self.search_in_index("professional", query)
        self.assertEqual(response["hits"]["total"], 0)

    def test_get_correct_document(self):
        response = self.get('personal', 'contact', '1')
        expected = {
            "_index": "personal",
            "_type": "contact",
            "_id": "1",
            "_version": 1,
            "found": True,
            "_source": {
                "name": "Dmitriy"
            }
        }
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertDictEqual(response, expected)

    def test_get_document_with_url_id(self):
        response = self.get('magical',
                            'wizard/mage',
                            'http://middleearth.com/gandalf')
        expected = {
            "_index": "magical",
            "_type": "wizard/mage",
            "_id": "http://middleearth.com/gandalf",
            "_version": 1,
            "found": True,
            "_source": {
                "name": "Gandalf the Grey"
            }
        }
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertDictEqual(response, expected)

    def test_get_exception_on_missing_document(self):
        with self.assertRaises(ElasticSearchException) as cm:
            self.get('personal', 'contact', '20')
        expected = {
            "_index": "personal",
            "_type": "contact",
            "_id": "20",
            "found": False
        }
        response = json.loads(cm.exception.message)
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertEqual(response, expected)


class SimpleQueryTestCase(ElasticSearchQueryTestCase):

    fixtures = SINGLE_INDEX_FIXTURE
    timeout = None

    def test_must_refresh_test_case_index(self):
        response = self.refresh()
        self.assertEqual(response['_shards']['failed'], 0)

    def test_refresh_must_call_refresh_index_with_test_case_index(self):
        with patch.object(self, 'refresh_index') as refresh_index:
            self.refresh()
        refresh_index.assert_called_once_with(self.index)

    def test_must_refresh_all_indices(self):
        response = self.refresh_index()
        self.assertEqual(response['_shards']['failed'], 0)

    def test_must_refresh_specific_index(self):
        response = self.refresh_index(self.index)
        self.assertEqual(response['_shards']['failed'], 0)

    def test_must_fail_when_passing_missing_index(self):
        with self.assertRaises(ElasticSearchException) as cm:
            self.refresh_index('ohnoes')
        expected = \
            '{"error":"IndexMissingException[[ohnoes] missing]","status":404}'
        self.assertEqual(cm.exception.message, expected)

    @patch('requests.post')
    def test_must_call_refresh_on_url_root(self, post):
        attrs = {
            "text": '{"_shards":{"total":20,"successful":10,"failed":0}}',
            "status_code": 200
        }
        post.return_value.configure_mock(**attrs)
        self.refresh_index()
        post.assert_called_once_with('{0}_refresh'.format(self.host),
                                     proxies=self.proxies)

    def test_search_by_nothing_returns_three_results(self):
        response = self.search()
        expected = {u"name": u"Nina Fox"}
        self.assertEqual(response["hits"]["total"], 3)
        ids = map(itemgetter('_id'), response["hits"]["hits"])
        self.assertIn(u"1", ids)
        self.assertIn(u"2", ids)
        self.assertIn(u"http://dog.com", ids)

    def test_search_by_nina_returns_one_result(self):
        response = self.search(SIMPLE_QUERY)
        expected = {u"name": u"Nina Fox"}
        self.assertEqual(response["hits"]["total"], 1)
        self.assertEqual(response["hits"]["hits"][0]["_id"], u"1")
        self.assertEqual(response["hits"]["hits"][0]["_source"], expected)

    def test_get_correct_document(self):
        response = self.get('dog', '1')
        expected = {
            "_index": "sample.test",
            "_type": "dog",
            "_id": "1",
            "_version": 1,
            "found": True,
            "_source": {
                "name": "Nina Fox"
            }
        }
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertDictEqual(response, expected)

    def test_get_document_with_url_id(self):
        response = self.get('internet/dog', 'http://dog.com')
        expected = {
            "_index": "sample.test",
            "_type": "internet/dog",
            "_id": "http://dog.com",
            "_version": 1,
            "found": True,
            "_source": {
                "name": "It bytes"
            }
        }
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertDictEqual(response, expected)

    def test_get_exception_on_missing_document(self):
        with self.assertRaises(ElasticSearchException) as cm:
            self.get('dog', '20')
        expected = {
            "_index": "sample.test",
            "_type": "dog",
            "_id": "20",
            "found": False
        }
        response = json.loads(cm.exception.message)
        # Compatibility with elasticsearch 0.90
        if 'exists' in response:
            expected['exists'] = expected.pop('found')
        self.assertEqual(response, expected)

    def test_tokenize_with_default_analyzer(self):
        response = self.tokenize("Nothing to declare", "default")
        items_list = response["tokens"]
        self.assertEqual(len(items_list), 2)
        tokens = [item["token"] for item in items_list]
        self.assertEqual(sorted(tokens), ["declare", "nothing"])

    def test_tokenize_with_default_analyzer(self):
        response = self.tokenize("Nothing to declare", "whitespace")
        items_list = response["tokens"]
        self.assertEqual(len(items_list), 3)
        tokens = [item["token"] for item in items_list]
        self.assertEqual(sorted(tokens), ['"Nothing', 'declare"', "to"])
