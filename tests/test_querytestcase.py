import unittest
import time
import requests
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
        self.assertEqual(response["hits"]["total"], 2)

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
                u'_id': u'1', u'_source': {u'name': u'Nikolay'},
                u'_index': u'professional'
            }
        ]
        self.assertEqual(response["hits"]["total"], 3)
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

    def test_search_by_nothing_returns_two_results(self):
        response = self.search()
        expected = {u"name": u"Nina Fox"}
        self.assertEqual(response["hits"]["total"], 2)
        self.assertEqual(response["hits"]["hits"][0]["_id"], u"1")
        self.assertEqual(response["hits"]["hits"][1]["_id"], u"2")

    def test_search_by_nina_returns_one_result(self):
        response = self.search(SIMPLE_QUERY)
        expected = {u"name": u"Nina Fox"}
        self.assertEqual(response["hits"]["total"], 1)
        self.assertEqual(response["hits"]["hits"][0]["_id"], u"1")
        self.assertEqual(response["hits"]["hits"][0]["_source"], expected)

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
