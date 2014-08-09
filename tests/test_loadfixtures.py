from mock import patch
from estester import ElasticSearchQueryTestCase, MultipleIndexesQueryTestCase


class MultipleIndexesFixtureLoadingTestCase(MultipleIndexesQueryTestCase):

    data = {
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
    timeout = None

    # Avoid that load_fixtures is called before every test
    def _pre_setup(self):
        for index_name, index in self.data.items():
            if self.reset_index:
                self.delete_index(index_name)
            settings = index.get("settings", {})
            mappings = index.get("mappings", {})
            self.create_index(index_name, settings, mappings)

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

    fixtures = [
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
