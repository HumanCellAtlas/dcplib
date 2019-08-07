import os
import sys
import unittest

import responses

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


@unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
class TestHALAgent(unittest.TestCase):

    def setUp(self):
        from dcplib.component_agents.hal_agent import HALAgent
        self.agent = HALAgent(api_url_base="https://does_not_exist/api")

    @responses.activate
    def test_get__with_path__adds_path_to_base(self):
        expected_url = "https://does_not_exist/api/some_resource"
        responses.add(responses.GET, expected_url, json='{}', status=200)

        self.agent.get("/some_resource")

        self.assertEqual(1, len(responses.calls))
        self.assertEqual('GET', responses.calls[0].request.method)
        self.assertEqual(expected_url, responses.calls[0].request.url)

    @responses.activate
    def test_get__with_url__uses_url(self):
        url = "https://a_different_url/api/some_resource"
        responses.add(responses.GET, url, json="{}", status=200)

        self.agent.get(url)

        self.assertEqual(1, len(responses.calls))
        self.assertEqual('GET', responses.calls[0].request.method)
        self.assertEqual(url, responses.calls[0].request.url)

    @responses.activate
    def test_get__returns_response_body_as_is(self):
        url = "https://does_not_exist/api/some_resource"
        result_data = '{"foo":null}'
        responses.add(responses.GET, url, json=result_data, status=200)

        result = self.agent.get("/some_resource")

        self.assertEqual(1, len(responses.calls))
        self.assertEqual('GET', responses.calls[0].request.method)
        self.assertEqual(url, responses.calls[0].request.url)
        self.assertEqual(result_data, result)

    @responses.activate
    def test_iter_pages__follows_next_links_and_returns_only_embedded_data(self):
        page_1_url = "https://somesite/someresource"
        page_2_url = "https://somesite/someresource?page=2&size=100"
        page1 = {
            "_embedded": ["item1", "item2"],
            "_links": {
                "next": {
                    "href": page_2_url
                }
            }
        }
        page2 = {
            "_embedded": ["item3", "item4"],
            "_links": {}
        }
        responses.add(responses.GET, page_1_url + "?size=100", json=page1, status=200)
        responses.add(responses.GET, page_2_url, json=page2, status=200)

        pages = []
        for page in self.agent.iter_pages(path_or_url="https://somesite/someresource"):
            pages.append(page)

        self.assertEqual(2, len(pages))
        self.assertEqual([["item1", "item2"], ["item3", "item4"]], pages)

    @responses.activate
    def test_get_all(self):
        page_1_url = "https://somesite/someresource"
        page_2_url = "https://somesite/someresource?page=2&size=100"
        page1 = {
            "_embedded": {"foos": ["foo1"]},
            "_links": {
                "next": {
                    "href": page_2_url
                }
            }
        }
        page2 = {
            "_embedded": {"foos": ["foo2"]},
            "_links": {}
        }
        responses.add(responses.GET, page_1_url + "?size=100", json=page1, status=200)
        responses.add(responses.GET, page_2_url, json=page2, status=200)

        result = self.agent.get_all(path_or_url=page_1_url, result_element_we_are_interested_in="foos")
        self.assertEqual(["foo1", "foo2"], result)
