from urllib.parse import urlencode

import requests


class HALAgent:
    """
    A collection of functions for working with a Hypermedia Application
    Language (HAL) JSON API.

    Specifically, the HCA DCP Ingest Service implementation thereof.

    Provide api_url_base WITHOUT a trailing /
    """

    def __init__(self, api_url_base):
        self.api_url_base = api_url_base
        self.headers = {'Content-type': 'application/json'}

    def get_all(self, path_or_url, result_element_we_are_interested_in, page_size=100):
        """Get a collection resource.

        Iterates through all pages gathering results and returns a list.

        :param str path_or_url: location of collection
        :param str result_element_we_are_interested_in: name to extract from _embedded
        :param int page_size: number of resources to request per page (default 100)
        :return: result dicts
        :rtype: list
        """
        results = []
        for page in self.iter_pages(path_or_url, page_size):
            results += page[result_element_we_are_interested_in]
        return results

    def iter_pages(self, path_or_url, page_size=100, sort=None):
        """Iterate through a collection using HAL pagination, yielding pages of data.

        The API we are interfacing with:
        - Must accept URL params 'size' and 'sort'
        - Return a list of results inside the element '_embedded'
        - Has a '_links' -> 'next' -> 'href' element if there is more data.

        :param str path_or_url: location of collection
        :param int page_size: number of resources to request per page (default 100)
        :param str sort: sort order to request (default=None)
        :return: yields a page of results at a time
        :rtype: list
        """
        url_params = {'size': page_size}
        if sort:
            url_params['sort'] = sort
        path_or_url += '?' + urlencode(url_params)

        while True:
            data = self.get(path_or_url)
            if '_embedded' not in data:
                break

            yield data['_embedded']

            if 'next' in data['_links']:
                path_or_url = data['_links']['next']['href']
            else:
                break

    def get(self, path_or_url):
        """Get a singleton resource.

        :param str path_or_url: location of resource
        :return: returns the raw response (does not unpack _embedded)
        :rtype: dict
        """
        if path_or_url.startswith('http'):
            url = path_or_url
        else:
            url = f"{self.api_url_base}{path_or_url}"

        response = requests.get(url, headers=self.headers)

        if response.ok:
            return response.json()
        else:
            raise RuntimeError(f"GET {url} got {response}")
