import logging

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import retry, timeout

logger = logging.getLogger(__name__)

class RetryPolicy(retry.Retry):
    def __init__(self, retry_after_status_codes={301}, *args, **kwargs):
        super(RetryPolicy, self).__init__(*args, **kwargs)
        self.RETRY_AFTER_STATUS_CODES = frozenset(retry_after_status_codes | retry.Retry.RETRY_AFTER_STATUS_CODES)

    def increment(self, *args, **kwargs):
        retry = super(RetryPolicy, self).increment(*args, **kwargs)
        logger.warning("Retrying: {}".format(retry.history[-1]))
        return retry

class HTTPRequest:
    """
    A requests wrapper preconfigured with best practice timeout and retry policies and per-thread session tracking.

    The timeout and retry policies are configured to turn on exponential backoff support provided by urllib3, and to
    make the client resistant to both network issues and intermittent server-side errors.

    The per-thread session tracking is to avoid many threads sharing the same session in multithreaded environments.
    """
    retry_policy = RetryPolicy(read=4,
                               status=4,
                               backoff_factor=0.1,
                               status_forcelist=frozenset({500, 502, 503, 504}))
    timeout_policy = timeout.Timeout(connect=20, read=40)
    max_redirects = 1024

    def __init__(self):
        self.sessions = {}

    def __call__(self, *args, **kwargs):
        if get_ident() not in self.sessions:
            session = requests.Session()
            session.max_redirects = self.max_redirects
            adapter = HTTPAdapter(max_retries=self.retry_policy)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions[get_ident()] = session
        return self.sessions[get_ident()].request(*args, timeout=self.timeout_policy, **kwargs)

    def head(self, url, *args, **kwargs):
        return self(url=url, method="HEAD", *args, **kwargs)

    def get(self, url, *args, **kwargs):
        return self(url=url, method="GET", *args, **kwargs)

    def put(self, url, *args, **kwargs):
        return self(url=url, method="PUT", *args, **kwargs)

    def post(self, url, *args, **kwargs):
        return self(url=url, method="POST", *args, **kwargs)

    def delete(self, url, *args, **kwargs):
        return self(url=url, method="DELETE", *args, **kwargs)

    def patch(self, url, *args, **kwargs):
        return self(url=url, method="PATCH", *args, **kwargs)

    def options(self, url, *args, **kwargs):
        return self(url=url, method="OPTIONS", *args, **kwargs)

http = HTTPRequest()
