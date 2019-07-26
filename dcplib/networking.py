import logging

try:
    from threading import get_ident
except ImportError:
    from thread import get_ident

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util import retry, timeout

logger = logging.getLogger(__name__)

class Session(requests.Session):
    """
    The purpose of this class is to patch Requests to obey Retry-After headers on redirect (300 series) HTTP responses.
    See https://github.com/HumanCellAtlas/dcp-cli/pull/341 for more information.
    """
    obey_retry_after = True
    def resolve_redirects(self, resp, req, **kwargs):
        if self.obey_retry_after and self.get_redirect_target(resp) and "Retry-After" in resp.headers:
            logger.warning("Waiting %ss before redirect per Retry-After header", resp.headers["Retry-After"])
            resp.connection.max_retries.sleep_for_retry(resp.raw)
        for rv in super(Session, self).resolve_redirects(resp, req, **kwargs):
            yield rv

class HTTPRequest:
    """
    A requests wrapper preconfigured with best practice timeout and retry policies and per-thread session tracking.

    The timeout and retry policies are configured to turn on exponential backoff support provided by urllib3, and to
    make the client resistant to both network issues and intermittent server-side errors.

    The per-thread session tracking is to avoid many threads sharing the same session in multithreaded environments.
    """
    retry_policy = retry.Retry(read=4,
                               status=4,
                               backoff_factor=0.1,
                               status_forcelist=frozenset({500, 502, 503, 504}))
    timeout_policy = timeout.Timeout(connect=20, read=40)
    codes = requests.codes

    def __init__(self, max_redirects=1024, obey_retry_after=True):
        self.sessions = {}
        self.max_redirects = max_redirects
        self.obey_retry_after = obey_retry_after

    def __call__(self, *args, **kwargs):
        if get_ident() not in self.sessions:
            session = Session()
            session.max_redirects = self.max_redirects
            session.obey_retry_after = self.obey_retry_after
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
