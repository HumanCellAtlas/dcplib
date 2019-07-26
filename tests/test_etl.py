#!/usr/bin/env python
import tempfile
import unittest, io, os, sys, json, logging, concurrent.futures, shutil
from collections import defaultdict

import requests
from requests.models import Response

logger = logging.getLogger(__name__)

calls = defaultdict(int)


def tf(*args, **kwargs):
    logger.debug("Transformer called with %s %s", args, kwargs)
    calls["tf"] += 1
    return "TEST"


def ld(*args, **kwargs):
    logger.debug("Loader called with %s %s", args, kwargs)
    calls["ld"] += 1
    assert kwargs["bundle"] == "TEST"


def fn(*args, **kwargs):
    logger.debug("Finalizer called with %s %s", args, kwargs)
    calls["fn"] += 1


def pp(*args, **kwargs):
    logger.debug("Per Page callable with %s %s", args, kwargs)
    calls['pp'] += 1


class TestETLException(Exception):
    pass


def error_fn(*args, **kwargs):
    logger.debug("Finalizer called with %s %s, raising error", args, kwargs)
    raise TestETLException("test")


files = [
    {"name": hex(i), "content-type": "application/json", "uuid": hex(i), "version": "1", "sha256": "0"}
    for i in range(256)
]


class MockHTTPClient:
    status_code = requests.codes.ok

    def get(self, url, params):
        if "files" in url:
            payload = {}
        else:
            payload = {"bundle": {"files": files}}
        res = Response()
        res.status_code = self.status_code
        res.raw = io.BytesIO(json.dumps(payload).encode())
        return res


class MockDSSClient(MockHTTPClient):
    host = "localhost"

    class MockDSSMethod:
        def __call__(self, es_query, replica):
            return {"total_hits": 1}

        def iterate(self, es_query, replica, per_page):
            for i in range(4):
                yield {"bundle_fqid": "a%d.b" % i}

        def paginate(self, es_query, replica, per_page):
            for i in range(4):
                yield {"results": [{"bundle_fqid": "a{0}.{1}.b".format(i, j)} for j in range(per_page)]}

    post_search = MockDSSMethod()

    def __init__(self, swagger_url="swagger_url"):
        self.swagger_url = swagger_url


class TestETL(unittest.TestCase):

    def setUp(self):
        calls["tf"] = 0
        calls["fn"] = 0
        calls["ld"] = 0
        calls["pp"] = 0

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(
                staging_directory=td,
                content_type_patterns=["application/json"],
                filename_patterns=["*.json"],
                dss_client=MockDSSClient(),
                http_client=MockHTTPClient()
            )
            e.extract(query={"test": True},
                      max_workers=2,
                      max_dispatchers=1,
                      transformer=tf,
                      loader=ld,
                      finalizer=fn,
                      page_size=1)
        self.assertEqual(calls["tf"], 4)
        self.assertEqual(calls["ld"], 4)
        self.assertEqual(calls["fn"], 1)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl_with_dispatcher(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient())
            e.extract(
                query={"test": True},
                max_workers=2,
                max_dispatchers=1,
                dispatch_executor_class=concurrent.futures.ProcessPoolExecutor,
                transformer=tf,
                loader=ld,
                finalizer=fn,
                page_size=1
            )
        self.assertEqual(calls["tf"], 0)
        self.assertEqual(calls["ld"], 4)
        self.assertEqual(calls["fn"], 1)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl_raises_errors_correctly(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient())
            with self.assertRaises(TestETLException):
                e.extract(query={"test": True},
                          max_workers=2,
                          max_dispatchers=1,
                          transformer=tf,
                          loader=ld,
                          finalizer=error_fn,
                          page_size=1)

        with tempfile.TemporaryDirectory() as td2:
            e = dcplib.etl.DSSExtractor(staging_directory=td2,
                                        continue_on_bundle_extract_errors=True,
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient())
            e._http.status_code = 500
            e.extract(query={"test": True}, page_size=1)
            e._continue_on_bundle_extract_errors = False
            with self.assertRaises(requests.exceptions.HTTPError):
                e.extract(query={"test": True}, page_size=1)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl_handles_dispatch_on_empty_bundles_is_true(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient(),
                                        dispatch_on_empty_bundles=True)

            e.extract(query={"test": True}, max_workers=2, transformer=tf, loader=ld, finalizer=fn, page_size=1)
        self.assertEqual(calls["tf"], 4)
        self.assertEqual(calls["ld"], 4)
        self.assertEqual(calls["fn"], 1)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_per_page_callable_called_once_per_page(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient(),
                                        dispatch_on_empty_bundles=True)

            e.extract(query={"test": True}, max_workers=2, transformer=tf, loader=ld, finalizer=fn, page_processor=pp,
                      page_size=5)
        self.assertEqual(calls["tf"], 20)
        self.assertEqual(calls["ld"], 20)
        self.assertEqual(calls["fn"], 1)
        self.assertEqual(calls["pp"], 4)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl_handles_empty_bundles(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        # simulate an empty bundle by not loading any files
                                        content_type_patterns=["NOTHING/NOTHING"],
                                        filename_patterns=["*.not_real"],
                                        dss_client=MockDSSClient(),
                                        http_client=MockHTTPClient(),
                                        dispatch_on_empty_bundles=False
                                        )

            e.extract(query={"test": True}, max_workers=2, transformer=tf, loader=ld, finalizer=fn, page_size=2)
        self.assertEqual(calls["tf"], 0)
        self.assertEqual(calls["ld"], 0)
        self.assertEqual(calls["fn"], 1)


if __name__ == '__main__':
    unittest.main()
