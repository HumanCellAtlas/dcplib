import tempfile
import unittest, io, sys, json, logging, concurrent.futures
from collections import defaultdict
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


class TestETLException(Exception):
    pass


def error_fn(*args, **kwargs):
    logger.debug("Finalizer called with %s %s, raising error", args, kwargs)
    raise TestETLException("test")


files = [
    {"name": hex(i), "content-type": "application/json", "uuid": hex(i), "version": "1", "sha256": "0"}
    for i in range(256)
]


class MockDSSClient:
    host = "localhost"
    swagger_url = "swagger_url"

    class MockDSSMethod:
        def __call__(self, es_query, replica):
            return {"total_hits": 1}

        def iterate(self, es_query, replica, per_page):
            for i in range(4):
                yield {"bundle_fqid": "a%d.b" % i}

        def paginate(self, es_query, replica, per_page):
            for i in range(4):
                yield {"results": [{"bundle_fqid": "a%d.b" % i}]}

    post_search = MockDSSMethod()

    def get(self, url, params):
        if "files" in url:
            payload = {}
        else:
            payload = {"bundle": {"files": files}}
        res = Response()
        res.status_code = 200
        res.raw = io.BytesIO(json.dumps(payload).encode())
        return res


class TestETL(unittest.TestCase):

    def setUp(self):
        import dcplib.etl
        dcplib.etl.http = MockDSSClient()

        calls["tf"] = 0
        calls["fn"] = 0
        calls["ld"] = 0

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:

            e = dcplib.etl.DSSExtractor(
                staging_directory=td,
                content_type_patterns=["application/json"],
                filename_patterns=["*.json"],
                dss_client=MockDSSClient()
            )
            e.extract(query={"test": True},
                      max_workers=2,
                      max_dispatchers=1,
                      transformer=tf,
                      loader=ld,
                      finalizer=fn)
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
                                        dss_client=MockDSSClient()
                                        )
            e.extract(
                query={"test": True},
                max_workers=2,
                max_dispatchers=1,
                dispatch_executor_class=concurrent.futures.ProcessPoolExecutor,
                transformer=tf,
                loader=ld,
                finalizer=fn
            )
        self.assertEqual(calls["tf"], 0)
        self.assertEqual(calls["ld"], 4)
        self.assertEqual(calls["fn"], 1)

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def new_etl_raises_error_correctly(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:

            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient()
                                        )
            with self.assertRaises(TestETLException):
                e.extract(query={"test": True},
                          max_workers=2,
                          max_dispatchers=1,
                          transformer=tf,
                          loader=ld,
                          finalizer=error_fn
                          )

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl_handles_dispatch_on_empty_bundles_is_true(self):
        import dcplib.etl
        with tempfile.TemporaryDirectory() as td:

            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        dispatch_on_empty_bundles=True
                                        )

            e.extract(query={"test": True}, max_workers=2, transformer=tf, loader=ld, finalizer=fn)
        self.assertEqual(calls["tf"], 4)
        self.assertEqual(calls["ld"], 4)
        self.assertEqual(calls["fn"], 1)


if __name__ == '__main__':
    unittest.main()
