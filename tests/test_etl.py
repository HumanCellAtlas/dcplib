import unittest, io, sys, json, tempfile, logging, concurrent.futures
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
    assert kwargs["bundles"]


def fn(*args, **kwargs):
    logger.debug("Finalizer called with %s %s", args, kwargs)
    calls["fn"] += 1


class TestETLException(Exception):
    pass


def error_fn(*args, **kwargs):
    logger.debug("Finalizer called with %s %s, raising error", args, kwargs)
    raise TestETLException("test")


class TestETL(unittest.TestCase):
    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_etl(self):
        from unittest.mock import MagicMock, patch
        import dcplib.etl

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

        dcplib.etl.http = MockDSSClient()
        with tempfile.TemporaryDirectory() as td:
            e = dcplib.etl.DSSExtractor(
                staging_directory=td,
                content_type_patterns=["application/json"],
                filename_patterns=["*.json"],
                dss_client=MockDSSClient(),
                transformer=tf,
                loader=ld,
                finalizer=fn
            )
            e.etl_bundles(query={"test": True},
                          max_workers=2,
                          max_dispatchers=1)
            self.assertEqual(calls["tf"], 4)
            self.assertEqual(calls["ld"], 1)
            self.assertEqual(calls["fn"], 1)

            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        transformer=tf,
                                        loader=ld,
                                        finalizer=fn)
            e.etl_bundles(
                query={"test": True},
                max_workers=2,
                max_dispatchers=1,
                dispatch_executor_class=concurrent.futures.ProcessPoolExecutor)
            # TODO why is this 8
            self.assertEqual(calls["tf"], 8)
            self.assertEqual(calls["ld"], 2)
            self.assertEqual(calls["fn"], 2)

            e = dcplib.etl.DSSExtractor(staging_directory=td,
                                        content_type_patterns=["application/json"],
                                        filename_patterns=["*.json"],
                                        dss_client=MockDSSClient(),
                                        transformer=tf,
                                        loader=ld,
                                        finalizer=error_fn)
            with self.assertRaises(TestETLException):
                e.etl_bundles(query={"test": True},
                              max_workers=2,
                              max_dispatchers=1)

        with tempfile.TemporaryDirectory() as td:
            files = []
            for dispatch_on_empty_bundles in True, False:
                e = dcplib.etl.DSSExtractor(staging_directory=td,
                                            content_type_patterns=["application/json"],
                                            filename_patterns=["*.json"],
                                            dss_client=MockDSSClient(),
                                            dispatch_on_empty_bundles=dispatch_on_empty_bundles,
                                            transformer=tf,
                                            loader=ld,
                                            finalizer=fn)

                e.etl_bundles(query={"test": True}, max_workers=2)
            self.assertEqual(calls["tf"], 16)
            self.assertEqual(calls["ld"], 5)
            self.assertEqual(calls["fn"], 4)


if __name__ == '__main__':
    unittest.main()
