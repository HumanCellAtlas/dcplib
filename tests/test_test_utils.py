import unittest, io, sys

class TestTestHelpers(unittest.TestCase):
    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_test_helpers(self):
        import requests.models
        from dcplib.test_helpers import DCPAssertMixin, ExpectedErrorFields
        from flask import wrappers
        from unittest.mock import MagicMock

        class MockFlaskApp:
            def post(*args, **kwargs):
                return wrappers.Response("{}", status=200, headers={"content-type": "application/json"})

        self.app = MockFlaskApp()
        DCPAssertMixin.assertResponse(self, "post", "/", 200)
        DCPAssertMixin.assertResponse(self, "post", "/", [200, 300])
        DCPAssertMixin.assertResponse(self, "post", "/", 200, {})

        class MockRequestsApp:
            def post(*args, **kwargs):
                res = requests.models.Response()
                res.status_code = 200
                res.raw = io.BytesIO(b"{}")
                res.headers["content-type"] = "application/json"
                return res

        self.app = MockRequestsApp()
        DCPAssertMixin.assertResponse(self, "post", "/", 200)
        DCPAssertMixin.assertResponse(self, "post", "/", [200, 300])
        DCPAssertMixin.assertResponse(self, "post", "/", 200, {})

if __name__ == '__main__':
    unittest.main()
