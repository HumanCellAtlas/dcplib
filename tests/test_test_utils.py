import unittest, io
from unittest.mock import MagicMock

class TestTestHelpers(unittest.TestCase):
    def test_test_helpers(self):
        from dcplib.test_helpers import DCPAssertMixin, ExpectedErrorFields
        from flask import wrappers
        import requests.models

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
