import unittest, time

from threading import Thread

from flask import Flask, Response, jsonify
import requests

from dcplib.networking import http

class MockServer(Thread):
    def __init__(self, port=5000):
        super(MockServer, self).__init__()
        self.port = port
        self.app = Flask(__name__)
        self.url = "http://localhost:%s" % self.port
        self.app.add_url_rule("/shutdown", view_func=self._shutdown_server)
        self.ncalls = 0

        @self.app.route('/')
        def index():
            self.ncalls += 1
            return Response(status=301, headers={"Location": "/target", "Retry-After": "2"})

        @self.app.route('/target')
        def target():
            return jsonify(ncalls=self.ncalls)

    def _shutdown_server(self):
        from flask import request
        request.environ['werkzeug.server.shutdown']()
        return 'Shutting down'

    def stop(self):
        requests.get("http://localhost:%s/shutdown" % self.port)
        self.join()

    def run(self):
        self.app.run(port=self.port)


class TestNetworking(unittest.TestCase):
    def setUp(self):
        self.server = MockServer()
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_networking(self):
        res = http(method="GET", url="https://google.com")
        res.raise_for_status()
        res = http.get("https://google.com")
        res.raise_for_status()
        assert res.status_code == http.codes.ok

    def test_redirect_with_retry_after(self):
        start_time = time.time()
        res = http.get(self.server.url)
        end_time = time.time()
        self.assertGreaterEqual(end_time - start_time, 2.0)
        self.assertLessEqual(end_time - start_time, 8.0)
        self.assertEqual(res.json(), {"ncalls": 1})

if __name__ == '__main__':
    unittest.main()
