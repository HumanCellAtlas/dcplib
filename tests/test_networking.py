import unittest

from dcplib.networking import http

class TestNetworking(unittest.TestCase):
    def test_networking(self):
        res = http(method="GET", url="https://google.com")
        res.raise_for_status()
        res = http.get("https://google.com")
        res.raise_for_status()
