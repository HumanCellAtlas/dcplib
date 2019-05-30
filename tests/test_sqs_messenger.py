import os
import sys
import unittest
from unittest import mock

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dcplib
from dcplib.aws.sqs import SQSMessenger, _send_message_batch

class TestSQS(unittest.TestCase):
    def test_message_queuer(self):
        batches = list()

        def mock_send_batch(queue_url, entries):
            batches.append([e['MessageBody'] for e in entries])

        with mock.patch("dcplib.aws.sqs._send_message_batch", mock_send_batch):
            with SQSMessenger("foo") as sqsm:
                for i in range(21):
                    sqsm.send(str(i))

        self.assertEqual(batches[0], [str(i) for i in range(10)])
        self.assertEqual(batches[1], [str(i) for i in range(10, 20)])
        self.assertEqual(batches[2], [str(i) for i in range(20, 21)])

    def test_send_message_batch(self):
        with self.assertRaises(AssertionError):
            _send_message_batch("foo", ["foo" for _ in range(20)])


if __name__ == "__main__":
    unittest.main()
