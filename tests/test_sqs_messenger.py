import sys
import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import dcplib

class TestSQS(unittest.TestCase):
    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_message_queuer(self):
        from dcplib.aws.sqs import SQSMessenger

        batches = list()
        delay_seconds = 123456

        def mock_send_batch(queue_url, entries):
            for e in entries:
                if "DelaySeconds" in e:
                    self.assertEqual(e['DelaySeconds'], delay_seconds)
            batches.append([e['MessageBody'] for e in entries])

        with patch("dcplib.aws.sqs._send_message_batch", mock_send_batch):
            with SQSMessenger("foo") as sqsm:
                for i in range(21):
                    if i == 20:
                        sqsm.send(str(i), delay_seconds)
                    else:
                        sqsm.send(str(i))

        self.assertEqual(batches[0], [str(i) for i in range(10)])
        self.assertEqual(batches[1], [str(i) for i in range(10, 20)])
        self.assertEqual(batches[2], [str(i) for i in range(20, 21)])

    @unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
    def test_send_message_batch(self):
        from dcplib.aws.sqs import _send_message_batch
        with self.assertRaises(AssertionError):
            _send_message_batch("foo", ["foo" for _ in range(20)])
