import ast
import unittest

import boto3
from moto import mock_s3, mock_sqs

from dcplib.aws.sqs_handler import SQSHandler


class TestSQSHandler(unittest.TestCase):
    """ Unit tests for the SQSHandler class """

    def setUp(self):
        # Start S3 mock
        self.s3_mock = mock_s3()
        self.s3_mock.start()
        self.sqs_mock = mock_sqs()
        self.sqs_mock.start()

        # Setup SQS
        self.sqs_queue_name = "test_query_job_q_name"
        self.sqs = boto3.resource('sqs')
        self.sqs.create_queue(QueueName=self.sqs_queue_name)

        self.sqs_handler = SQSHandler(queue_name=self.sqs_queue_name)
        self.sqs.meta.client.purge_queue(QueueUrl=self.sqs_queue_name)

    def tearDown(self):
        self.s3_mock.stop()
        self.sqs_mock.stop()

    def test__add_message_to_queue__successful(self):
        payload = {'test_key': 'test_value'}

        self.sqs_handler.add_message_to_queue(payload)

        messages = self.sqs_handler.receive_messages_from_queue()
        self._assert_payload_and_message_are_equal(payload, messages)

    def test__receive_messages_from_queue__returns_none_when_no_messages_found(self):
        message = self.sqs_handler.receive_messages_from_queue(wait_time=1)
        self.assertFalse(message)

    def test__receive_messages_from_queue__returns_message_when_message_is_found(self):
        payload = {'test_key': 'test_value'}
        self.sqs_handler.add_message_to_queue(payload)

        messages = self.sqs_handler.receive_messages_from_queue()

        self._assert_payload_and_message_are_equal(payload, messages)

    def test__receive_messages_from_queue__returns_multiple_messages(self):
        payload_1 = {'test_key_1': 'test_value_1'}
        payload_2 = {'test_key_2': 'test_value_2'}
        self.sqs_handler.add_message_to_queue(payload_1)
        self.sqs_handler.add_message_to_queue(payload_2)

        messages = self.sqs_handler.receive_messages_from_queue(num_messages=2)

        payload_1.update(payload_2)
        self._assert_payload_and_message_are_equal(payload_1, messages)

    def test__delete_message_from_queue__successful(self):
        # Add a message to the queue and receive it, noting the receipt handler
        payload = {'test_key': 'test_value'}
        self.sqs_handler.add_message_to_queue(payload)
        messages = self.sqs_handler.receive_messages_from_queue()
        receipt_handle = messages[0].receipt_handle

        # Delete message
        self.sqs_handler.delete_message_from_queue(receipt_handle)

        # Verify that there are no more messages in the queue
        message = self.sqs_handler.receive_messages_from_queue(wait_time=1)
        self.assertFalse(message)

    def test__add_message_to_queue__instantiated_with_nothing_throws_exception(self):
        with self.assertRaises(Exception) as exception:
            # Attempt to create an SQSHandler object with neither queue_name nor queue_url specified
            SQSHandler()
        self.assertTrue("Expected either queue_name or queue_url to be specified" in str(exception.exception))

    def _assert_payload_and_message_are_equal(self, payload, messages):
        for message in messages:
            message_body = ast.literal_eval(message.body)
            for key in message_body:
                self.assertTrue(key in payload)
                self.assertEqual(message_body[key], payload[key])
