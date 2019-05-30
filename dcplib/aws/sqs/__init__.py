from uuid import uuid4
from functools import lru_cache

from contextlib import AbstractContextManager

from dcplib.aws.clients import sqs  # type: ignore

class SQSMessenger(AbstractContextManager):
    """
    Context manager for batch SQS delivery
    """
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.chunk = list()

    def send(self, body, delay_seconds=None):
        msg = dict(Id=str(uuid4()), MessageBody=body)
        if delay_seconds is not None:
            msg['DelaySeconds'] = delay_seconds
        self.chunk.append(msg)
        if 10 == len(self.chunk):
            _send_message_batch(self.queue_url, self.chunk)
            self.chunk = list()

    def __exit__(self, *args, **kwargs):
        if len(self.chunk):
            _send_message_batch(self.queue_url, self.chunk)

def _send_message_batch(queue_url, entries):
    assert 10 >= len(entries)
    resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    return resp

@lru_cache()
def get_queue_url(queue_name):
    return sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
