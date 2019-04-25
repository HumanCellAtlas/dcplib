import json

from . import resources, clients


class SQSHandler:
    """
    A class encapsulating the behaviors associated with interacting with an SQS Queue object and raising errors when
    queue-related behaviors fail.
    """

    def __init__(self, queue_name=None, queue_url=None):
        if queue_name:
            self.queue = resources.sqs.Queue(clients.sqs.get_queue_url(QueueName=queue_name)['QueueUrl'])
        elif queue_url:
            self.queue = resources.sqs.Queue(queue_url)
        else:
            raise Exception("Expected either queue_name or queue_url to be specified")

    def add_message_to_queue(self, payload, **attributes):
        """ Given a payload (a dict) and any optional message attributes (also a dict), add it to the queue. """

        return self.queue.send_message(MessageBody=json.dumps(payload),
                                       MessageAttributes={k: {"StringValue": v} for k, v in attributes.items()})

    def receive_messages_from_queue(self, wait_time=15, num_messages=1):
        """ Returns the first (according to FIFO) element in the queue; if none, then returns None."""

        return self.queue.receive_messages(MaxNumberOfMessages=num_messages,
                                           WaitTimeSeconds=wait_time)

    def delete_message_from_queue(self, receipt_handle):
        """ Deletes the specified element from the queue if it exists and does nothing otherwise. """

        self.queue.delete_messages(Entries=[{'Id': '12345', 'ReceiptHandle': receipt_handle}])
