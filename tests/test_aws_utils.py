import unittest

from dcplib.aws import ARN, clients, resources


class TestAWSUtils(unittest.TestCase):

    def test_aws_utils(self):
        clients.sts.get_caller_identity()
        resources.s3.Bucket("foo").Object("bar")
        ARN().get_region()
        ARN().get_account_id()
