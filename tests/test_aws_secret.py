import sys
import unittest
import uuid

from mock import patch

import boto3

from dcplib.aws_secret import AwsSecret


@unittest.skipIf(sys.version_info < (3, 6), "Only testing under Python 3.6+")
class TestAwsSecret(unittest.TestCase):

    UNKNOWN_SECRET = 'dcplib/test/secret_that_does_not_exist'  # Don't ever create this

    class ExistingSecretTestFixture:

        SECRET_ID_TEMPLATE = 'dcplib/test/test_secret/{}'
        EXISTING_SECRET_DEFAULT_VALUE = '{"top":"secret"}'

        def __init__(self):
            self.secrets_mgr = boto3.client("secretsmanager")
            self.secret_id = self.SECRET_ID_TEMPLATE.format(uuid.uuid4())
            self._secret = None
            self._value = None

        def __enter__(self, secret_value=None):
            self._value = secret_value or self.EXISTING_SECRET_DEFAULT_VALUE
            self._secret = self.secrets_mgr.create_secret(Name=self.secret_id,
                                                          SecretString=self._value)
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.delete()

        @property
        def value(self):
            return self._value

        @property
        def arn(self):
            return self._secret['ARN']

        def delete(self):
            self.secrets_mgr.delete_secret(SecretId=self.arn, ForceDeleteWithoutRecovery=True)

    @classmethod
    def setUpClass(cls):
        # To reduce eventual consistency issues, get everyone using the same Secrets Manager session
        cls.secrets_mgr = boto3.client("secretsmanager")
        cls.patcher = patch('dcplib.aws_secret.boto3.client')
        boto3_client = cls.patcher.start()
        boto3_client.return_value = cls.secrets_mgr

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()

    def test_init_of_unknown_secret_does_not_set_secret_metadata(self):
        secret = AwsSecret(name=self.UNKNOWN_SECRET)
        self.assertEqual(secret.secret_metadata, None)

    def test_init_of_existing_secret_retrieves_secret_metadata(self):
        with self.ExistingSecretTestFixture() as existing_secret:
            secret = AwsSecret(name=existing_secret.secret_id)
            self.assertIsNotNone(secret.secret_metadata)

    # value

    def test_value_of_unknown_secret_raises_exception(self):
        with self.ExistingSecretTestFixture():
            secret = AwsSecret(name=self.UNKNOWN_SECRET)
            with self.assertRaisesRegex(RuntimeError, 'No such'):
                secret.value  # noqa

    def test_value_of_existing_deleted_secret_raises_exception(self):
        with self.ExistingSecretTestFixture() as existing_secret:
            secret = AwsSecret(name=existing_secret.secret_id)
            secret.delete()
            with self.assertRaisesRegex(RuntimeError, 'deleted'):
                x = secret.value  # noqa

    def test_value_of_existing_secret_returns_value(self):
        with self.ExistingSecretTestFixture() as existing_secret:
            secret = AwsSecret(name=existing_secret.secret_id)
            self.assertEqual(secret.value, existing_secret.value)

    # update

    def test_delete_of_unknown_secret_raises_exception(self):
        secret = AwsSecret(name=self.UNKNOWN_SECRET)
        with self.assertRaises(RuntimeError):
            secret.delete()

    def test_update_of_existing_secret_updates_secret(self):
        with self.ExistingSecretTestFixture() as existing_secret:
            secret = AwsSecret(name=existing_secret.secret_id)
            secret.update(value='{"foo":"bar"}')
            self.assertEqual(self.secrets_mgr.get_secret_value(SecretId=existing_secret.arn)['SecretString'],
                             '{"foo":"bar"}')

    # delete

    def test_delete_of_existing_secret_deletes_secret(self):
        with self.ExistingSecretTestFixture() as existing_secret:
            secret = AwsSecret(name=existing_secret.secret_id)
            secret.delete()
            secret = self.secrets_mgr.describe_secret(SecretId=existing_secret.arn)
            self.assertIn('DeletedDate', secret)
