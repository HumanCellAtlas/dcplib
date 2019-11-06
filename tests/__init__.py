import os
import uuid

import boto3


class EnvironmentSetup:
    """
    Set environment variables.
    Provide a dict of variable names and values.
    Setting a value to None will delete it from the environment.
    """
    def __init__(self, env_vars_dict):
        self.env_vars = env_vars_dict
        self.saved_vars = {}

    def enter(self):
        for k, v in self.env_vars.items():
            if k in os.environ:
                self.saved_vars[k] = os.environ[k]
            if v:
                os.environ[k] = v
            else:
                if k in os.environ:
                    del os.environ[k]

    def exit(self):
        for k, v in self.saved_vars.items():
            os.environ[k] = v

    def __enter__(self):
        self.enter()

    def __exit__(self, type, value, traceback):
        self.exit()


def fixture_file_path(filename):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures', filename))


class ExistingAwsSecretTestFixture:

    """
    A test fixture to manage an AWS Secrets Manager secret.
    It is a context manager that will instantiate and tear down the secret around your code.
    Use it with:

        with ExistingAwsSecretTestFixture(secret_name="foo", secret_value="bar") as secret:
            print("My name is {secret.name}")
            # test something

    If you leave the secret_name blank, a unique name will be generated for you.
    """

    SECRET_ID_TEMPLATE = 'dcplib/test/test_secret/{}'
    EXISTING_SECRET_DEFAULT_VALUE = '{"top":"secret"}'

    def __init__(self, secret_name=None, secret_value=None):
        self.secrets_mgr = boto3.client("secretsmanager")
        self.name = secret_name or self.SECRET_ID_TEMPLATE.format(uuid.uuid4())
        self._value = secret_value or self.EXISTING_SECRET_DEFAULT_VALUE
        self._secret = None

    def __enter__(self):
        self._secret = self.secrets_mgr.create_secret(Name=self.name,
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
