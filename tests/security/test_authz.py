import json
import unittest
from unittest import mock

from requests import Response

from dcplib import security
from dcplib.errors import AuthorizationException
from dcplib.security import authz, DCPServiceAccountManager


class TestAuthn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        security.Config.setup(['dummy.iam.gserviceaccount.com'],
                              openid_provider='https://openid_provider.test',
                              auth_url='https://auth_url.test',
                              )
        cls.gcp_creds = DCPServiceAccountManager.from_secrets_manager(
            DCPServiceAccountManager.format_secret_id(
                'gcp_credentials.json',
                deployment='test',
                service='dcplib'
            ),
            audience=["https://data.humancellatlas.org/"])

    @mock.patch('dcplib.security.authz.session.post')
    def test_authorized(self, mocked_class):
        test_token_info = {'https://auth.data.humancellatlas.org/email': 'fake_email@somewhere.somewhere'}
        resp = Response()
        resp.encoding = 'utf-8'
        resp.status_code = 200
        mocked_class.return_value = resp

        @authz.authorize(self.gcp_creds, ['test:Action'], ['aws:test:resource:place:21351345134'])
        def _test(token_info: dict):
            pass

        resp._content = json.dumps({'result': True}).encode('utf-8')
        with self.subTest("result = True"):
            _test(token_info=test_token_info)

        resp._content = json.dumps({'result': False}).encode('utf-8')
        with self.subTest("result = False"):
            with self.assertRaises(AuthorizationException):
                _test(token_info=test_token_info)

        resp.status_code = 500
        with self.subTest("Request failed"):
            with self.assertRaises(AuthorizationException):
                _test(token_info=test_token_info)
