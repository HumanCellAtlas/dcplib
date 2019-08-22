import time
import unittest

from dcplib import security
from dcplib.security import DCPServiceAccountManager
from dcplib.security import authn


class TestAuthn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        security.Config.setup(["human-cell-atlas-travis-test.iam.gserviceaccount.com"],
                              openid_provider='https://openid_provider.test',
                              auth_url='https://auth_url.test',
                              )

    def test_expired(self):
        gcp_creds = DCPServiceAccountManager.from_secrets_manager(
            DCPServiceAccountManager.format_secret_id(
                'gcp_credentials.json',
                deployment='test',
                service='dcplib'
            ),
            audience=["https://data.humancellatlas.org/"],
            lifetime=1,
            renew_buffer=0
        )

        jwt = gcp_creds.get_jwt()
        with self.subTest('Token not expired'):
            self.assertFalse(gcp_creds.is_expired(jwt))
        time.sleep(2)
        with self.subTest('Token expired'):
            self.assertTrue(gcp_creds.is_expired(jwt))

    def test_positive(self):
        gcp_creds = DCPServiceAccountManager.from_secrets_manager(
            DCPServiceAccountManager.format_secret_id(
                'gcp_credentials.json',
                deployment='test',
                service='dcplib'
            ),
            audience=["https://data.humancellatlas.org/"],
        )

        jwt = gcp_creds.get_jwt()
        token_info = authn.decode_jwt(jwt, ["https://data.humancellatlas.org/"])
        email = authn.get_email_claim(token_info)
        self.assertEqual(email, "dcplib-test@human-cell-atlas-travis-test.iam.gserviceaccount.com")
