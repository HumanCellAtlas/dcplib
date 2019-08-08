import time
import unittest

from dcplib import security
from dcplib.security import DCPServiceAccount


class TestAuthn(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        security.Config.set_config('https://openid_provider.test',
                                   'https://auth_url.test',
                                   ['travis-test@test.iam.gserviceaccount.com'])

    def test_expired(self):
        gcp_creds = DCPServiceAccount.from_secrets_manager(
            DCPServiceAccount.format_secret_id(
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

