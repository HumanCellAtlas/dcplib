import os
import unittest
from unittest import mock

from dcplib import errors
from dcplib import security
from dcplib.security import authn, DCPServiceAccountManager


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

    @mock.patch('dcplib.security.Config._trusted_google_projects', new=['test.iam.gserviceaccount.com'])
    @mock.patch('dcplib.security.Config._openid_provider', new=['fake_provider.test'])
    def test_authorized_issuer(self):
        valid_issuers = [
            {'iss': security.Config.get_openid_provider()},
            {'iss': "travis-test@test.iam.gserviceaccount.com",
             'sub': "travis-test@test.iam.gserviceaccount.com"}
        ]
        for issuer in valid_issuers:
            with self.subTest(issuer):
                authn.assert_authorized_issuer(issuer)

    def test_not_authorized_issuer(self):
        invalid_issuers = [{'iss': "https://project.auth0.com/"},
                           {'iss': "travis-test@test.iam.gserviceaccount.com",
                            'sub': "travis-test@test.iam.gserviceaccount.com"}
                           ]
        for issuer in invalid_issuers:
            with self.subTest(issuer):
                with self.assertRaises(errors.AuthenticationException):
                    authn.assert_authorized_issuer(issuer)

    @mock.patch('dcplib.security.Config._trusted_google_projects',
                new=['human-cell-atlas-travis-test.iam.gserviceaccount.com'])
    def test_verify_jwt_audience(self):
        audiences_positive = [
            [["something else"], ["something else"]],
            [["something", "e"], ["something", "e"]],
            [["something", "e"], ["something"]],
            [["something"], ["something", "e"]]
        ]
        audiences_negative = [
            [["something else"], ["something"]],
            [["something", "else"], ["something else"]]
        ]
        for audience, expected in audiences_positive:
            with self.subTest("Positive: " + str(audience)):
                jwt = self.gcp_creds.create_jwt(audience)
                authn.decode_jwt(jwt, expected)
        for audience, expected in audiences_negative:
            with self.subTest("Negative: " + str(audience)):
                jwt = self.gcp_creds.create_jwt(audience)
                self.assertRaises(errors.AuthenticationException, authn.decode_jwt, jwt, expected)

    def test_negative_verify_jwt(self):
        jwts = [self.gcp_creds.get_jwt()]
        for jwt in jwts:
            with self.subTest(jwt):
                with self.assertRaises(errors.AuthenticationException):
                    authn.decode_jwt(jwt, ["https://data.humancellatlas.org/"])

    def test_custom_email_claims(self):
        email = 'email@email.com'
        email_claim = 'email@claim.com'
        tests = [
            ({'email': email, security.OIDC_claims['email']: email_claim}, email_claim),
            ({security.OIDC_claims['email']: 'email@claim.com'}, email_claim),
            ({'email': email}, email)
        ]

        for param, result in tests:
            with self.subTest("no custom claim " + str(param)):
                self.assertEqual(authn.get_email_claim(param), result)

        os.environ['OIDC_EMAIL_CLAIM'] = 'TEST_CLAIM'
        for param, result in tests:
            with self.subTest("custom claim " + str(param)):
                self.assertEqual(authn.get_email_claim(param), result)

        with self.subTest("missing claim"):
            with self.assertRaises(errors.AuthenticationException):
                authn.get_email_claim({})
