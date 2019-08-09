import json
import time
from typing import List, Dict

import jwt

from ..aws_secret import AwsSecret


class DCPServiceAccountManager:
    """
    Manage the minting of JWT for making authenticated request from DCP service account.
    """
    _default_renew_buffer = 30
    _default_lifetime = 3600
    _options = {
        'verify_signature': False,
        'verify_aud': False,
        'verify_iss': False
    }

    # options used to check expiration of token.

    @classmethod
    def from_secrets_manager(cls, secret_id, audience, **kwargs):
        """
        Retrieve the secretId stored in AWS secrets manager and create a DCPServiceAccountManager.

        :param secret_id: The secretId store in AWS secrets manager
        :param audience: The audience to use in the JWT
        :param kwargs: arguments passed to the DCPServiceAccountManager.__init__ method
        :return:
        """
        credentials = json.loads(AwsSecret(secret_id).value)
        return cls(credentials, audience, **kwargs)

    @staticmethod
    def format_secret_id(secret_name, deployment='', service=''):
        return '/'.join([i for i in ['dcp', service, deployment, secret_name] if i])

    def __init__(self,
                 google_service_account_credentials: dict,
                 audience: List[str],
                 additional_claims: Dict = None,
                 renew_buffer: int = None,
                 lifetime: int = None):
        """

        :param google_service_account_credentials: a json dict containing google service account credentials.
        :param audience: the audience used for the managed JWT.
        :param additional_claims: additional claims added to the managed JWT.
        :param renew_buffer: (seconds) If the JWT will expire in less than this amount of time, generate a new JWT.
        :param lifetime: (seconds) how long the token should live.
        """

        self.credentials = google_service_account_credentials
        self.audience = audience
        self.additional_claims = additional_claims
        self.renew_buffer = renew_buffer or self._default_renew_buffer
        self.lifetime = lifetime or self._default_lifetime
        self.token = None

    def get_header(self):
        """
        Generate a new JWT if the old JWT has expired and returns an authorization header for making authenticated
        requests to DCP components.

        :return: authorization header
        """
        return {"Authorization": "Bearer {}".format(self.get_jwt())}

    def create_header(self, **kwargs):
        """
        Generates a new JWT and returns it in an authorization header for making authenticated requests to DCP
        components.
        :param kwargs: arguments passed through to create_jwt
        :return: authorization header
        """
        return {"Authorization": "Bearer {}".format(self.create_jwt(**kwargs))}

    def get_jwt(self):
        """
        Generate and manages JWT token. A new JWT is minted when the current JWT expires.
        """
        if self.token is None or self.is_expired(self.token):
            self.token = self.create_jwt(self.audience, self.additional_claims)
        return self.token

    def create_jwt(self, audience: List[str], additional_claims=None):
        """
        Creates a JWT using the provided audience and additional_claims

        :param audience:
        :param additional_claims:
        :return: str
        """
        iat = time.time()
        exp = iat + self.lifetime
        payload = additional_claims or {}
        payload.update({'iss': self.credentials["client_email"],
                        'sub': self.credentials["client_email"],
                        'aud': audience,
                        'iat': iat,
                        'exp': exp,
                        'scope': ['email', 'openid', 'offline_access'],
                        'email': self.credentials["client_email"]
                        })
        additional_headers = {'kid': self.credentials["private_key_id"]}
        token = jwt.encode(
            payload,
            self.credentials["private_key"],
            headers=additional_headers,
            algorithm='RS256').decode()
        return token

    def is_expired(self, token: str):
        """
        Check if the provided token has expired.

        :param token:
        :return: bool
        """
        try:
            decoded_token = jwt.decode(token, options=self._options)
        except jwt.ExpiredSignatureError:  # type: ignore
            return True
        else:
            if decoded_token['exp'] - time.time() >= self.renew_buffer:
                # If the token will expire in less than cls._renew_buffer amount of time in seconds, the token is
                # considered expired.
                return True
            else:
                return False
