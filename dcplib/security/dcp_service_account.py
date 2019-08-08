import json
from typing import List, Dict

import jwt
import time

from ..aws import clients

sm = clients.secretsmanager


class DCPServiceAccount:
    _default_renew_buffer = 30
    # If the token will expire in less than this amount of time in seconds, generate a new token.
    _default_lifetime = 3600
    # How long the token should live.
    _options = {
        'verify_signature': False,
        'verify_aud': False,
        'verify_iss': False
    }

    # options used to check expiration of token.

    @classmethod
    def from_secrets_manager(cls, deployment, service, name, **kwargs):
        secret_id = '/'.join(['/dcp', deployment, service, name])
        response = sm.get_secret_value(SecretId=secret_id)
        credentials = json.loads(response['SecretString'])
        return cls.__init__(credentials, **kwargs)

    def __init__(self,
                 google_service_account_credentials: dict,
                 audience: List[str],
                 additional_claims: Dict = None,
                 renew_buffer: int = None,
                 lifetime: int = None):
        """

        :param google_service_account_credentials: a json dict containing google service account credentials.
        :param renew_buffer:
        :param lifetime:
        """

        self.credentials: dict = google_service_account_credentials
        self.audience = audience
        self.additional_claims = additional_claims
        self.renew_buffer: int = renew_buffer or self._default_renew_buffer
        self.lifetime: int = lifetime or self._default_lifetime
        self.token = None

    def get_header(self):
        return {"Authorization": f"Bearer {self.get_jwt()}"}

    def get_jwt(self):
        """
        Generate and manages JWT token. A new JWT is minted when the current JWT expires.
        """
        if self.token is None or self.is_expired(self.token):
            self.token = self.create_jwt(self.audience, self.additional_claims)
        return self.token

    def create_jwt(self, audience, additional_claims=None):
        """
        Creates a JWT using the provided audience and additional_claims

        :param audience:
        :param additional_claims:
        :return:
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

    def is_expired(self, token):
        try:
            decoded_token = jwt.decode(token, options=self._options)
        except jwt.ExpiredSignatureError:  # type: ignore
            return True
        else:
            if decoded_token['exp'] - time.time() <= self.renew_buffer:
                # If the token will expire in less than cls._renew_buffer amount of time in seconds, the token is
                # considered expired.
                return True
            else:
                return False
