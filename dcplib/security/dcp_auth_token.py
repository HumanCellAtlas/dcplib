from typing import List, Dict

import jwt
import time


class DCP_Auth_Token:
    """
    Generate and manages JWT tokens. A new JWT is minted when the current JWT expires.
    """
    _default_renew_buffer = 30
    # If the token will expire in less than this amount of time in seconds, generate a new token.
    _default_lifetime = 3600
    # How long the token should live.
    _options = {
        'verify_signature': False,
        'verify_aud': False,
        'verify_iss': False
    }

    def __init__(self, google_service_account_credentials, renew_buffer=None, lifetime=None):
        self.credentials = google_service_account_credentials
        self.renew_buffer = renew_buffer or self._default_renew_buffer
        self.lifetime = lifetime or self._default_lifetime
        self.token = None

    def get_header(self, *args):
        return {"Authorization": "Bearer {jwt}".format(jwt=self.get_service_jwt(*args))}

    def get_service_jwt(self, audience: List[str], additional_claims: Dict = None):
        token = self.token.get((audience, additional_claims))
        if token is None or self.is_expired(token):
            # create a JWT
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
            self.token[(audience, additional_claims)] = jwt.encode(
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
