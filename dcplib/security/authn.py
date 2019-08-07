"""
Generate and verify JWTS
"""

import base64
import functools
import typing

import jwt
import requests
import time
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from furl import furl

from ..errors import AuthenticationException, AuthorizationException
from ..security import Config

allowed_algorithms = ['RS256']
gserviceaccount_domain = "iam.gserviceaccount.com"

# recycling the same session for all requests.
session = requests.Session()


@functools.lru_cache(maxsize=32)
def get_openid_config(openid_provider):
    """

    :param openid_provider: the openid provider's domain.
    :return: the openid configuration
    """
    if openid_provider.endswith(gserviceaccount_domain):
        openid_provider = 'accounts.google.com'
    elif openid_provider.startswith("https://"):
        openid_provider = furl(openid_provider).host
    res = requests.get("https://{op}/.well-known/openid-configuration".format(op=openid_provider))
    res.raise_for_status()
    return res.json()


def get_jwks_uri(openid_provider):
    if openid_provider.endswith(gserviceaccount_domain):
        return "https://www.googleapis.com/service_accounts/v1/jwk/{op}".format(op=openid_provider)
    else:
        return get_openid_config(openid_provider)["jwks_uri"]


@functools.lru_cache(maxsize=32)
def get_public_keys(openid_provider):
    """
    Fetches the public key from an OIDC Identity provider to verify the JWT.
    :param openid_provider: the openid provider's domain.
    :return: Public Keys
    """
    keys = session.get(get_jwks_uri(openid_provider)).json()["keys"]
    return {
        key["kid"]: rsa.RSAPublicNumbers(
            e=int.from_bytes(base64.urlsafe_b64decode(key["e"] + "==="), byteorder="big"),
            n=int.from_bytes(base64.urlsafe_b64decode(key["n"] + "==="), byteorder="big")
        ).public_key(backend=default_backend())
        for key in keys
    }


def decode(token: str, audience: typing.List[str]) -> typing.Optional[typing.Mapping]:
    """
    Verify the JWT from the request. This is function is referenced in
    securitySchemes.BearerAuth.x-bearerInfoFunc. It's used by connexion to authorize api endpoints that use BearAuth
    securitySchema.

    :param token: the Authorization header in the request.
    :param audience:
    :return: Decoded and verified token.
    """
    try:
        unverified_token = jwt.decode(token, verify=False)
    except jwt.DecodeError:
        raise AuthenticationException('Failed to decode token.')

    assert_authorized_issuer(unverified_token)
    issuer = unverified_token['iss']
    public_keys = get_public_keys(issuer)

    try:
        token_header = jwt.get_unverified_header(token)
        verified_tok = jwt.decode(token,
                                  key=public_keys[token_header["kid"]],
                                  issuer=issuer,
                                  audience=audience,
                                  algorithms=allowed_algorithms,
                                  )
    except jwt.PyJWTError as ex:  # type: ignore
        raise AuthenticationException('Authorization token is invalid') from ex
    return verified_tok


def assert_authorized_issuer(token: typing.Mapping[str, typing.Any]) -> None:
    """
    Must be either `Config.get_openid_provider()` or in `Config.get_trusted_google_projects()`
    :param token: dict
    """
    issuer = token['iss']
    if issuer == Config.get_openid_provider():
        return
    service_name, _, service_domain = issuer.partition("@")
    if service_domain in Config.get_trusted_google_projects() and issuer == token['sub']:
        return
    raise AuthenticationException("Token issuer not authorized: {iss}".format(iss=issuer))


def assert_authorized(principal: str,
                      actions: typing.List[str],
                      resources: typing.List[str]):
    question = {"action": actions,
                "resource": resources,
                "principal": principal}
    resp = session.post("{host}/v1/policies/evaluate".format(host=Config.fusillade_url),
                        headers=DCP_Auth_Token.get_header(),
                        json=question)
    if not resp.json()['result']:
        raise AuthorizationException("Principal denied.", question)


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

    def get_service_jwt(self, audience: typing.List[str], additional_claims: typing.Dict):
        token = self.token.get((audience, additional_claims))
        if token is None or self.is_expired(token):
            # create a JWT
            iat = time.time()
            exp = iat + self.lifetime
            payload = additional_claims
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
