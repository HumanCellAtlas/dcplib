"""
This module provide utilities to verify JWTs
"""

import base64
import functools
import typing

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from furl import furl

from ..errors import AuthenticationException
from ..security import Config, OIDC_Claims

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


def decode_jwt(token: str, audience: typing.List[str]) -> typing.Optional[typing.Mapping]:
    """
    Verify and decode the JWT from the request.

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


def get_email_claim(token_info: typing.Dict[str, typing.Any]) -> str:
    email = token_info.get(OIDC_Claims['email']) or token_info.get('email')
    if email:
        return email
    else:
        raise AuthenticationException("Email claim is missing from token.")
