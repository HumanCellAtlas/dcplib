import functools
import typing

import requests
from requests import HTTPError

from dcplib.errors import AuthorizationException
from dcplib.security import Config, DCPServiceAccountManager

# recycling the same session for all requests.
session = requests.Session()


def assert_authorized(principal: str,
                      actions: typing.List[str],
                      resources: typing.List[str],
                      authorization_header: typing.Dict[str, str]):
    resp = session.post("{}/v1/policies/evaluate".format(Config.get_auth_url()),
                        headers=authorization_header,
                        json={"action": actions,
                              "resource": resources,
                              "principal": principal})
    try:
        resp.raise_for_status()
    except HTTPError as ex:
        raise AuthorizationException(resp) from ex
    if not resp.json()['result']:
        raise AuthorizationException()


def get_email_claim(token_info: dict):
    email = token_info.get(Config.get_oidc_email_claim()) or token_info.get('email')
    if email:
        return email
    else:
        raise AuthorizationException("Email claim {} is missing from token.".format(Config.get_oidc_email_claim()))


def authorize(
        service_account: DCPServiceAccountManager,
        actions: typing.List[str],
        resources: typing.List[str],
):
    """
    A decorator for assert_authorized. The Decorated function must take the variable `token_info`, which is a decoded
    JWT with the appropriate OIDC email claim.

    :param service_account: A DCPServiceAccountManager with a service account that has permission to make request to the
     /policy/evaluation endpoint.
    :param actions: The actions passed to assert_authorized
    :param resources: The resources passed to assert_authorized
    :return:
    """

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            assert_authorized(get_email_claim(kwargs['token_info']),
                              actions,
                              resources,
                              service_account.get_authorization_header()
                              )
            return func(*args, **kwargs)

        return call

    return decorate
