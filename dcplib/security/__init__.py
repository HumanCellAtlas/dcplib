"""
This library contains utilities to use secure features of the DCP.

This libray provides tools to generate and verify JWTs.
"""

from typing import List

from .dcp_service_account import DCPServiceAccount
from ..errors import SecurityException

security_config_not_set_error = "security configuration not set for {}. Use Config.set_config() to set."


class Config:
    _openid_provider = None
    _auth_url = None
    _trusted_google_projects = None

    @staticmethod
    def set_config(openid_provider: str = None, auth_url: str = None, trusted_google_projects: List[str] = None):
        """
        Set the configuration values
        :param openid_provider:
        :param auth_url:
        :param trusted_google_projects:
        :return:
        """
        Config._openid_provider = openid_provider
        Config._auth_url = auth_url
        Config._trusted_google_projects = [x for x in trusted_google_projects if x.endswith("iam.gserviceaccount.com")]

    @staticmethod
    def get_openid_provider() -> str:
        if not Config._openid_provider:
            raise SecurityException(security_config_not_set_error.format('openid_provider'))
        return Config._openid_provider

    @staticmethod
    def get_auth_url() -> str:
        if not Config._auth_url:
            raise SecurityException(security_config_not_set_error.format('auth_url'))
        return Config._auth_url

    @staticmethod
    def get_trusted_google_projects():
        if Config._trusted_google_projects is None:
            raise SecurityException(security_config_not_set_error.format('trusted_google_projects'))
        return Config._trusted_google_projects


OIDC_Claims = {
    'email': 'https://auth.data.humancellatlas.org/email',
    'group': 'https://auth.data.humancellatlas.org/group'
}
