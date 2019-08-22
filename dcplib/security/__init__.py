"""
This library contains utilities to use authentication and authorization features of the DCP.
"""

from typing import List

from .dcp_service_account import DCPServiceAccountManager
from ..errors import SecurityException

security_config_not_set_error = "security configuration not set for {}. Use Config.setup() to set."

OIDC_claims = {
    'email': 'https://auth.data.humancellatlas.org/email',
    'group': 'https://auth.data.humancellatlas.org/group'
}

allowed_algorithms = ['RS256']  # for encrypting and decrypting a JWT


class Config:
    _openid_provider = "humancellatlas.auth0.com"
    _auth_provider = "https://auth.data.humancellatlas.org"
    _trusted_google_projects = None

    @staticmethod
    def setup(trusted_google_projects: List[str], *, openid_provider: str = None, auth_url: str = None):
        """
        Set the configuration values
        :param trusted_google_projects: trust service account project domains to allow access to your service.
        :param openid_provider: the openid provider used to authenticate users.
        :param auth_url: The url for the DCP authentication and authorization provider.
        :return:
        """
        Config._openid_provider = openid_provider or Config._openid_provider
        Config._auth_url = auth_url or Config._auth_provider
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
