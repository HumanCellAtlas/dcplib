"""
This library contains utilities to use secure features of the DCP.

This libray provides tools to generate and verify JWTs.
"""

import os
from typing import List


class Config:
    _openid_provider: str = None
    _auth_url: str = None
    _trusted_google_projects: List[str] = None

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
    def get_openid_provider():
        if not Config.openid_provider:
            Config.openid_provider = os.environ['DCP_OPENID_PROVIDER']
        return Config.openid_provider

    @staticmethod
    def get_auth_url():
        if not Config._auth_url:
            Config._auth_url = os.environ['DCP_AUTH_URL']
        return Config._auth_url

    @staticmethod
    def get_trusted_google_projects():
        if Config._TRUSTED_GOOGLE_PROJECTS is None:
            Config._TRUSTED_GOOGLE_PROJECTS = [x for x in os.environ['DCP_AUTHORIZED_DOMAINS'].split()
                                               if x.endswith("iam.gserviceaccount.com")]
        return Config._TRUSTED_GOOGLE_PROJECTS


class OIDC_Claims:
    email = 'https://auth.data.humancellatlas.org/email'
    group = 'https://auth.data.humancellatlas.org/group'
