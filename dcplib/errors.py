class DCPLibException(BaseException):
    """
    Base class for all DCPLIB exceptions
    """


class SecurityException(DCPLibException):
    """
    A security exception occurred
    """

class AuthorizationException(SecurityException):
    """
    Authorization error occurred
    """


class AuthenticationException(SecurityException):
    """
    Authentication error occurred
    """
