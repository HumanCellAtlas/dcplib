class DCPLIBException(BaseException):
    """
    Base class for all DCPLIB exceptions
    """


class SecurityException(DCPLIBException):
    """
    A security exception occurred
    """

class AuthorizationException(DCPLIBException):
    """
    Authorization error occurred
    """


class AuthenticationException(DCPLIBException):
    """
    Authentication error occurred
    """
