class DCPLIBException(BaseException):
    """
    Base class for all DCPLIB exceptions
    """

class AuthorizationException(DCPLIBException):
    """
    Authorization error occurred
    """

class AuthenticationException(DCPLIBException):
    """
    Authentication error occurred
    """