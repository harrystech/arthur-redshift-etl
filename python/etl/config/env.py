import getpass
import os
from typing import Union


def get(name: str, default: Union[str, None]=None) -> str:
    """
    Retrieve environment variable or error out if variable is not set.
    This is mildly more readable than direct use of os.environ.
    """
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError('Environment variable "%s" not set' % name)
    if not value:
        raise ValueError('Environment variable "%s" is empty' % name)
    return value


def getuser() -> str:
    """
    Find user's name, normally by looking up the value of the USER environment
    variable. This is used, for example, to select the default prefix in the S3 bucket.

    If the lookup for the environment variable fails, we fall back to a more
    exhaustive search using getuser.
    """
    user = os.environ.get("USER", "")
    if len(user) == 0:
        user = getpass.getuser()
    return user


if __name__ == "__main__":
    user_name = getuser()
    print("Hello {}!".format(user_name))
