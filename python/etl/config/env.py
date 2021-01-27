import getpass
import os
from typing import Union


def get(name: str, default: Union[str, None] = None) -> str:
    """
    Retrieve environment variable or error out if variable is not set.

    This is mildly more readable than direct use of os.environ.
    """
    value = os.environ.get(name, default)
    if value is None:
        raise KeyError('environment variable "%s" not set' % name)
    if not value:
        raise ValueError('environment variable "%s" is empty' % name)
    return value


def get_default_prefix() -> str:
    """
    Return default prefix, gleaned from the environment in ARTHUR_DEFAULT_PREFIX or USER.

    We have a last resort by checking the user name using the getapass module.

    >>> os.environ["ARTHUR_DEFAULT_PREFIX"] = "doctest"
    >>> get_default_prefix()
    'doctest'
    """
    try:
        default_prefix = get("ARTHUR_DEFAULT_PREFIX")
    except (KeyError, ValueError):
        default_prefix = os.environ.get("USER", "")
        if len(default_prefix) == 0:
            default_prefix = getpass.getuser()
    return default_prefix


if __name__ == "__main__":
    prefix = get_default_prefix()
    print("Default prefix = {}".format(prefix))
