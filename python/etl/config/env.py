import logging
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    user_name = get("USER")
    print("Hello {}!".format(user_name))
