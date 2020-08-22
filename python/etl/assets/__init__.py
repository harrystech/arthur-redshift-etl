import datetime
import mimetypes
from functools import lru_cache
from typing import Dict, List, Union

import pkg_resources
import simplejson

from etl.json_encoder import FancyJsonEncoder


class Content:
    def __init__(self, name: str = None, json: Union[List, Dict] = None) -> None:
        if name is not None:
            self.content = pkg_resources.resource_string(__name__, name)
            self.content_type, self.content_encoding = mimetypes.guess_type(name)
            self.cache_control = None
        elif json is not None:
            json_text = simplejson.dumps(json, sort_keys=True, cls=FancyJsonEncoder)
            self.content = (json_text + "\n").encode("utf-8")
            self.content_type, self.content_encoding = "application/json", "UTF-8"
            self.cache_control = "no-cache, no-store, must-revalidate"
        self.content_length = len(self.content)
        self.last_modified = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %Z")


def asset_exists(name: str) -> bool:
    """Return True only if the desired asset is known to exist."""
    if name:
        return pkg_resources.resource_exists(__name__, name)
    else:
        return False  # don't allow access to directory


@lru_cache()
def get_asset(name: str) -> Content:
    """
    Return asset along with content type and length.

    If asset does not exist, simply return None.
    """
    return Content(name=name)
