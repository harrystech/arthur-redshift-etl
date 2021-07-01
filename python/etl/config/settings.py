import fnmatch
from typing import List, Optional

import etl.config
import etl.text
from etl.errors import InvalidArgumentError


def show_value(name: str, default: Optional[str]) -> None:
    """
    Show value of a specific variable.

    This fails if the variable is not set and no default is provided.

    This is a callback for a command.
    """
    value = etl.config.get_config_value(name, default)
    if value is None:
        raise InvalidArgumentError("setting '{}' has no value".format(name))
    print(value)


def show_vars(names: List[str]) -> None:
    """
    List "variables" with values.

    This shows all known configuration settings as "variables" with their values or just
    the variables that are selected.

    This is a callback for a command.
    """
    config_mapping = etl.config.get_config_map()
    all_keys = sorted(config_mapping)
    if not names:
        keys = all_keys
    else:
        selected_keys = set()
        for name in names:
            matching_keys = [key for key in all_keys if fnmatch.fnmatch(key, name)]
            if not matching_keys:
                raise InvalidArgumentError("no matching setting for '{}'".format(name))
            selected_keys.update(matching_keys)
        keys = sorted(selected_keys)
    values = [config_mapping[key] for key in keys]
    print(etl.text.format_lines(zip(keys, values), header_row=["Name", "Value"]))
