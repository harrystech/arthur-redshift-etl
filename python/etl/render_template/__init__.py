import fnmatch
import logging
import os.path
import string
from collections import OrderedDict
from typing import Dict, Optional

import pkg_resources
import simplejson
import yaml

from etl.errors import InvalidArgumentError, MissingValueTemplateError
import etl.config

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DottedNameTemplate(string.Template):
    """
    Support ${name} substitutions where name may be any identifier (including dotted or hyphenated names).
    """
    idpattern = r'[_a-z][-._a-z0-9]*'


def _find_templates() -> Dict[str, str]:
    """
    Find all templates and return a map from short name to full name
    """
    lookup = OrderedDict()  # type: Dict[str, str]
    templates = pkg_resources.resource_listdir(__name__, "templates")
    for filename in sorted(templates):
        name = os.path.splitext(filename)[0]
        lookup[name] = os.path.join("templates", filename)
    return lookup


def list_templates(compact=False) -> None:
    """
    Print available template names to stdout.
    """
    if compact:
        for name in _find_templates():
            print(name)
    else:
        lookup = _find_templates()
        name_column = ["Template Name"] + list(lookup)
        filename_column = ["File Location"] + [lookup[name] for name in lookup]
        width = max(map(len, name_column))
        for name, filename in zip(name_column, filename_column):
            print("{name:{width}s}: {filename}".format(name=name, filename=filename, width=width))


def _render_template_string(template_name: str) -> str:
    """
    Replace template ${strings} by configuration values.
    """
    resource_name = _find_templates()
    if template_name not in resource_name:
        raise InvalidArgumentError("template name not found: '{}'".format(template_name))
    filename = resource_name[template_name]
    logger.info("Rendering template '%s' from file '%s'", template_name, filename)
    original = pkg_resources.resource_string(__name__, filename).decode()

    try:
        config_mapping = etl.config.get_config_map()
        template = DottedNameTemplate(original)
        rendered = template.substitute(config_mapping)
        return rendered, filename
    except (KeyError, ValueError) as exc:
        raise MissingValueTemplateError("failed to render template in '{}'".format(filename)) from exc


def _print_template(obj, as_json=True, compact=False):
    if as_json:
        if compact:
            print(simplejson.dumps(obj, separators=(',', ':'), sort_keys=True))
        else:
            print(simplejson.dumps(obj, indent="    ", sort_keys=True))
    else:
        print(obj, end='')


def render(template_name: str, compact=False) -> None:
    rendered, filename = _render_template_string(template_name)
    if filename.endswith((".json", ".yaml", ".yml")):
        # Always load as YAML in order to support comments.
        obj = yaml.safe_load(rendered)
        # But since we don't support anything that couldn't be done in JSON, dump the (prettier) JSON format.
        _print_template(obj, as_json=True, compact=compact)
    else:
        _print_template(rendered, as_json=False, compact=compact)


def show_value(name: str, default: Optional[str]) -> None:
    """
    Show value of a specific variable. This fails if the variable is not set and no default is provided.
    """
    value = etl.config.get_config_value(name, default)
    if value is None:
        raise InvalidArgumentError("setting '{}' has no value".format(name))
    print(value)


def show_vars(name: Optional[str]) -> None:
    """
    List all the known configuration settings as "variables" with their values or
    just for the variable that's selected.
    """
    config_mapping = etl.config.get_config_map()
    if name is None:
        keys = sorted(config_mapping)
    else:
        keys = [key for key in sorted(config_mapping) if fnmatch.fnmatch(key, name)]
        if not keys:
            raise InvalidArgumentError("no matching setting for '{}'".format(name))
    values = [config_mapping[key] for key in keys]
    width = max(map(len, keys))
    for key, value in zip(keys, values):
        print("{key:{width}} = {value}".format(key=key, value=value, width=width))
