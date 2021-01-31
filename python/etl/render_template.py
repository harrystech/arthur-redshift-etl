import fnmatch
import logging
import os.path
import string
from collections import OrderedDict
from typing import Dict, List, Optional

import pkg_resources
import simplejson as json
import yaml

import etl.config
import etl.text
from etl.errors import InvalidArgumentError, MissingValueTemplateError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DottedNameTemplate(string.Template):
    """
    Support ${name} substitutions where name may be any identifier.

    This also allows dotted or hyphenated names.
    """

    idpattern = r"[_a-z][-._a-z0-9]*"


def _find_templates() -> Dict[str, str]:
    """Find all templates and return a map from short name to full name."""
    lookup = OrderedDict()  # type: Dict[str, str]
    templates = pkg_resources.resource_listdir("etl", "templates")
    for filename in sorted(templates):
        name = os.path.splitext(filename)[0]
        lookup[name] = os.path.join("templates", filename)
    return lookup


def list_templates(compact=False) -> None:
    """Print available template names to stdout."""
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


def render_from_config(template_string, context=None):
    try:
        config_mapping = etl.config.get_config_map()
        # These assignments are to create backwards-compatibility with old settings files.
        config_mapping.update(
            {
                "prefix": config_mapping["object_store.s3.prefix"],
                "today": config_mapping["date.today"],
                "yesterday": config_mapping["date.yesterday"],
            }
        )
        template = DottedNameTemplate(template_string)
        return template.substitute(config_mapping)
    except (KeyError, ValueError) as exc:
        raise MissingValueTemplateError("failed to render template in {}".format(context)) from exc


def render(template_name: str, compact=False) -> None:
    """Replace template ${strings} by configuration values."""
    resource_name = _find_templates()
    if template_name not in resource_name:
        raise InvalidArgumentError("template name not found: '{}'".format(template_name))
    filename = resource_name[template_name]
    logger.info("Rendering template '%s' from file '%s'", template_name, filename)
    original = pkg_resources.resource_string("etl", filename).decode()

    rendered = render_from_config(original, context="'{}'".format(filename))

    if filename.endswith((".json", ".yaml", ".yml")):
        # Always load as YAML in order to support comments.
        obj = yaml.safe_load(rendered)
        # But since we don't support anything that couldn't be done in JSON, dump the (prettier)
        # JSON format.
        if compact:
            print(json.dumps(obj, separators=(",", ":"), sort_keys=True))
        else:
            print(json.dumps(obj, indent="    ", sort_keys=True))
    else:
        print(rendered, end="")


def show_value(name: str, default: Optional[str]) -> None:
    """
    Show value of a specific variable.

    This fails if the variable is not set and no default is provided.
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
