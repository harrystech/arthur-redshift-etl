"""
Utilities to deal with templates.

There are two families of templates: text and sql [*].

"text" templates: these are used for configuration files and can access settings using a ${}
    notation.

"SQL" templates: these are similar to text templates but should evaluate to working SQL code.

[*] We play both flavors of music here, Country and Western.
"""
import logging
import os.path
import string
from collections import OrderedDict
from typing import Dict

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


def _find_templates(template_type: str) -> Dict[str, str]:
    """Find all templates and return a map from short name to full name."""
    lookup: Dict[str, str] = OrderedDict()
    templates = pkg_resources.resource_listdir("etl", os.path.join("templates", template_type))
    for filename in sorted(templates):
        if filename == "README.md":
            continue
        name = os.path.splitext(filename)[0]
        lookup[name] = os.path.join("templates", template_type, filename)
    return lookup


def list_templates(compact=False) -> None:
    """Print available template names to stdout."""
    if compact:
        for name in _find_templates("text"):
            print(name)
    else:
        lookup = _find_templates("text")
        print(etl.text.format_lines(lookup.items(), ("Template Name", "File Location")))


def list_sql_templates(compact=False) -> None:
    """Print available templates with SQL queries to stdout."""
    lookup = _find_templates("sql")
    print(etl.text.format_lines(lookup.items(), ("Template Name", "File Location")))


def render_from_config(template_string: str, context=None):
    try:
        config_mapping = etl.config.get_config_map()
        # These assignments are to create backwards-compatibility with old settings files.
        config_mapping.update(
            prefix=config_mapping["object_store.s3.prefix"],
            today=config_mapping["date.today"],
            yesterday=config_mapping["date.yesterday"],
        )
        template = DottedNameTemplate(template_string)
        return template.substitute(config_mapping)
    except (KeyError, ValueError) as exc:
        raise MissingValueTemplateError("failed to render template in {}".format(context)) from exc


def render_string(template_name: str, template_type: str, compact=False) -> str:
    """
    Replace template ${strings} by configuration values.

    In case the input is a JSON-formatted file, we make sure output is nicely JSON-formatted.
    """
    resource_name = _find_templates(template_type)
    if template_name not in resource_name:
        raise InvalidArgumentError("template name not found: '{}'".format(template_name))
    filename = resource_name[template_name]
    logger.info("Rendering template '%s' from file '%s'", template_name, filename)
    original = pkg_resources.resource_string("etl", filename).decode()

    rendered = render_from_config(original, context="'{}'".format(filename))
    if not filename.endswith((".json", ".yaml", ".yml")):
        return rendered

    # Always load as YAML in order to support comments.
    obj = yaml.safe_load(rendered)
    # But since we don't support anything that couldn't be done in JSON, dump as
    # the (prettier) JSON format.
    if compact:
        return json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n"
    return json.dumps(obj, indent="    ", sort_keys=True) + "\n"


def render(template_name: str, compact=False) -> None:
    """Print the rendered text template."""
    print(render_string(template_name, "text", compact=compact), end="")


def render_sql(template_name: str) -> str:
    """Return SQL query after filling in template."""
    stmt = render_string(template_name, "sql")
    return etl.text.whitespace_cleanup(stmt).rstrip(";")
