# Configuration file for the Sphinx documentation builder.
#
# For a full list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import os
import sys
import time

sys.path.insert(0, os.path.abspath("../python"))

import etl  # noqa: E402, F401

etl.monitor.Monitor.environment = "sphinx"  # type: ignore

# -- Project information -----------------------------------------------------

project = "Arthur ELT"
copyright = "2017-%s, Harry's, Inc." % time.strftime("%Y")

author = "Data Engineering at Harry's"

# TODO(tom): Extract from setup.py
version = "1.58"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    # This must be after sphinx.ext.napoleon:
    "sphinx_autodoc_typehints",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["README.md"]

needs_sphinx = "4.3"

# -- Options for extensions -------------------------------------------------

napoleon_include_init_with_doc = True

nitpicky = True

# Adding standard libraries here to avoid too many warnings during build.
nitpick_ignore = [
    ("py:class", "abc.ABC"),
    ("py:class", "botocore.response.StreamingBody"),
    ("py:class", "datetime.datetime"),
    ("py:class", "logging.Filter"),
    ("py:class", "logging.Formatter"),
    ("py:class", "simplejson.encoder.JSONEncoder"),
    ("py:class", "string.Template"),
    ("py:class", "textwrap.TextWrapper"),
    ("py:class", "threading.Thread"),
]
nitpick_ignore_regex = [
    ("py:class", r"argparse\..*"),
    ("py:data", r"typing\..*"),
]

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_book_theme"

html_theme_options = {
    "home_page_in_toc": True,
    # "path_to_docs": "./docs",
    # "repository_branch": "master",
    "repository_url": "https://github.com/harrystech/arthur-redshift-etl",
    # "use_issues_button": True,
    "use_repository_button": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
