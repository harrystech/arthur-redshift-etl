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
version = "1.52"
release = "1.52.1"

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["README.md"]

needs_sphinx = "4.0"
nitpicky = True

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
