# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

import subprocess
from datetime import datetime
from recommonmark.parser import CommonMarkParser
from recommonmark.transform import AutoStructify


# -- Project information -----------------------------------------------------

project = 'Digdag'
copyright = '2016-' + datetime.now().strftime("%Y") + ', Digdag Project'
author = '2016, Digdag Project'
version = '0.10'
release = subprocess.check_output(['git', 'describe', '--abbrev=0', '--tags'])[1:].strip().decode("utf-8")


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['recommonmark', 'sphinx_markdown_tables']

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# http://recommonmark.readthedocs.io/en/latest/auto_structify.html
def setup(app):
    app.add_config_value('recommonmark_config', {
            #'url_resolver': lambda url: url,
            'enable_auto_toc_tree': False,
            'enable_eval_rst': True,
            }, True)
    app.add_transform(AutoStructify)

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_extra_path = ['_extra']

html_context = {
  'extra_css_files': ['_static/custom.css']
}
