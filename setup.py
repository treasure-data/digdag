#!/usr/bin/env python

from distutils.core import setup

setup(
    name='digdag',
    version='0.1.0',
    author='Sadayuki Furuhashi',
    author_email='frsyuki@gmail',
    url='https://github.com/treasure-data/digdag',
    description='Digdag workflow engine',
    long_description="""Digdag is a fully-featured workflow engine.""",
    packages=['digdag'],
    data_files = [("", ["README.md"])],
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    keywords='digdag workflow',
    )

