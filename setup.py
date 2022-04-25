#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
"""
.. py:currentmodule:: setup.py

Setup for this package
"""
import io
import os

import itertools

from setuptools import find_packages, setup

# Package meta-data.
NAME = 'py_bq_sampler'
DESCRIPTION = 'Implements BigQuery sampler from Production into Data Science Environment'
LICENSE = 'Proprietary'
URL = 'https://github.com/fgka/py-grpc-common'
EMAIL = 'gkandriotti@google.com'
AUTHOR = 'Gustavo Kuhn Andriotti'
# https://devguide.python.org/#branchstatus
REQUIRES_PYTHON = '>=3.9.0'  # End-of-life: 2025-10 (checked on 2022-02-04)
VERSION = 1.00
CLASSIFIERS = [
    # Trove classifiers
    # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
    'License :: Other/Proprietary License',
    'Development Status :: 5 - Production/Stable',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.9',
    'Operating System :: OS Independent',
    'Environment :: Console',
]

# What packages are required for this module to be executed?
INSTALL_REQUIRED = [line.strip() for line in open("requirements.txt").readlines()]

DEBUG_REQUIRED = [
    'ipython>=7.5.0',
]

CODE_QUALITY_REQUIRED = [
    'black>=20.8b1',
    'deepdiff>=5.8.0',
    'mock>=3.0.5',
    'nose>=1.3.7',
    'pudb>=2019.1',
    'pylama>=7.7.1',
    'pylama-pylint>=3.1.1',
    'pylint>=2.3.1',
    'pytest>=4.5.0',
    'pytest-cov>=2.7.1',
    'pytest-mock>=1.10.4',
    'pytest-pudb>=0.7.0',
    'pytest-pylint>=0.14.0',
    'pytest-xdist>=1.28.0',
    'requests-mock>=1.9.3',
    'vulture>=1.0',
]

SETUP_REQUIRED = [
    'pytest-runner>=5.3.0',
]

# What packages are required for this module's docs to be built
DOCS_REQUIRED = [
    'Sphinx>=2.0.1',
]

EXTRA_REQUIRED = {
    'tools': DEBUG_REQUIRED,
    'docs': DOCS_REQUIRED,
    'tests': CODE_QUALITY_REQUIRED,
    'setup': SETUP_REQUIRED,
}
ALL_REQUIRED = list(itertools.chain(*EXTRA_REQUIRED.values(), INSTALL_REQUIRED))
EXTRA_REQUIRED['all'] = ALL_REQUIRED

HERE = os.path.abspath(os.path.dirname(__file__))

# Long description
try:
    with io.open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
        LONG_DESCRIPTION = '\n' + f.read()
except FileNotFoundError:
    LONG_DESCRIPTION = DESCRIPTION

# Long license
try:
    with io.open(os.path.join(HERE, 'LICENSE'), encoding='utf-8') as f:
        LONG_LICENSE = '\n' + f.read()
except FileNotFoundError:
    LONG_LICENSE = LICENSE

# Load the package's __version__.py module as a dictionary.
ABOUT = {}
if not VERSION:
    with open(os.path.join(HERE, NAME, '__version__.py')) as f:
        # pylint: disable=exec-used
        exec(f.read(), ABOUT)
else:
    ABOUT['__version__'] = VERSION


# Where the magic happens:
setup(
    name=NAME,
    version=ABOUT['__version__'],
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    install_requires=INSTALL_REQUIRED,
    setup_requires=SETUP_REQUIRED,
    extras_require=EXTRA_REQUIRED,
    include_package_data=True,
    license=LONG_LICENSE,
    packages=find_packages(exclude=('tests',)),
    classifiers=CLASSIFIERS,
)
