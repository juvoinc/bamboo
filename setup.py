# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Install bamboo package."""
from setuptools import setup

VERSION = '0.3.2'

setup(
    name='elasticsearch-bamboo',
    packages=['bamboo'],
    version=VERSION,
    license='Mozilla Public License 2.0 (MPL 2.0)',
    description='DataFrame interface for ElasticSearch',
    author='Aaron Mangum',
    author_email='aaron.mangum@juvo.com',
    url='https://github.com/juvoinc/bamboo',
    download_url='https://github.com/juvoinc/bamboo/archive/{}.tar.gz'.format(VERSION),
    keywords=['elasticsearch', 'dataframe', 'pandas'],
    python_requires='>=2.7',
    install_requires=[
        'elasticsearch>=6.0.0',
        'six'  # temp py2 support
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Topic :: Software Development',
        'Topic :: Utilities',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    extras_require=dict(
        dev=[
            'coverage',
            'flake8',
            'flake8-docstrings',
            'pytest',
            'tox'
        ],
        test=[
            'pandas',
            'pytest',
        ],
        pandas=['pandas']
    )
)
