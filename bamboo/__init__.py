"""Provides a pandas-style interface for Elasticsearch.

Classes:
    DataFrame: Api for elasticsearch with pandas-style
        filtering operations
    ElasticDataFrame (deprecated): Api for elasticsearch with pandas-style
        filtering operations

Functions:
    boost: Boosts the weight of query by a value
    config: Accepts keyward arguments as configuration parameters

Exceptions:
    BadOperatorError: Raise when an inappropriate operator is used
    FieldConflictError: Raise when a root field conflicts with a namespace
    MissingMappingError: Raise when no mapping could be found for an index
    MissingQueryError: Raise when no query has been defined

Copyright 2020 Juvo Mobile Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import pkg_resources

from .config import config
from .dataframe import DataFrame, ElasticDataFrame
from .exceptions import (BadOperatorError, FieldConflictError,
                         MissingMappingError, MissingQueryError)
from .queries import boost

__all__ = [
    'DataFrame',
    'ElasticDataFrame',

    'boost',
    'config',

    'BadOperatorError',
    'FieldConflictError',
    'MissingMappingError',
    'MissingQueryError'
]
__version__ = pkg_resources.get_distribution('bamboo').version
