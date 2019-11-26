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
"""
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
