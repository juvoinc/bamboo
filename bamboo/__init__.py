"""Provides a pandas-style interface for Elasticsearch.

Classes:
    ElasticDataFrame: Api for elasticsearch with pandas-style
        filtering operations

Exceptions:
    BadOperatorError: Raise when an inappropriate operator is used
    FieldConflictError: Raise when a root field conflicts with a namespace
    MissingMappingError: Raise when no mapping could be found for an index
    MissingQueryError: Raise when no query has been defined
"""
from .dataframe import ElasticDataFrame
from .exceptions import (BadOperatorError, FieldConflictError,
                         MissingMappingError, MissingQueryError)

__all__ = [
    ElasticDataFrame,
    BadOperatorError,
    FieldConflictError,
    MissingMappingError,
    MissingQueryError
]
