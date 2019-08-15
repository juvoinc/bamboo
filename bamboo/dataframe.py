"""Pandas-style framework for interacting with elasticsearch."""
from copy import deepcopy

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from ..utils import CONFIG, HOST
from .exceptions import BadOperatorError, MissingQueryError
from .orm import OrmMixin
from .utils import BaseQuery


class ElasticDataFrame(OrmMixin):
    """Api for elasticsearch with pandas-style filtering operations.

    Attributes:
        index: The elasticsearch index name
        body: Raw query sent to elasticsearch
    """

    _es = Elasticsearch([HOST], timeout=int(CONFIG['TIMEOUT']))

    def __init__(self, index, frozen=True):
        """Init ElasticDataFrame.

        Args:
            index (str): The name of the index to use
            frozen (bool, optional): Whether the dataframe is immutable.
                Defaults to True.
        """
        if not frozen:
            raise NotImplementedError
        self.index = index
        self._load_mapping()
        self._query = None
        self._limit = None

    def __getitem__(self, item):
        if isinstance(item, basestring):
            return getattr(self, item)
        if isinstance(item, BaseQuery):
            new = deepcopy(self)
            new._query &= item
            return new
        raise BadOperatorError

    def __and__(self, other):
        new = deepcopy(self)
        new._query &= other._query
        return new

    __add__ = __and__

    def __or__(self, other):
        new = deepcopy(self)
        new._query |= other._query
        return new

    def __invert__(self):
        new = deepcopy(self)
        if new._query is None:
            raise MissingQueryError
        new._query = ~new._query
        return new

    def get(self, id, fields=None):
        """Return the document represented by an ID.

        Args:
            id (str): ID for a document
            fields (list): The field names that should be returned from source.
                If None then returns all. Defaults to None.
                Note: Namespaced fields should be referenced using `.` syntax
                    (`<namespace>.<field>`). They will be returned using the
                    same syntax.

        Returns:
            dict: Document source for a given ID
        """
        doc = self._es.get(index=self.index,
                           id=id,
                           doc_type='doc',
                           _source=fields)
        return doc['_source']

    __call__ = get

    @classmethod
    def list_indices(cls):
        """List all the indices in the elasticsearch cluster."""
        return cls._es.indices.get('*').keys()

    def limit(self, n):
        """Limit the number of results returned by elasticsearch.

        Args:
            n (int): The number of results to return

        Returns:
            ElasticDataFrame: self
        """
        new = deepcopy(self)
        new._limit = n
        return new

    @property
    def _body(self):
        """Raw query sent to elasticsearch."""
        if not self._query:
            return {'query': {'match_all': {}}}
        return {'query': self._query.body}

    def where(self, expression):
        """Set condition according to expression.

        Args:
            expression (str): Parseable expression string

        imagining something like:
          ds = ds.where('ns1.attr1 > 5')
          ds = ds.where('attr1 > 5', namespace='ns1')
        """
        raise NotImplementedError

    def _execute(self, body, size, fields=None, preserve_order=False, **es_kwargs):
        """Execute elasticsearch query."""
        if size is None:
            return scan(client=self._es,
                        index=self.index,
                        query=body,
                        preserve_order=preserve_order,
                        _source=fields,
                        **es_kwargs)
        return self._es.search(index=self.index,
                               body=body,
                               size=size,
                               _source=fields,
                               **es_kwargs)

    def collect(self, fields=None, limit=None, raw=False, preserve_order=False,
                **es_kwargs):
        """Collect documents according to query conditions.

        Args:
            fields (list): The field names that should be returned from source.
                If None then returns all. Defaults to None.
                Note: Namespaced fields should be referenced using `.` syntax
                    (`<namespace>.<field>`). They will be returned using the
                    same syntax.
            limit (int, optional): The number of results to return. Defaults to None.
            raw (bool, optional): Whether to return the raw results or just the
                source of document hits. Defautls to False.
            preserve_order (bool, optional): Whether to keep the documents
                in sorted order. This will only be applied where no limit has
                been set on the number of documents. Defaults to False.
                Note: This may be expensive on large queries.
            es_kwargs (dict, optional): Additional arguments to pass to elasticsearch.

        Returns:
            generator: The response from elasticsearch
        """
        size = limit or self._limit
        results = self._execute(self._body, size, fields, preserve_order)
        if raw:
            return results
        return self.__hits(results)

    def count(self):
        """Return the count of documents that match.

        Returns:
            int: The number of documents
        """
        results = self._es.count(index=self.index, body=self._body)
        return results['count']

    def take(self, n, fields=None):
        """Collect documents according to query conditions.

        Args:
            n (int, optional): The number of results to return. Defaults to None.
            fields (list): The field names that should be returned from source.
                If None then returns all. Defaults to None.
                Note: Namespaced fields should be referenced using `.` syntax
                    (`<namespace>.<field>`). They will be returned using the
                    same syntax.

        Returns:
            generator: The response from elasticsearch
        """
        return self.collect(fields=fields, limit=n)

    def __hits(self, results):
        """Format the raw elasticsearch results to return just source."""
        results = results['hits']['hits'] if isinstance(results, dict) else results
        for hit in results:
            yield hit['_source']

    def to_pandas(self, fields=None):
        """Collect documents according to query conditions as a pandas DataFrame.

        Args:
            fields (list): The field names that should be returned from source.
                If None then returns all. Defaults to None.
                Note: Namespaced fields should be referenced using `.` syntax
                    (`<namespace>.<field>`). They will be returned using the
                    same syntax.

        Returns:
            pd.DataFrame: The response from elasticsearch
        """
        data = self.collect(fields, raw=False, preserve_order=False)
        data = (self.__nested_to_dot(i) for i in data)
        return pd.DataFrame(data)

    @classmethod
    def __nested_to_dot(cls, hit, namespace=''):
        """Recursively flattens nested objects to use dot-notation."""
        d = {}
        for k, v in hit.items():
            key = '{}.{}'.format(namespace, k) if namespace else k
            if isinstance(v, dict):
                d.update(cls.__nested_to_dot(v, key))
            else:
                d[key] = v
        return d
