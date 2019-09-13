"""Pandas-style framework for interacting with elasticsearch."""
from copy import deepcopy

from elasticsearch.helpers import scan

from .config import config
from .exceptions import BadOperatorError, MissingQueryError
from .orm import OrmMixin
from .query import Query
from .utils import deprecated


class DataFrame(OrmMixin):
    """Api for elasticsearch with pandas-style filtering operations.

    Attributes:
        index: The elasticsearch index name
        body: Raw query sent to elasticsearch
    """

    def __init__(self, index, frozen=True):
        """Init DataFrame.

        Args:
            index (str): The name of the index to use
            frozen (bool, optional): Whether the dataframe is immutable.
                Defaults to True.
        """
        if not frozen:
            raise NotImplementedError
        self.index = index
        self._query = None
        self._limit = None
        self._load_orm()

    @property
    def _es(self):
        return config.connection

    def __repr__(self):
        try:
            import pandas as pd
        except ImportError:
            return '{}({})'.format(type(self).__name__, self.index)
        else:
            data = self.collect(limit=100, preserve_order=False)
            data = (self.__nested_to_dot(i) for i in data)
            return repr(pd.DataFrame(data))

    def __getitem__(self, item):
        if isinstance(item, basestring):
            return getattr(self, item)
        if isinstance(item, Query):
            new = deepcopy(self)
            new._query &= item
            return new
        raise BadOperatorError(item)

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

    @staticmethod
    def list_indices():
        """List all the indices in the elasticsearch cluster."""
        return config.connection.indices.get('*').keys()

    def limit(self, n):
        """Limit the number of results returned by elasticsearch.

        Args:
            n (int): The number of results to return

        Returns:
            DataFrame: DataFrame with limit applied.
        """
        new = deepcopy(self)
        new._limit = n
        return new

    @property
    def _body(self):
        """Raw query sent to elasticsearch."""
        if not self._query:
            return {'query': {'match_all': {}}}
        return {'query': self._query()}

    def where(self, expression):
        """Set condition according to expression.

        Args:
            expression (str): Parseable expression string

        imagining something like:
          ds = ds.where('ns1.attr1 > 5')
          ds = ds.where('attr1 > 5', namespace='ns1')
        """
        raise NotImplementedError

    def execute(self, body, size, fields=None, preserve_order=False, **es_kwargs):
        """Execute elasticsearch query.

        Args:
            body (dict): Query body in json format
            size (int): The number of results to return. If None then
                returns a generator scan of the entire index.
            fields (List[str], optional): The fields to include in the document
                _source in the return results. Defaults to None.
            preserve_order (bool, optional): Whether to return the results
                in sorted order. Only applies where size is None, otherwise
                always returns results in sorted order. Defaults to False.

        Returns:
            List[dict]: The raw elasticsearch results. Returns a generator
                if `size` is None.
        """
        if size is None:
            return scan(
                client=self._es,
                index=self.index,
                query=body,
                preserve_order=preserve_order,
                _source=fields,
                **es_kwargs
            )
        return self._es.search(
            index=self.index,
            body=body,
            size=size,
            _source=fields,
            **es_kwargs
        )

    def collect(self, fields=None, limit=None, preserve_order=False,
                include_score=False, include_id=False, **es_kwargs):
        """Collect documents according to query conditions.

        Args:
            fields (list): The field names that should be returned from source.
                If None then returns all. Defaults to None.
                Note: Namespaced fields should be referenced using `.` syntax
                    (`<namespace>.<field>`). They will be returned using the
                    same syntax.
            limit (int, optional): The number of results to return. If a limit
                is set then the documents will always be in sorted order.
                Defaults to None.
            preserve_order (bool, optional): Whether to keep the documents
                in sorted order. This will only be applied where no limit has
                been set on the number of documents. Defaults to False.
                Note: This may be expensive on large queries.
            include_score (bool, optional): Whether to include the document score in
                the results. This can be computationally expensive. Default False.
            include_id (bool, optional): Whether to include the document id
                in the results. Default False.
            **es_kwargs (dict, optional): Additional arguments to pass to elasticsearch.

        Returns:
            generator: The response from elasticsearch
        """
        size = limit or self._limit
        body = dict(self._body, **{'track_scores': include_score})
        results = self.execute(body, size, fields, preserve_order, **es_kwargs)
        return self.__hits(results, include_score, include_id)

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
            list: The response from elasticsearch
        """
        return list(self.collect(fields=fields, limit=n))

    def __hits(self, results, include_score, include_id):
        """Format the raw elasticsearch results to return just source."""
        results = results['hits']['hits'] if isinstance(results, dict) else results
        for hit in results:
            result = hit['_source']
            if include_score:
                result['_score'] = hit.pop('_score')
            if include_id:
                result['_id'] = hit.pop('_id')
            yield result

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
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('Install pandas for pandas support.')
        else:
            data = self.collect(fields, preserve_order=False)
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


@deprecated
class ElasticDataFrame(DataFrame):  # noqa: D101
    pass
