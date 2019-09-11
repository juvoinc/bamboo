"""Helper classes for creating nested and inverted queries."""
from abc import ABCMeta, abstractproperty
from collections import defaultdict
from copy import deepcopy

from .exceptions import BadOperatorError


class BaseQuery(dict):
    """Parent class for query and bool classes."""

    __metaclass__ = ABCMeta

    @staticmethod
    def _validate(item):
        """Ensure inputs are of correct type."""
        if not isinstance(item, BaseQuery):
            raise BadOperatorError

    @abstractproperty
    def body(self):
        """Return the query body formatted for elasticsearch."""
        pass


class Query(BaseQuery):
    """Class for keeping track of simple queries.

    Attributes:
        body: Query body formatted for elasticsearch
    """

    def __init__(self, query):
        """Init Query.

        Args:
            query (str): The query body
        """
        self.update(query)

    def __and__(self, other):
        if other is None:
            return self
        if not isinstance(other, Query):
            return NotImplemented
        return Bool(must=[self, other])

    def __or__(self, other):
        if not isinstance(other, Query):
            return NotImplemented
        return Bool(should=[self, other])

    def __invert__(self):
        return Bool(must_not=[self])

    @property
    def body(self):
        """Query body formatted for elasticsearch."""
        return self

    __rand__ = __and__


class Bool(BaseQuery):
    """Class for keeping track of complex and nested queries.

    Attributes:
        body: Query body formatted for elasticsearch
    """

    _valid_params = {'must', 'must_not', 'should'}

    def __init__(self, **params):
        """Init Bool.

        Args:
            must (list): A list of queries that must exist
            must_not (list): A list of queries that must not exist
            should (list): A list of queries that do not have to exist
                but affect scoring/sorting
        """
        params = dict(self.__validate_params(params))
        self.update(params)

    @classmethod
    def __validate_params(cls, params):
        """Ensure parameters are in correct format."""
        assert params, "Params are empty"
        for k, v in params.items():
            assert k in cls._valid_params, "Bad parameter"
            if v:
                assert isinstance(v, list), "Parameter must be a list"
                yield k, filter(None, v)

    def __add__(self, other):
        params = self.merge_params(self, other)
        return Bool(**params)

    def __and__(self, other):
        if other is None:
            return self
        self._validate(other)
        if isinstance(other, Query):
            return self + Bool(must=[other])
        if isinstance(self.body, Query):
            return Bool(must=[self]) + other
        if not set(self) & set(other):
            return self + other
        return Bool(must=[self.body, other.body])

    def __or__(self, other):
        self._validate(other)
        if isinstance(other, Query):
            return self + Bool(should=[other])
        return Bool(
            should=[self.body, other.body]
        )

    def __invert__(self):
        must_not = self.get('must_not', [])
        must = self.get('must', [])
        should = self.get('should', [])
        return Bool(
            must=[self.__negate(i) for i in should] + must_not,
            should=[self.__negate(i) for i in must]
        )

    def __negate(self, value):
        """Convert `must` queries to `must_not`."""
        must_not = value.get('bool', {}).get('must_not')
        if must_not:
            return Bool(must=must_not).body
        return Bool(must_not=[value]).body

    @property
    def body(self):
        """Query body formatted for elasticsearch."""
        must = self.get('must', [])
        if len(self) == 1 == len(must):
            return Query(must[0])
        return {'bool': self}

    @staticmethod
    def merge_params(a, b):
        """Merge the parameters of two queries into one.

        Args:
            a (dict of lists of dicts): Query parameters
            b (dict of lists of dicts): Query parameters

        Returns:
            dict of lists of dicts: The merged query parameters
        """
        d = defaultdict(list, deepcopy(a))
        for k, v in b.items():
            d[k].extend(v)
        return d

    __rand__ = __and__
    __radd__ = __add__
    __ror__ = __or__
