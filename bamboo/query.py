"""Helper classes for creating nested and inverted queries."""
from abc import ABCMeta, abstractproperty
from collections import defaultdict
from copy import deepcopy

from .exceptions import BadOperatorError


def boost(query, value):
    """Boost the weight of the query by the value.

    Helps keep things clean when writing multiple querys by avoiding
    an extra pair of parenthesis.

    Instead of writing:
        df[(df.attr1 > 5).boost(2.0) & (df.attr2 < 2).boost(3.0)]
    You can write:
        df[boost(ds.attr1 > 5, 2.0) & boost(ds.attr2 < 2, 3.0)]
    """
    return query.boost(value)


class Query(object):
    """Base class for other query types.

    Attributes:
        field: Field the query is conditioned upon
        value: Value the condition is applying
        body: Query body formatted for elasticsearch
    """

    __metaclass__ = ABCMeta

    def __init__(self, field, value, boost=None):
        """Init Query.

        Args:
            field (str): Field the query is conditioned upon
            value (obj): Value the condition is applying
            boost (float, optional): Weight given to this query.
                Default None.
        """
        self.field = field
        self.value = value
        self._boost = boost

    @abstractproperty
    def key(self):
        """Get type of elasticsearch query object."""

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

    def body(self):
        """Return the query body formatted for elasticsearch."""
        if self._boost:
            return self._boosted_query
        return self._query

    @property
    def _query(self):
        return {
            self.key: {
                self.field: self.value
            }
        }

    def _boosted_query(self):
        return {
            self.key: {
                self.field: {
                    'value': self.value,
                    'boost': self._boost
                }
            }
        }

    def boost(self, value):
        """Boost the weight of the query by the value."""
        new = deepcopy(self)
        new._boost = value
        return new

    __rand__ = __and__


class Term(Query):
    """Find documents containing the exact term specified in the inverted index."""

    key = 'term'


class Regexp(Query):
    """Find documents that contain terms matching a regular expression."""

    key = 'regexp'


class Wildcard(Query):
    """Find documents that have fields matching a wildcard expression.

    The field is not analyzed.

    Supported wildcards are *, which matches any character sequence
    (including the empty one), and ?, which matches any single character.

    Note that this query can be slow, as it needs to iterate over many terms.
    In order to prevent extremely slow wildcard queries, a wildcard term
    should not start with one of the wildcards * or ?.
    """

    key = 'wildcard'


class Prefix(Query):
    """Find documents that contain a specific prefix in a provided field."""

    key = 'prefix'


class Match(Query):
    """Find documents that match a provided text, number, date or boolean value.

    The provided text is analyzed before matching.
    """

    key = 'match'

    @property
    def _boosted_query(self):
        return {
            self.key: {
                self.field: {
                    'query': self.value,
                    'boost': self._boost
                }
            }
        }


class Exists(Query):
    """Find documents that contain an indexed value for a field."""

    key = 'exists'

    def __init__(self, field, boost=None):
        """Init Exists.

        Args:
            field (str): Field the query is checking for existence
            boost (float, optional): Weight given to this query.
                Default None.
        """
        super(Exists, self).__init__('field', field, boost)

    def _boosted_query(self):
        return {
            self.key: {
                'field': self.field,
                'boost': self._boost
            }
        }


class Range(Query):
    """Find documents that contain terms within a provided range."""

    key = 'range'

    def __init__(self, field, value, boost=None):
        """Init Query.

        Args:
            field (str): Field the query is conditioned upon
            value (obj): Value the condition is applying
            boost (float, optional): Weight given to this query.
                Default None.
        """
        super(Range, self).__init__(self, field, value, boost)
        self._operators = {}

    def greater_than(self, value):
        """Apply greater than operator to value."""
        self._operators['gt'] = value

    def greater_than_or_equal(self, value):
        """Apply greater than or equal operator to value."""
        self._operators['gte'] = value

    def less_than(self, value):
        """Apply less than operator to value."""
        self._operators['lt'] = value

    def less_than_or_equal(self, value):
        """Apply less than or equal operator to value."""
        self._operators['lte'] = value

    @property
    def _query(self):
        return {
            self.key: {
                self.field: self._operators
            }
        }

    @property
    def _boosted_query(self):
        return {
            self.key: {
                self.field: dict(self._operators, **{'boost': self._boost})
            }
        }


class Bool(Query):
    """Class for keeping track of complex and nested queries.

    Attributes:
        body: Query body formatted for elasticsearch
    """

    _valid_params = {'must', 'must_not', 'should'}

    key = 'bool'

    def __init__(self, **params):
        """Init Bool.

        Args:
            must (list): A list of queries that must exist
            must_not (list): A list of queries that must not exist
            should (list): A list of queries that do not have to exist
                but affect scoring/sorting
        """
        # must, must_not and should (and filter?) as defined attributes
        # validate at least one param is supplied
        # validate_params should allow set and if set then convert to
        # List[set]
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

    @staticmethod
    def _validate_obj(item):
        """Ensure inputs are of correct type."""
        if not isinstance(item, Query):
            raise BadOperatorError(item)

    def __add__(self, other):
        params = self.merge_params(self, other)
        return Bool(**params)

    def __and__(self, other):
        if other is None:
            return self
        self._validate_obj(other)
        if isinstance(other, Query):
            return self + Bool(must=[other])
        if isinstance(self.body, Query):
            return Bool(must=[self]) + other
        if not set(self) & set(other):
            return self + other
        return Bool(must=[self.body, other.body])

    def __or__(self, other):
        self._validate_obj(other)
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
        d = defaultdict(list, a)
        for k, v in b.items():
            d[k].extend(v)
        return d

    __rand__ = __and__
    __radd__ = __add__
    __ror__ = __or__
