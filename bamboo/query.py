"""Classes representing elasticsearch queries."""
from abc import ABCMeta, abstractproperty
from collections import defaultdict
from copy import deepcopy

from .utils import dict_to_params


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

    def __repr__(self):
        return '{}(field={}, value={}, boost={})'.format(
            type(self).__name__, self.field, self.value, self._boost
        )

    def __and__(self, other):
        if other is None:
            return self
        if isinstance(other, Bool):
            return NotImplemented
        return Bool(must=[self, other])
    __rand__ = __and__

    def __or__(self, other):
        if other is None:
            return self
        if isinstance(other, Bool):
            return NotImplemented
        return Bool(should=[self, other])
    __ror__ = __or__

    def __invert__(self):
        return Bool(must_not=self)

    def __call__(self):
        """Return the query body formatted for elasticsearch."""
        if self._boost is not None:
            return self._boosted_query
        return self._query

    @property
    def _query(self):
        return {
            self.key: {
                self.field: self.value
            }
        }

    @property
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
        """Boost the weight of the query by the value.

        Returns:
            Query: Query with boost applied.
        """
        new = deepcopy(self)
        new._boost = value
        return new


class Term(Query):
    """Find documents containing the exact term specified in the inverted index.

    The field is not analyzed.
    """

    key = 'term'


class Terms(Query):
    """Find documents containing any of the provided terms.

    The field is not analyzed.
    """

    key = 'terms'

    def __init__(self, field, value, boost=None):
        """Init Terms.

        Args:
            field (str): Field the query is conditioned upon
            value (list): List of terms to match
            boost (float, optional): Weight given to this query.
                Default None.
        """
        super(Terms, self).__init__(field, value, boost)

    @property
    def _boosted_query(self):
        return {
            self.key: {
                self.field: self.value,
                'boost': self._boost
            }
        }


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

    @property
    def _boosted_query(self):
        return {
            self.key: {
                self.field: self.value,
                'boost': self._boost
            }
        }

    def __repr__(self):
        return 'Exists(field={}, boost={})'.format(self.value, self._boost)


class Range(Query):
    """Find documents that contain terms within a provided range."""

    key = 'range'

    def __init__(self, field, boost=None):
        """Init Query.

        Args:
            field (str): Field the query is conditioned upon
            boost (float, optional): Weight given to this query.
                Default None.
        """
        self.field = field
        self._boost = boost
        self._operators = {}

    def greater_than(self, value):
        """Apply greater than operator to value.

        Returns:
            Range: Range query with greater than value applied.
        """
        new = deepcopy(self)
        new._operators['gt'] = value
        return new

    def greater_than_or_equal(self, value):
        """Apply greater than or equal operator to value.

        Returns:
            Range: Range query with greater than or equal value applied.
        """
        new = deepcopy(self)
        new._operators['gte'] = value
        return new

    def less_than(self, value):
        """Apply less than operator to value.

        Returns:
            Range: Range query with less than value applied.
        """
        new = deepcopy(self)
        new._operators['lt'] = value
        return new

    def less_than_or_equal(self, value):
        """Apply less than or equal operator to value.

        Returns:
            Range: Range query with less than or equal value applied.
        """
        new = deepcopy(self)
        new._operators['lte'] = value
        return new

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

    def __repr__(self):
        return 'Range({})'.format(dict_to_params(self._operators))


class Bool(Query):
    """Find documents matching boolean combinations of other queries.

    Attributes:
        body: Query body formatted for elasticsearch
    """

    key = 'bool'
    _validation_msg = 'At least one initialization parameter must be supplied.'

    def __init__(self, must=None, must_not=None, should=None, boost=None):
        """Init Bool.

        Args:
            must (list): A list of queries that must exist
            must_not (list): A list of queries that must not exist
            should (list): A list of queries that do not have to exist
                but affect scoring/sorting
            boost (float, optional): Weight given to this query.
                Default None.
        """
        # TODO: add filter param
        self.params = {
            'must': must or [],
            'must_not': must_not or [],
            'should': should or []
        }
        self._boost = boost
        self.__validate_params()

    def __repr__(self):
        return 'Bool({})'.format(dict_to_params(self.params))

    def __validate_params(self):
        """Ensure parameters are in correct format."""
        assert any(self.params.values()), self._validation_msg
        for key, param in self.params.items():
            if isinstance(param, Query):
                self.params[key] = [param]

    @property
    def must(self):
        """Get the must conditions."""
        return self.params['must']

    @property
    def must_not(self):
        """Get the must_not conditions."""
        return self.params['must_not']

    @property
    def should(self):
        """Get the should conditions."""
        return self.params['should']

    def __add__(self, other):
        params = self.merge_params(self.params, other.params)
        return Bool(**params)

    def __and__(self, other):
        if not isinstance(other, Bool):
            return self + Bool(must=other)
        if self._can_flatten_with(other):
            return self + other
        return Bool(must=[self, other])

    def __or__(self, other):
        if not isinstance(other, Bool):
            return self + Bool(should=other)
        if self._can_flatten_with(other):
            return self + other
        return Bool(should=[self, other])

    def _can_flatten_with(self, other):
        """Check whether it is safe to add self+other.

        Used to flatten must(must(must())) to must(). Likewise with should.
        """
        return not set(self.filtered_params) & set(other.filtered_params)

    def __invert__(self):
        return Bool(
            must=[self.__negate(i) for i in self.should] + self.must_not,
            should=[self.__negate(i) for i in self.must]
        )

    def __negate(self, query):
        """Convert `must` queries to `must_not`."""
        if isinstance(query, Bool):
            return ~query
        return Bool(must_not=query)

    @property
    def _query(self):
        params = self._finalize_params()
        # if must is only parameter and contains only one condition
        # then convert that one condition to a stand-alone query
        if len(params.get('must', [])) == 1 == len(params):
            return params['must'][0]
        return {self.key: params}

    @property
    def _boosted_query(self):
        params = self._finalize_params()
        return {self.key: dict(params, **{'boost': self._boost})}

    @property
    def filtered_params(self):
        """Get only the parameters populated with conditions."""
        return {k: v for k, v in self.params.items() if v}

    def _finalize_params(self):
        return {
            key: [param() for param in params]
            for key, params in self.params.items()
            if params
        }

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


class Script(Query):
    """Find documents matching a painless scripts as a query."""

    key = 'script'

    def __init__(self, source, boost=None):
        """Init Exists.

        Args:
            source (str): Painless script
            boost (float, optional): Weight given to this query.
                Default None.
        """
        self.source = source
        self._boost = boost

    def __repr__(self):
        return 'Script(source={}, boost={})'.format(self.source, self._boost)

    @property
    def _query(self):
        return {
            self.key: {
                self.key: {
                    'source': self.source,
                    'lang': 'painless'
                }
            }
        }

    @property
    def _boosted_query(self):
        return {
            self.key: {
                self.key: {
                    'source': self.source,
                    'lang': 'painless'
                },
                'boost': self._boost
            }
        }
