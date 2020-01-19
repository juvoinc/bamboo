# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Field and namespace objects representing Elasticsearch field types."""
import warnings
from abc import ABCMeta
from copy import deepcopy
from datetime import datetime, timedelta
from functools import wraps

from . import queries


def check_inversion(func):
    """Decorate a method for invertible operations."""
    @wraps(func)
    def wrapper(obj, *args, **kwargs):
        condition = func(obj, *args, **kwargs)
        if obj._inverted is True:
            condition = queries.Bool(must_not=condition)
        return condition
    return wrapper


class Base(object):
    """Parent class for field and namespace classes.

    Attributes:
        name: Full field name including any parents
        parent: Parent for a field
    """

    __metaclass__ = ABCMeta

    def __init__(self, name, parent):
        """Init Base.

        Args:
            name (str): The name for the class
        """
        self.parent = parent
        self._name = name
        self._inverted = False

    def __invert__(self):
        new = deepcopy(self)
        new._inverted = True
        return new

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, self._name)

    @property
    def name(self):
        """Full field name including any parents."""
        if isinstance(self.parent, Base):
            return self.parent.name + '.' + self._name
        return self._name

    @property
    def root(self):
        """Dataframe where a field is attached."""
        return self.__get_root(self)

    @staticmethod
    def __get_root(obj):
        """Recurse parent objects until root dataframe."""
        parent = obj.parent
        if not isinstance(parent, Base):
            return parent
        return Base.__get_root(parent)

    @check_inversion
    def exists(self):
        """Condition checking whether the field exists."""
        return queries.Exists(self.name)


class Namespace(Base):
    """Namespace field meant to hold children fields.

    Attributes:
        name: Full field name including any parents
        parent: Parent for a field
        fields: Children fields held by namespace
    """

    def __getitem__(self, key):
        return getattr(self, key)

    @property
    def fields(self):
        """Fields that exist within a namespace."""
        return [i._name
                for i in vars(self).values()
                if isinstance(i, Field)]


class Field(Base):
    """Base field class for different dtypes.

    Attributes:
        name: Full field name including any parents
        parent: Parent for a field
    """

    __metaclass__ = ABCMeta

    @check_inversion
    def __eq__(self, value):
        return queries.Term(self.name, value)

    def __ne__(self, value):
        condition = queries.Term(self.name, value)
        if self._inverted:
            return condition
        return queries.Bool(must_not=condition)

    @check_inversion
    def isin(self, values):
        """Condition checking whether multiple terms are in a field's value.

        Args:
            values (list): Sequence of values to check
        """
        return queries.Terms(self.name, values)

    def value_counts(self, n=10, normalize=False, missing=None, **es_kwargs):
        """Get the unique value counts for a field.

        The results are approximate if the number of unique terms is
        greater than the `n` parameter. The higher the requested `n` is,
        the more accurate the results will be, but also, the more expensive
        it will be to compute the final results.

        Args:
            n (int, optional): The number of value counts to return.
                Defaults to 10.
            normalize (bool, optional): If True then the object returned
                will contain the relative frequencies of the unique values.
                Defaults to False.
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.

        Returns:
            List[tuple]: Tuple consiting of (key, count) ordered by descending count.
                `OTHER` is included in counts if the `n` parameter is not
                enough to cover all counts.
        """
        results = self._simple_aggregation('terms',
                                           missing=missing,
                                           buckets=False,
                                           size=n,
                                           es_kwargs=es_kwargs)
        counts = [(i['key'], i['doc_count']) for i in results['buckets']]
        other_counts = results['sum_other_doc_count']
        error_bound = results['doc_count_error_upper_bound']
        if counts and error_bound > counts[-1][1]:
            msg = """
                The current `n` parameter may result in an approximation
                where a term is not included in the returned results ({})
            """.format(error_bound)
            warnings.warn(msg)
        if other_counts:
            counts.append(('OTHER', other_counts))
        if normalize:
            total = sum(i[1] for i in counts)
            counts = [(i[0], i[1] / float(total)) for i in counts]
        return counts

    def nunique(self, precision=3000, **es_kwargs):
        """Get the approximate count of distinct values.

        Args:
            precision (int, optional): Define a unique count below which
                counts are expected to be close to accurate. Trade memory
                for accuracy. Above this value, counts might become a bit
                more fuzzy. The maximum supported value is 40000, thresholds
                above this number will have the same effect as a threshold
                of 40000. Defaults to 3000.

        Returns:
            int: Distinct count
        """
        result = self._simple_aggregation('cardinality',
                                          precision_threshold=precision,
                                          es_kwargs=es_kwargs)
        return result['value']

    def _simple_aggregation(self, key, buckets=True, **params):
        """Execute simple aggregation query with no query hits."""
        if self.root._limit:
            warnings.warn("Limits are not applied in aggregations.")
        es_kwargs = params.pop('es_kwargs')
        params = {k: v for k, v in params.items() if v is not None}
        body = dict(self.root._body, **{
            'aggs': {
                key: {
                    key: dict({'field': self.name}, **params)
                }
            }
        })
        results = self.root.execute(body, size=0, **es_kwargs)
        results = results['aggregations'][key]
        if buckets:
            return results.get('buckets', results)
        return results


class AggregationMixin(object):
    """Adds aggregations to a field."""

    __metaclass__ = ABCMeta

    def average(self, **es_kwargs):
        """Get the average value for a field.

        Returns:
            float: Average
        """
        result = self._simple_aggregation('avg', es_kwargs=es_kwargs)
        return result['value']

    def max(self, **es_kwargs):
        """Get the max value for a field.

        Returns:
            float: Max
        """
        result = self._simple_aggregation('max', es_kwargs=es_kwargs)
        return result['value']

    def min(self, **es_kwargs):
        """Get the min value for a field.

        Returns:
            float: Max
        """
        result = self._simple_aggregation('min', es_kwargs=es_kwargs)
        return result['value']

    def percentiles(self, missing=None, precision=100, **es_kwargs):
        """Get percentiles for a filed.

        Percentiles are calculated over the range of [ 1, 5, 25, 50, 75, 95, 99 ]

        Args:
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.
            precision (int, optional): Defines the accuracy - memory tradeoff.
                Larger precision result in greater accuracy at the cost of
                greater memory usage and computation time. Defaults to 1000.

        Notes:
            - Accuracy is proportional to q(1-q). This means that extreme
                percentiles (e.g. 99%) are more accurate than less extreme
                percentiles, such as the median.
            - For small sets of values, percentiles are highly accurate
                (and potentially 100% accurate if the data is small enough).
            - As the quantity of values in a bucket grows, the algorithm
                begins to approximate the percentiles. It is effectively
                trading accuracy for memory savings.

        Returns:
            List[dict]: List of dicts with `key` as the perecentile range
                and `value` as the point where that percentile occurs.
        """
        result = self._simple_aggregation('percentiles',
                                          keyed=False,
                                          tdigest={'compression': precision},
                                          missing=missing,
                                          es_kwargs=es_kwargs)
        return result['values']

    def describe(self, extended=False, missing=None, **es_kwargs):
        """Get statistical details for a field.

        Args:
            extended (bool, optional): Increase the number of statistics returned.
                Defaults to False.
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.

        Returns:
            dict: Always returns `count`, `min`, `max`, `average`, `sum`.
                If `extended` is True then also returns `sum_of_squares`,
                `variance`, `std_deviation`, and `std_deviation_bounds`.

        """
        key = 'extended_stats' if extended else 'stats'
        return self._simple_aggregation(key, missing=missing, es_kwargs=es_kwargs)

    def histogram(self, interval=50, min_doc_count=1, missing=None, **es_kwargs):
        """Get a count of values for a field bucketed by interval.

        Args:
            interval (number, optional): Bin size for counts. Defaults to 50.
            min_doc_count (int, optional): Minimum number of documents for
                a bin to show up.
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.

        Returns:
            List[tuple]: Tuple consiting of (range, count).
        """
        def make_key(key):
            return '[{}, {})'.format(key, key+interval)
        results = self._simple_aggregation('histogram',
                                           interval=interval,
                                           min_doc_count=min_doc_count,
                                           missing=missing,
                                           es_kwargs=es_kwargs)
        return [(make_key(i['key']), i['doc_count']) for i in results]


class RangeMixin(object):
    """Adds comparator operators to a Field dtype."""

    __metaclass__ = ABCMeta

    @check_inversion
    def __lt__(self, value):
        return queries.Range(self.name).less_than(value)

    @check_inversion
    def __le__(self, value):
        return queries.Range(self.name).less_than_or_equal(value)

    @check_inversion
    def __gt__(self, value):
        return queries.Range(self.name).greater_than(value)

    @check_inversion
    def __ge__(self, value):
        return queries.Range(self.name).greater_than_or_equal(value)


class Numeric(Field, RangeMixin, AggregationMixin):
    """Numeric base for conditions and aggregations."""

    __metaclass__ = ABCMeta

    def percentile_ranks(self, values, missing=None, precision=100, **es_kwargs):
        """Get the percentage of observed values which are below a certain value.

        Args:
            values (List[int]): The values for which percentages are desired
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.
            precision (int, optional): Defines the accuracy - memory tradeoff.
                Larger precision result in greater accuracy at the cost of
                greater memory usage and computation time. Defaults to 1000.

        Notes:
            - Accuracy is proportional to q(1-q). This means that extreme
                percentiles (e.g. 99%) are more accurate than less extreme
                percentiles, such as the median.
            - For small sets of values, percentiles are highly accurate
                (and potentially 100% accurate if the data is small enough).
            - As the quantity of values in a bucket grows, the algorithm
                begins to approximate the percentiles. It is effectively
                trading accuracy for memory savings.

        Returns:
            List[dict]: List of dicts with `key` as the value and `value` as the
                percentage of items falling below that value.
        """
        result = self._simple_aggregation('percentile_ranks',
                                          keyed=False,
                                          values=values,
                                          tdigest={'compression': precision},
                                          missing=missing,
                                          es_kwargs=es_kwargs)
        return result['values']

    def sum(self, missing=None, **es_kwargs):
        """Get the sum of observed values.

        Args:
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.

        Returns:
            float: The sum
        """
        result = self._simple_aggregation('sum', missing=missing, es_kwargs=es_kwargs)
        return result['value']

    def median_absolute_deviation(self, missing=None, precision=1000, **es_kwargs):
        """Approximates the median absolute deviation for a field.

        Median absolute deviation is a measure of variability. It is a
        robust statistic, meaning that it is useful for describing data
        that may have outliers, or may not be normally distributed. For
        such data it can be more descriptive than standard deviation.

        https://en.wikipedia.org/wiki/Median_absolute_deviation

        Args:
            missing (num, optional): How documents that are missing a
                value should be treated. Defaults to ignore.
            precision (int, optional): Defines the accuracy - memory tradeoff.
                Larger precision result in greater accuracy at the cost of
                greater memory usage and computation time. Defaults to 1000.

        Returns:
            float: Median absolute deviation
        """
        result = self._simple_aggregation('median_absolute_deviation',
                                          compression=precision,
                                          missing=missing,
                                          es_kwargs=es_kwargs)
        return result['value']


class Integer(Numeric):
    """Integer field dtype."""

    dtype = 'integer'


class Float(Numeric):
    """Float field dtype."""

    dtype = 'float'


class Decimal(Numeric):
    """Decimal field dtype."""

    dtype = 'decimal'


class Boolean(Field):
    """Boolean field dtype."""

    dtype = 'boolean'


class String(Field):
    """String field dtype."""

    dtype = 'string'

    def match(self, term):
        """Condition checking whether term occurs in an analyzed field.

        Args:
            term (str): The term being matched
        """
        return queries.Match(self.name, term)

    @check_inversion
    def regexp(self, value):
        """Condition checking whether regular expression is to a field's value.

        Args:
            value (str): Regular expression
        """
        return queries.Regexp(self.name, value)

    @check_inversion
    def contains(self, value):
        """Condition checking whether string is contained in a field's value.

        This naive query is going to be significantly slower than using a
        regexp query or a match query with analyzed fields.

        Args:
            value (str): String that should exist
        """
        return queries.Wildcard(self.name, '*{}*'.format(value))

    @check_inversion
    def startswith(self, value):
        """Condition checking whether a field's value starts with a string.

        Args:
            value (str): String that a field's value should start with
        """
        return queries.Prefix(self.name, value)

    @check_inversion
    def endswith(self, value):
        """Condition checking whether a field's value ends with a string.

        This naive query is going to be significantly slower than using a
        regexp query or a match query with analyzed fields.

        Args:
            value (str): String that a field's value should end with
        """
        return queries.Wildcard(self.name, '*{}'.format(value))


class Date(Field, RangeMixin, AggregationMixin):
    """Date/datetime field dtype."""

    dtype = 'date'

    class Age(Field, RangeMixin):
        """Delta from datetime field dtype."""

        dtype = 'age'

        def age_to_dt(func):
            """Decorate a method to supply delta from today as date."""
            @wraps(func)
            def wrapper(obj, value):
                delta = datetime.today() - timedelta(value)
                return func(obj, delta)
            return wrapper

        __eq__ = age_to_dt(Field.__eq__)
        __ne__ = age_to_dt(Field.__ne__)
        __lt__ = age_to_dt(RangeMixin.__gt__)
        __le__ = age_to_dt(RangeMixin.__ge__)
        __gt__ = age_to_dt(RangeMixin.__lt__)
        __ge__ = age_to_dt(RangeMixin.__le__)

    @property
    def age(self):
        """Condition treating the value being compared as delta since today."""
        return self.Age(self.name, None)

    def _epoch_to_dt(func):
        @wraps(func)
        def wrapper(obj, *args, **kwargs):
            result = func(obj, *args, **kwargs)
            if result:
                return datetime.fromtimestamp(result / 1000)
        return wrapper

    average = _epoch_to_dt(AggregationMixin.average)
    max = _epoch_to_dt(AggregationMixin.max)
    min = _epoch_to_dt(AggregationMixin.min)

    def histogram(self, interval=50, min_doc_count=1):
        """Get a count of values for a field bucketed by interval.

        Args:
            interval (number, optional): Bin size for counts. Defaults to 50.
            min_doc_count (int, optional):
        """
        raise NotImplementedError("Bug Aaron to implement it.")


class Dummy(Field):
    """Dummy field for not implemented dtypes."""

    dtype = 'dummy'
