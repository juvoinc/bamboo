from datetime import datetime


def test_value_counts(df):
    counts = df.ns1.attr1.value_counts()
    assert counts == [(5, 2), (1, 1), (10, 1)]


def test_value_counts_above_size(df):
    counts = df.ns1.attr1.value_counts(n=1)
    assert counts == [(5, 2), ('OTHER', 2)]


def test_value_counts_subquery(df):
    df = df[df.ns1.attr1 < 7]
    counts = df.ns1.attr1.value_counts()
    assert counts == [(5, 2), (1, 1)]


def test_value_counts_normalize(df):
    counts = df.ns1.attr1.value_counts(normalize=True)
    assert counts == [(5, 0.5), (1, 0.25), (10, 0.25)]


def test_float_histogram(df):
    hist = df.ns4.attr4.histogram()
    assert hist == [('[0.0, 50.0)', 1), ('[50.0, 100.0)', 2), ('[100.0, 150.0)', 1)]


def test_average(df):
    r = df.ns1.attr1.average()
    assert r == 5.25


def test_date_average(df):
    r = df.ns3.test_date.average()
    assert r == datetime(2019, 7, 5, 16, 41, 58, 571000)


def test_max(df):
    r = df.ns1.attr1.max()
    assert r == 10


def test_date_max(df):
    r = df.ns3.test_date.max()
    assert r == datetime(2019, 7, 15, 4, 35, 55, 713000)


def test_min(df):
    r = df.ns1.attr1.min()
    assert r == 1.0


def test_date_min(df):
    r = df.ns3.test_date.min()
    assert r == datetime(2019, 6, 30, 17, 0)


def test_percentiles(df):
    r = df.ns1.attr1.percentiles()
    assert r == [{'value': 1.0, 'key': 1.0}, {'value': 1.0, 'key': 5.0}, {'value': 3.0, 'key': 25.0},
                 {'value': 5.0, 'key': 50.0}, {'value': 7.5, 'key': 75.0}, {'value': 10.0, 'key': 95.0},
                 {'value': 10.0, 'key': 99.0}]


def test_date_percentiles(df):
    r = df.ns3.test_date.percentiles()
    assert r == [
        {'value': 1561939200000.0, 'key': 1.0, 'value_as_string': '2019-07-01T00:00:00.000'},
        {'value': 1561939200000.0, 'key': 5.0, 'value_as_string': '2019-07-01T00:00:00.000'},
        {'value': 1561949550000.0, 'key': 25.0, 'value_as_string': '2019-07-01T02:52:30.000'},
        {'value': 1561980600000.0, 'key': 50.0, 'value_as_string': '2019-07-01T11:30:00.000'},
        {'value': 1562888066784.75, 'key': 75.0, 'value_as_string': '2019-07-11T23:34:26.784'},
        {'value': 1563190555713.0, 'key': 95.0, 'value_as_string': '2019-07-15T11:35:55.713'},
        {'value': 1563190555713.0, 'key': 99.0, 'value_as_string': '2019-07-15T11:35:55.713'}
    ]


def test_percentile_ranks(df):
    r = df.ns1.attr1.percentile_ranks([5, 10])
    assert r == [{u'value': 50.0, u'key': 5.0}, {u'value': 100.0, u'key': 10.0}]


def test_sum(df):
    r = df.ns1.attr1.sum()
    assert r == 21.0


def test_sum_missing(df):
    r = df.ns1.attr1.sum(missing=1)
    assert r == 34.0


def test_median_absolute_deviation(df):
    r = df.ns1.attr1.median_absolute_deviation()
    assert r == 2.0


def test_count_distinct(df):
    assert df[df.ns1.attr1.exists()].count() == 4
    assert df.ns1.attr1.nunique(precision=3) == 3


def test_stats(df):
    stats = df.ns1.attr1.describe(extended=False)
    assert stats == {'count': 4, 'max': 10.0, 'sum': 21.0, 'avg': 5.25, 'min': 1.0}


def test_extended_stats(df):
    stats = df.ns1.attr1.describe(extended=True)
    assert stats == {
        'count': 4, 'min': 1.0, 'sum_of_squares': 151.0, 'max': 10.0, 'sum': 21.0,
        'std_deviation': 3.191786333700926,
        'std_deviation_bounds': {
            'upper': 11.633572667401852, 'lower': -1.133572667401852},
        'variance': 10.1875, 'avg': 5.25
    }


def test_date_stats(df):
    stats = df.ns3.test_date.describe()
    assert stats == {
        'count': 3, 'sum_as_string': '2118-07-12T23:05:55.713', 'min': 1561939200000.0,
        'max': 1563190555713.0, 'sum': 4687110355713.0, 'min_as_string': '2019-07-01T00:00:00.000',
        'max_as_string': '2019-07-15T11:35:55.713', 'avg_as_string': '2019-07-05T23:41:58.571',
        'avg': 1562370118571.0
    }
