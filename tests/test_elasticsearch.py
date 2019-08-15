import pandas as pd

from fixtures import TEST_ID


def test_get_by_id(df):
    result = df.get(TEST_ID)
    assert result == {'ns1': {'attr1': 10}, 'attr2': 4}


def test_to_pandas(df):
    df = df.to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df.columns) == set([
        'ns2.big_fee', 'ns4.attr4', 'ns2.attr3', 'attr2', 'ns2.os',
        'ns1.attr2', 'ns1.attr1', 'ns3.test_date', 'ns1.ns2.attr1'
    ])


def test_empty_to_pandas(df):
    df = df.limit(0).to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_take(df):
    df = df[df.ns1.attr1.exists()]
    df = df.limit(3)
    assert df._limit == 3
    results = list(df.take(1))
    assert len(results) == 1
    assert df._limit == 3  # does not alter internal limit


def test_field_filter_collect_attr(df):
    df = df[df.ns1.attr1.exists()]
    results = list(df.collect(fields=['attr2'], raw=False))
    for i in results:
        assert 'ns1.attr1' not in i


def test_field_filter_collect_namespaced_attr(df):
    df = df[df.ns1.attr1.exists()]
    results = list(df.collect(fields=['ns1.attr1'], raw=False))
    for i in results:
        assert 'attr2' not in i
        assert 'ns1' in i


def test_field_filter_get_attr(df):
    result = df.get(TEST_ID, fields=['attr2'])
    assert result == {'attr2': 4}


def test_field_filter_get_namespaced_attr(df):
    result = df.get(TEST_ID, fields=['ns1.attr1'])
    assert result == {'ns1': {'attr1': 10}}


def test_count(df):
    df = df[df.ns1.attr1.exists()]
    assert df.count() == 4


def test_hits_returns_namespace_as_dict(df):
    df = df[df.ns2.attr3.exists()]
    results = list(df.collect(raw=False))
    assert results == [
        {
            'ns2': {
                'attr3': False
            }
        }
    ]
