import pandas as pd


def test_get_by_id(df, test_id):
    result = df.get(test_id)
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
    results = df.take(1)
    assert len(results) == 1
    assert df._limit == 3  # does not alter internal limit


def test_field_filter_collect_attr(df):
    df = df[df.ns1.attr1.exists()]
    results = list(df.collect(fields=['attr2']))
    for i in results:
        assert 'ns1.attr1' not in i


def test_field_filter_collect_namespaced_attr(df):
    df = df[df.ns1.attr1.exists()]
    results = list(df.collect(fields=['ns1.attr1'],))
    for i in results:
        assert 'attr2' not in i
        assert 'ns1' in i


def test_field_filter_get_attr(df, test_id):
    result = df.get(test_id, fields=['attr2'])
    assert result == {'attr2': 4}


def test_field_filter_get_namespaced_attr(df, test_id):
    result = df.get(test_id, fields=['ns1.attr1'])
    assert result == {'ns1': {'attr1': 10}}


def test_count(df):
    df = df[df.ns1.attr1.exists()]
    assert df.count() == 4


def test_hits_returns_namespace_as_dict(df):
    df = df[df.ns2.attr3.exists()]
    results = list(df.collect())
    assert results == [
        {
            'ns2': {
                'attr3': False
            }
        }
    ]


def test_include_score(df):
    df = df[df.ns1.attr1.exists()]
    result = list(df.collect(include_score=True))
    for i in result:
        assert i['_score']


def test_include_score_limit(df):
    df = df[df.ns1.attr1.exists()]
    result = list(df.collect(limit=1, include_score=True))
    for i in result:
        assert i['_score']


def test_include_id(df):
    df = df[df.ns1.attr1.exists()]
    results = list(df.collect(include_id=True))
    for i in results:
        assert i['_id']


def test_regexp_boost_applied(df):
    # check scores and make sure actually increases
    assert 0


def test_prefix_boost_applied(df):
    # check scores and make sure actually increases
    assert 0

def test_terms_boost_applied(df):
    df = df[df.ns1.attr1.isin([5, 10]).boost(2)]
    print list(df.collect(include_score=True))
    assert 0


def test_negative_weight(df):
    df = df[df.attr2.exists().boost(-2) | (df.ns1.attr1 >= 5)]
    print list(df.collect(include_score=True))
    assert 0


def test_score_increases_past_one(df):
    df = df[df.attr2.exists() | (df.ns1.attr1 > 5)]
    print list(df.collect(include_score=True))
    assert 0


def test_boost_zero_scores_zero(df):
    assert 0
