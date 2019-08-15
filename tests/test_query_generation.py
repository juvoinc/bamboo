import pytest

from bamboo import BadOperatorError


@pytest.fixture(params=[True, False])
def reverse(request):
    return request.param


def test_namespace_exists(df):
    df = df[df.ns1.exists()]
    assert df._body == {
        'query': {
            'exists': {'field': 'ns1'}
        }
    }
    r = list(df.collect())  # assert no query error
    assert all('ns1' in i for i in r)


def test_namespace_exists_inverted(df):
    df = ~df[df.ns1.exists()]
    assert df._body == {
        'query': {
            'bool': {
                'must_not': [
                    {'exists': {'field': 'ns1'}}
                ]
            }
        }
    }
    r = list(df.collect())  # assert no query error
    assert all('ns1' not in i for i in r)


def test_different_dtypes(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns2.os == 'android']
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {'gt': 5}
                        }
                    },
                    {
                        'term': {'ns2.os': 'android'}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_namespace(df):
    ns1 = vars(df.ns1)
    ns2 = vars(df.ns2)
    nested_ns2 = vars(df.ns1.ns2)
    assert ns1 != ns2 != nested_ns2
    df = df[df.ns1.ns2.attr1 > 5]
    assert df._body == {
        'query': {
            'range': {
                'ns1.ns2.attr1': {
                    'gt': 5
                }
            }
        }
    }
    list(df.collect())  # assert no query error


def test_bool_query(ds, reverse):
    df = df[df.ns2.attr3 == False] if reverse else df[False == df.ns2.attr3]
    assert df._body == {
        'query': {
            'term': {'ns2.attr3': False}
        }
    }
    list(df.collect())  # assert no query error


def test_currency(ds, reverse):
    # ingest as 10.5999 but expect to 2 decimal places on retrieve
    df = df[df.ns2.big_fee == 10.60] if reverse else df[10.60 == df.ns2.big_fee]
    assert df._body == {
        'query': {
            'term': {'ns2.big_fee': 10.60}
        }
    }
    r = list(df.collect())
    assert r


def test_numeric_lt(ds, reverse):
    df = df[df.ns1.attr1 < 5] if reverse else df[5 > df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'lt': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_lte(ds, reverse):
    df = df[df.ns1.attr1 <= 5] if reverse else df[5 >= df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'lte': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_gt(ds, reverse):
    df = df[df.ns1.attr1 > 5] if reverse else df[5 < df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'gt': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_gte(ds, reverse):
    df = df[df.ns1.attr1 >= 5] if reverse else df[5 <= df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'gte': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_equal(ds, reverse):
    df = df[df.ns1.attr1 == 5] if reverse else df[5 == df.ns1.attr1]
    assert df._body == {
        'query': {
            'term': {'ns1.attr1': 5}
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_not_equal(ds, reverse):
    df = df[df.ns1.attr1 != 5] if reverse else df[5 != df.ns1.attr1]
    assert df._body == {
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {'ns1.attr1': 5}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_multiple_numeric(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns4.attr4 == 9]
    df = df[df.ns1.attr2 == 6.0]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {'gt': 5}
                        }
                    },
                    {
                        'term': {'ns4.attr4': 9}
                    },
                    {
                        'term': {'ns1.attr2': 6.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_invert(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns4.attr4 == 9]
    df = df[df.ns1.attr2 != 6.0]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {'gt': 5}
                        }
                    },
                    {
                        'term': {'ns4.attr4': 9}
                    }
                ],
                'must_not': [
                    {
                        'term': {'ns1.attr2': 6.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_invert_2(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns4.attr4 == 9]
    df = df[~df.ns1.attr2 == 6.0]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {'gt': 5}
                        }
                    },
                    {
                        'term': {'ns4.attr4': 9}
                    }
                ],
                'must_not': [
                    {
                        'term': {'ns1.attr2': 6.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_invert_3(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns4.attr4 == 9]
    df = df[df.ns1.attr2 == 6.0]
    df = ~ds
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'range': {
                                        'ns1.attr1': {'gt': 5}
                                    }
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {
                                        'ns4.attr4': 9
                                    }
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {
                                        'ns1.attr2': 6.0
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_text_equals(ds, reverse):
    df = df[df.ns2.os == 'value'] if reverse else df['value' == df.ns2.os]
    assert df._body == {
        'query': {
            'term': {'ns2.os': 'value'}
        }
    }
    list(df.collect())  # assert no query error


def test_text_regexmatch(df):
    df = df[df.ns2.os.regexp('(galaxy)|(note)')]
    assert df._body == {
        'query': {
            'regexp': {'ns2.os': '(galaxy)|(note)'}
        }
    }
    list(df.collect())  # assert no query error


def test_text_contains(df):
    df = df[df.ns2.os.contains('value')]
    assert df._body == {
        'query': {
            'wildcard': {'ns2.os': '*value*'}
        }
    }
    list(df.collect())  # assert no query error


def test_text_startswith(df):
    df = df[df.ns2.os.startswith('value')]
    assert df._body == {
        'query': {
            'prefix': {'ns2.os': 'value'}
        }
    }
    list(df.collect())  # assert no query error


def test_text_endswith(df):
    df = df[df.ns2.os.endswith('value')]
    assert df._body == {
        'query': {
            'wildcard': {'ns2.os': '*value'}
        }
    }
    list(df.collect())  # assert no query error


def test_text_notequals(df):
    df = df[df.ns2.os != 'value'] if reverse else df['value' != df.ns2.os]
    assert df._body == {
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {'ns2.os': 'value'}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_or_condition(df):
    df = df[(df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0)]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'term': {'ns1.attr2': 8.0}
                    },
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_or_condition_2(df):
    df = df[df.ns1.attr1 == 5] | df[df.ns1.attr2 == 8.0]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'term': {'ns1.attr2': 8.0}
                    },
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_and_condition(df):
    df = df[(df.ns1.attr1 == 5) & (df.ns1.attr2 == 8.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'term': {'ns1.attr2': 8.0}
                    },
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_and_condition_2(df):
    df = df[df.ns1.attr1 == 5] & df[df.ns1.attr2 == 8.0]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'term': {'ns1.attr2': 8.0}
                    },
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_outer_or(df):
    df = (df[df.ns1.attr1 == 5] & df[df.ns1.attr2 == 8.0]) \
        | (df[df.ns2.attr3 == True] & df[df.attr2 == 6]) \
        | df[df.ns4.attr4 == 1]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'must': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    },
                    {
                        'term': {'ns4.attr4': 1}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_outer_or_2(df):
    mask = ((df.ns1.attr1 == 5) & (df.ns1.attr2 == 8.0)) \
        | ((df.ns2.attr3 == True) & (df.attr2 == 6)) \
        | (df.ns4.attr4 == 1)
    df = df[mask]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'must': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    },
                    {
                        'term': {'ns4.attr4': 1}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_outer_and(df):
    df = (df[df.ns1.attr1 == 5] | df[df.ns1.attr2 == 8.0]) \
        & (df[df.ns2.attr3 == True] | df[df.attr2 == 6]) \
        & df[df.ns4.attr4 == 1]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    },
                    {
                        'term': {'ns4.attr4': 1}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_outer_and_2(df):
    mask = ((df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0)) \
        & ((df.ns2.attr3 == True) | (df.attr2 == 6)) \
        & (df.ns4.attr4 == 1)
    df = df[mask]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    },
                    {
                        'term': {'ns4.attr4': 1}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_inner_or(df):
    one = df[(df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0)]
    two = df[(df.ns2.attr3 == True) | (df.attr2 == 6)]
    df = one & two
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_inner_or_2(df):
    df = df[(df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0)]
    df = df[(df.ns2.attr3 == True) | (df.attr2 == 6)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'should': [
                                {
                                    'term': {'ns2.attr3': True}
                                },
                                {
                                    'term': {'attr2': 6}
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_bad_operator(df):
    with pytest.raises(BadOperatorError):
        df = df[df.ns1.attr1 is False]


def test_age_query(df):
    days = 10
    df = df[df.ns3.test_date.age >= days]
    # direct access for rounding
    print df._body
    dt = df._body['query']['range']['ns3.test_date']['lte']
    expected = date.today() - timedelta(days=days)
    assert dt.date() == expected
    list(df.collect())  # assert no query error


def test_age_query_inverted(df):
    days = 10
    df = ~df[df.ns3.test_date.age >= days]
    # test does not raise key error
    df._body['query']['bool']['must_not'][0]['range']['ns3.test_date']['lte']
    list(df.collect())  # assert no query error


def test_age_query_inverted_2(df):
    days = 10
    df = df[~df.ns3.test_date.age >= days]
    # test does not raise key error
    df._body['query']['bool']['must_not'][0]['range']['ns3.test_date']['lte']
    list(df.collect())  # assert no query error


def test_exists_query(df):
    df = df[df.ns1.attr1.exists()]
    assert df._body == {
        'query': {
            'exists': {
                'field': 'ns1.attr1'
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert(df):
    df = df[~df.ns1.attr1 == 5]
    assert df._body == {
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {'ns1.attr1': 5}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert_2(df):
    df = ~df[df.ns1.attr1 == 5]
    assert df._body == {
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {'ns1.attr1': 5}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert_or(df):
    df = df[(df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0)]
    df = ~ds
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr1': 5}
                                }
                            ]

                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]

                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert_and(df):
    df = df[(df.ns1.attr1 == 5) & (df.ns1.attr2 == 8.0)]
    df = ~ds
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr1': 5}
                                }
                            ]

                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr2': 8.0}
                                }
                            ]

                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_double_invert_and(df):
    df = df[~(df.ns1.attr1 == 9) & ~(df.ns1.attr2 == 5.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr1': 9}
                                }
                            ]

                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr2': 5.0}
                                }
                            ]

                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_double_invert_or(df):
    df = df[~(df.ns1.attr1 == 9) | ~(df.ns1.attr2 == 5.0)]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr1': 9}
                                }
                            ]

                        }
                    },
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'term': {'ns1.attr2': 5.0}
                                }
                            ]

                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert_invert_and(df):
    df = df[~(df.ns1.attr1 == 9) & ~(df.ns1.attr2 == 5.0)]
    df = ~ds
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'term': {'ns1.attr1': 9}
                    },
                    {
                        'term': {'ns1.attr2': 5.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_invert_invert_or(df):
    df = df[~(df.ns1.attr1 == 9) | ~(df.ns1.attr2 == 5.0)]
    df = ~ds
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 9}
                    },
                    {
                        'term': {'ns1.attr2': 5.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_invert(df):
    df = df[(df.ns1.attr1 == 9) & ~(df.ns1.attr2 == 5.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 9}
                    }
                ],
                'must_not': [
                    {
                        'term': {'ns1.attr2': 5.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_nested_invert_or(df):
    df = df[(df.ns1.attr1 == 9) | ~(df.ns1.attr2 == 5.0)]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'term': {'ns1.attr1': 9}
                    }
                ],
                'must_not': [
                    {
                        'term': {'ns1.attr2': 5.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_double_negative(df):
    df = df[~df.ns1.attr1 != 5]
    assert df._body == {
        'query': {
            'term': {'ns1.attr1': 5}
        }
    }
    list(df.collect())  # assert no query error


def test_double_negative_2(df):
    df = ~df[df.ns1.attr1 != 5]
    assert df._body == {
        'query': {
            'term': {'ns1.attr1': 5}
        }
    }
    list(df.collect())  # assert no query error


def test_limit(df):
    df = df[df.ns1.attr1.exists()]
    one = df.limit(1)
    two = df.limit(2)
    assert len(list(one.collect(raw=False))) == 1
    assert len(list(two.collect(raw=False))) == 2


def test_match(df):
    df = df[df.ns2.os.match('mac')]
    assert df._body == {
        'query': {
            'match': {'ns2.os': 'mac'}
        }
    }
    list(df.collect())  # assert no query error
