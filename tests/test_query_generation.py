from datetime import date, timedelta

import pytest

from bamboo import BadOperatorError, boost
from bamboo.queries import Bool, Term


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


def test_bool_query(df, reverse):
    df = df[df.ns2.attr3 == False] if reverse else df[False == df.ns2.attr3]
    assert df._body == {
        'query': {
            'term': {'ns2.attr3': False}
        }
    }
    list(df.collect())  # assert no query error


def test_currency(df, reverse):
    # ingest as 10.5999 but expect to 2 decimal places on retrieve
    df = df[df.ns2.big_fee == 10.60] if reverse else df[10.60 == df.ns2.big_fee]
    assert df._body == {
        'query': {
            'term': {'ns2.big_fee': 10.60}
        }
    }
    r = list(df.collect())
    assert r


def test_numeric_lt(df, reverse):
    df = df[df.ns1.attr1 < 5] if reverse else df[5 > df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'lt': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_lte(df, reverse):
    df = df[df.ns1.attr1 <= 5] if reverse else df[5 >= df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'lte': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_gt(df, reverse):
    df = df[df.ns1.attr1 > 5] if reverse else df[5 < df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'gt': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_gte(df, reverse):
    df = df[df.ns1.attr1 >= 5] if reverse else df[5 <= df.ns1.attr1]
    assert df._body == {
        'query': {
            'range': {
                'ns1.attr1': {'gte': 5}
            }
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_equal(df, reverse):
    df = df[df.ns1.attr1 == 5] if reverse else df[5 == df.ns1.attr1]
    assert df._body == {
        'query': {
            'term': {'ns1.attr1': 5}
        }
    }
    list(df.collect())  # assert no query error


def test_numeric_not_equal(df, reverse):
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
                    {'range': {'ns1.attr1': {'gt': 5}}},
                    {'term': {'ns4.attr4': 9}},
                    {'bool': {'must_not': [{'term': {u'ns1.attr2': 6.0}}]}}
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
                    {'range': {'ns1.attr1': {'gt': 5}}},
                    {'term': {'ns4.attr4': 9}},
                    {'bool': {'must_not': [{'term': {u'ns1.attr2': 6.0}}]}}
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_chained_invert_3(df):
    df = df[df.ns1.attr1 > 5]
    df = df[df.ns4.attr4 == 9]
    df = df[df.ns1.attr2 == 6.0]
    df = ~df
    # flattened
    # assert df._body == {
    #     'query': {
    #         'body': {
    #             'must_not': [
    #                 {
    #                     'range': {
    #                         'ns1.attr1': {'gt': 5}
    #                     }
    #                 },
    #                 {'term': {'ns4.attr4': 9}},
    #                 {'term': {'ns1.attr2': 6.0}}
    #             ]
    #         }
    #     }
    # }
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


def test_text_equals(df, reverse):
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
    dt = df._body['query']['range']['ns3.test_date']['lte']
    expected = date.today() - timedelta(days=days)
    assert dt.date() == expected
    matches = list(df.collect())
    assert len(matches) == 3


def test_age_query_inverted(df):
    days = 10
    df = ~df[df.ns3.test_date.age >= days]
    # test does not raise key error
    df._body['query']['bool']['must_not'][0]['range']['ns3.test_date']['lte']
    matches = list(df.collect())
    assert len(matches) == 14


def test_age_query_inverted_2(df):
    days = 10
    df = df[~df.ns3.test_date.age >= days]
    # test does not raise key error
    df._body['query']['bool']['must_not'][0]['range']['ns3.test_date']['lte']
    x = list(df.collect())  # assert no query error
    matches = list(df.collect())
    assert len(matches) == 14



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
    df = ~df
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
    df = ~df
    # flattened
    # assert df._body == {
    #     'query': {
    #         'bool': {
    #             'must_not': [
    #                 {'term': {'ns1.attr1': 5}},
    #                 {'term': {'ns1.attr2': 8.0}}
    #             ]
    #         }
    #     }
    # }
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
    # flattened
    # assert df._body == {
    #     'query': {
    #         'bool': {
    #             'must_not': [
    #                 {'term': {'ns1.attr1': 9}},
    #                 {'term': {'ns1.attr2': 5.0}}
    #             ]
    #         }
    #     }
    # }
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
    df = ~df
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
    # not a or not b
    # a and b
    df = df[~(df.ns1.attr1 == 9) | ~(df.ns1.attr2 == 5.0)]
    df = ~df
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
    # flattened
    # assert df._body == {
    #     'query': {
    #         'bool': {
    #             'must': [
    #                 {
    #                     'term': {'ns1.attr1': 9}
    #                 }
    #             ],
    #             'must_not': [
    #                 {
    #                     'term': {'ns1.attr2': 5.0}
    #                 }
    #             ]
    #         }
    #     }
    # }
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {'term': {u'ns1.attr1': 9}},
                    {
                        'bool': {
                            'must_not': [
                                {'term': {u'ns1.attr2': 5.0}}
                            ]
                        }
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
                    {'term': {u'ns1.attr1': 9}},
                    {
                        'bool': {
                            'must_not': [
                                {'term': {u'ns1.attr2': 5.0}}
                            ]
                        }
                    },
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
    assert len(list(one.collect())) == 1
    assert len(list(two.collect())) == 2


def test_match(df):
    df = df[df.ns2.os.match('mac')]
    assert df._body == {
        'query': {
            'match': {'ns2.os': 'mac'}
        }
    }
    list(df.collect())  # assert no query error


def test_boost(df):
    x = df[(df.ns1.attr1 > 5).boost(2.0)]
    mask = df.ns1.attr1 > 5
    y = df[mask.boost(2.0)]
    z = df[boost(mask, 2.0)]
    assert x._body == y._body == z._body
    assert x._body == {
        'query': {
            'range': {
                'ns1.attr1': {
                    'boost': 2.0,
                    'gt': 5
                }
            }
        }
    }
    list(df.collect())  # assert no query error


def test_boost_non_operator(df):
    x = df[boost(df.ns1.exists(), 2.0)]
    y = df[df.ns1.exists().boost(2.0)]
    assert x._body == y._body
    assert x._body == {
        'query': {
            'exists': {
                'boost': 2.0,
                'field': 'ns1'
            }
        }
    }
    list(df.collect())  # assert no query error


def test_boost_two_filters(df):
    df = df[boost(df.ns1.attr1 > 5, 2.0) & boost(df.attr2 == 6, 3.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {
                                'gt': 5,
                                'boost': 2.0
                            }
                        }
                    },
                    {
                        'term': {
                            'attr2': {
                                'boost': 3.0,
                                'value': 6
                            }
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_boost_two_filters_one_boost(df):
    df = df[boost(df.ns1.attr1 > 5, 2.0) & (df.attr2 == 6)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {
                                'gt': 5,
                                'boost': 2.0
                            }
                        }
                    },
                    {
                        'term': {
                            'attr2': 6
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_boost_filter_combination(df):
    mask = (df.attr2 > 5) & (df.attr2 == 6)
    df = df[mask.boost(2.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'attr2': {
                                'gt': 5,
                            }
                        }
                    },
                    {
                        'term': {
                            'attr2': 6
                        }
                    }
                ],
                'boost': 2.0
            }
        }
    }
    list(df.collect())  # assert no query error


def test_boost_chained_condition(df):
    df = df[boost(df.ns1.attr1 > 5, 2.0)]
    df = df[boost(df.attr2 == 6, 3.0)]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'ns1.attr1': {
                                'gt': 5,
                                'boost': 2.0
                            }
                        }
                    },
                    {
                        'term': {
                            'attr2': {
                                'boost': 3.0,
                                'value': 6
                            }
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_deeply_nested_query(df):
    # a or (b and (c or not d))
    c1 = df.ns1.attr1 == 5
    c2 = df.ns1.attr2 == 8.0
    c3 = df.ns2.attr3 == True
    c4 = df.attr2 == 6
    df = df[c1 | (c2 & (c3 | ~c4))]
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {'term': {'ns1.attr1': 5}},
                    {
                        'bool': {
                            'must': [
                                {'term': {'ns1.attr2': 8.0}},
                                {
                                    'bool': {
                                        'should': [
                                            {'term': {'ns2.attr3': True}},
                                            {'bool': {'must_not': [{'term': {u'attr2': 6}}]}}
                                        ]
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


def test_deeply_nested_query_2(df):
    c1 = df.ns1.attr1 == 5
    c2 = df.ns1.attr2 == 8.0
    c3 = df.ns2.attr3 == True
    c4 = df.attr2 == 6
    df = df[c1 & (c2 | (c3 & ~c4))]
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {'term': {u'ns1.attr1': 5}},
                    {
                        'bool': {
                            'should': [
                                {'term': {u'ns1.attr2': 8.0}},
                                {
                                    'bool': {
                                        'must': [
                                            {'term': {u'ns2.attr3': True}},
                                            {'bool': {'must_not': [{'term': {u'attr2': 6}}]}}
                                        ]
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


def test_scripted_query(df):
    source = "doc['ns1.attr1'].value > doc['ns4.attr4'].value"
    df = df[df.query(source)]
    assert df._body == {
        'query': {
            'script': {
                'script': {
                    'lang': 'painless',
                    'source': "doc['ns1.attr1'].value > doc['ns4.attr4'].value"
                }
            }
        }
    }
    list(df.collect())  # assert no query error


def test_leaking_union():
    c1 = Bool(Term('val1', 1))
    c2 = Term('val2', 2)
    c1 | c2
    assert c1() == {'term': {'val1': 1}}


def test_flatten():
    c1 = Bool(should=[Term('val1', 1), Term('val3', 3)])
    c2 = Term('val2', 2)
    c3 = c1 | c2
    assert c3() == {
        'bool': {
            'should': [
                {'term': {'val1': 1}},
                {'term': {'val3': 3}},
                {'term': {'val2': 2}}
            ]
        }
    }


def test_no_flatten_if_additional_params():
    c1 = Bool(must=[Term('val1', 1), Term('val3', 3)])
    c2 = Term('val2', 2)
    c3 = c1 | c2
    assert c3() == {
        'bool': {
            'should': [
                {
                    'bool': {
                        'must': [
                            {'term': {'val1': 1}},
                            {'term': {'val3': 3}}
                        ]
                    }
                },
                {'term': {'val2': 2}},
            ]
        }
    }


def test_no_flatten_if_additional_params_2():
    c1 = Bool(should=[Term('val1', 1), Term('val3', 3)])
    c2 = Term('val2', 2)
    c3 = c1 & c2
    assert c3() == {
        'bool': {
            'must': [
                {
                    'bool': {
                        'should': [
                            {'term': {'val1': 1}},
                            {'term': {'val3': 3}}
                        ]
                    }
                },
                {'term': {'val2': 2}},
            ]
        }
    }


def test_no_flatten_3():
    c1 = Bool(should=[Term('val1', 1), Term('val2', 2)])
    c2 = Bool(should=[Term('val3', 3), Term('val4', 4)])
    c3 = c1 & c2
    assert c3() == {
        'bool': {
            'must': [
                {
                    'bool': {
                        'should': [
                            {'term': {'val1': 1}},
                            {'term': {'val2': 2}}
                        ]
                    }
                },
                {
                    'bool': {
                        'should': [
                            {'term': {'val3': 3}},
                            {'term': {'val4': 4}}
                        ]
                    }
                }
            ]
        }
    }


def test_no_flatten_4():
    c1 = Bool(must=[Term('val1', 1), Term('val2', 2)])
    c2 = Bool(must=[Term('val3', 3), Term('val4', 4)])
    c3 = Bool(must=[Term('val5', 5), Term('val6', 6)])
    c4 = c1 | c2 | c3
    assert c4() == {
        'bool': {
            'should': [
                {
                    'bool': {
                        'must': [
                            {'term': {'val1': 1}},
                            {'term': {'val2': 2}}
                        ]
                    }
                },
                {
                    'bool': {
                        'must': [
                            {'term': {'val3': 3}},
                            {'term': {'val4': 4}}
                        ]
                    }
                },
                {
                    'bool': {
                        'must': [
                            {'term': {'val5': 5}},
                            {'term': {'val6': 6}}
                        ]
                    }
                }
            ]
        }
    }


def test_isin(df):
    df = df[df.ns1.attr1.isin([1, 10])]
    assert df._body == {'query': {'terms': {'ns1.attr1': [1, 10]}}}
    list(df.collect())  # assert no query error


def test_negate_inner_should_with_must_not(df):
    # not (z and (x or not y)) == not z or not (x or not y)
    # not z or not (x or not y) == not z or (not x and y)
    x = Term('x', 1)
    y = Term('y', 2)
    z = Term('z', 3)
    b = Bool(should=[x, ~y])
    b = ~Bool(must=[z, b])
    query = b()
    assert query == {'bool': {
        'should': [
            {'bool': {'must_not': [{'term': {'z': 3}}]}},
            {'bool': {'must': [
                {'bool': {'must_not': [{'term': {'x': 1}}]}},
                {'term': {'y': 2}}
            ]}}
        ]
    }}
    assert df.execute({'query': query}, size=1)


def test_negate_inner_must_with_must_not(df):
    # not (z and (x and not y)) == not z or not (x and not y)
    # not z or not (x and not y) == not z or (not x or y)
    x = Term('x', 1)
    y = Term('y', 2)
    z = Term('z', 3)
    b = Bool(must=[x, ~y])
    b = ~Bool(must=[z, b])
    query = b()
    assert query == {'bool': {
        'should': [
            {'bool': {'must_not': [{'term': {'z': 3}}]}},
            {'bool': {'should': [
                {'bool': {'must_not': [{'term': {'x': 1}}]}},
                {'term': {'y': 2}}]
            }}
        ]
    }}
    assert df.execute({'query': query}, size=1)


def test_negate_inner_must_with_should(df):
    # not (z and (x and y)) == not z or not (x and y)
    # not z or not (x and y) == not z or (not x or not y)
    x = Term('x', 1)
    y = Term('y', 2)
    z = Term('z', 3)
    b = Bool(must=[x, y])
    b = ~Bool(must=[z, b])
    query = b()
    assert query == {'bool': {
        'should': [
            {'bool': {'must_not': [{'term': {'z': 3}}]}},
            {'bool': {'should': [
                {'bool': {'must_not': [{'term': {'x': 1}}]}},
                {'bool': {'must_not': [{'term': {'y': 2}}]}}
            ]}}
        ]
    }}
    assert df.execute({'query': query}, size=1)


def test_or_with_inner_or(df):
    # x or (y or z)
    x = Term('x', 1)
    y = Term('y', 2)
    z = Term('z', 3)
    b = Bool(should=[x, Bool(should=[y, z])])
    query = b()
    assert query == {'bool': {
        'should': [
            {'term': {'x': 1}},
            {'bool': {'should': [
                {'term': {'y': 2}},
                {'term': {'z': 3}}
            ]}}
        ]
    }}
    assert df.execute({'query': query}, size=1)


def test_and_with_inner_and(df):
    # x and (y and z)
    x = Term('x', 1)
    y = Term('y', 2)
    z = Term('z', 3)
    b = Bool(must=[x, Bool(must=[y, z])])
    query = b()
    assert query == {'bool': {
        'must': [
            {'term': {'x': 1}},
            {'bool': {'must': [
                {'term': {'y': 2}},
                {'term': {'z': 3}}
            ]}}
        ]
    }}
    assert df.execute({'query': query}, size=1)


def test_disjunction_is_associative():
    x = Bool(must=[Term(1,1), Term(2,2)])
    y = Bool(should=[Term(3,3), Term(4,4)])
    z = x | y
    # flatten
    assert z() == {'bool': {'should': [
        {'bool': {'must': [
            {'term': {1: 1}},
            {'term': {2: 2}}
        ]}},
        {'term': {3: 3}},
        {'term': {4: 4}},
    ]}}
    # assert z() == {'bool': {'should': [
    #     {'bool': {'must': [
    #         {'term': {1: 1}},
    #         {'term': {2: 2}}
    #     ]}},
    #     {'bool': {'should': [
    #         {'term': {3: 3}},
    #         {'term': {4: 4}}
    #     ]}}
    # ]}}


def test_bool_and_must_not_not_combined():
    # (a and not b) and not c
    # a and not (b or c)
    # a and not b and not c
    x = Bool(must=Term(1,1), must_not=Term(2,2))
    y = Bool(must_not=Term(3,3))
    z = x & y
    # flattened
    # assert z() == {'bool': {
    #     'must': [{'term': {1: 1}}],
    #     'must_not': [
    #         {'term': {2: 2}},
    #         {'term': {3: 3}}
    #     ],
    # }}
    # assert z() == {'bool': {'must': [
    #     {'bool': {
    #         'must_not': [{'term': {2: 2}}],
    #         'must': [{'term': {1: 1}}]
    #     }},
    #     {'bool': {'must_not': [{'term': {3: 3}}]}}
    # ]}}
    assert z() == {'bool': {'must': [
        {'term': {1: 1}},
        {'bool': {'must_not': [{'term': {2: 2}}]}},
        {'bool': {'must_not': [{'term': {3: 3}}]}}
    ]}}


def test_boosted_flattened_bool():
    x = boost(Bool(Term(1, 1)), 2.0)
    y = boost(Term(1, 1), 2.0)
    assert x() == y()


def test_combined_inverts_and():
    # (not x and y) and not z
    x = Bool(must_not=[Term(1,1)])
    y = Bool(must=Term(2,2))
    z = Bool(must_not=Term(3,3))
    a = x & y
    b = a & z
    assert b() == {'bool': {'must': [
        {'bool': {'must_not': [{'term': {1: 1}}]}},
        {'term': {2: 2}},
        {'bool': {'must_not': [{'term': {3: 3}}]}}
    ]}}


def test_combined_inverts_or():
    # (not x and y) or not z
    x = Bool(must_not=[Term(1,1)])
    y = Bool(must=Term(2,2))
    z = Bool(must_not=Term(3,3))
    a = x & y
    b = a | z
    assert b() == {'bool': {
        'should': [
            {'bool': {'must': [
                {'bool': {'must_not': [{'term': {1: 1}}]}},
                {'term': {2: 2}},
            ]}},
            {'bool': {'must_not': [{'term': {3: 3}}]}}
        ]
    }}


def test_filter(df):
    df = df.filter(df.ns1.attr1==5)
    assert df._body == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'term': {'ns1.attr1': 5}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_filter_multiple(df):
    df = df.filter(df.ns1.attr1==5, df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'filter': [
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


def test_filter_plus_conditions(df):
    df = df[df.ns1.attr1==5]
    df = df.filter(df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 5}
                    }
                ],
                'filter': [
                    {
                        'term': {'ns1.attr2': 8.0}
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_outer_invert_filter(df):
    df = ~df.filter(df.ns1.attr1==5)
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'must_not': [
                                {'term': {u'ns1.attr1': 5}}
                            ]
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_inner_invert_filter(df):
    df = df.filter(~df.ns1.attr1==5)
    assert df._body == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'bool': {
                            'must_not': [
                                {'term': {u'ns1.attr1': 5}}
                            ]
                        }
                    }
                ]
            }
        }
    }
    list(df.collect())  # assert no query error



def test_filtered_or(df):
    df = df.filter((df.ns1.attr1 == 5) | (df.ns1.attr2 == 8.0))
    assert df._body == {
        'query': {
            'bool': {
                'filter': [
                    {
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
                ]
            }
        }
    }
    list(df.collect())  # assert no query error


def test_filtered_on_self_raises(df):
    df = df[df.ns1.attr1 == 5]
    with pytest.raises(TypeError):
        df.filter(df)


def test_and_filter(df):
    df = df.filter(df.ns1.attr1==5) & df.filter(df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'bool': {
                            'filter': [
                                {
                                    'term': {'ns1.attr1': 5}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'filter': [
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


def test_or_filter(df):
    df = df.filter(df.ns1.attr1==5) | df.filter(df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'bool': {
                            'filter': [
                                {
                                    'term': {'ns1.attr1': 5}
                                }
                            ]
                        }
                    },
                    {
                        'bool': {
                            'filter': [
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


def test_or_filter_plus_conditions(df):
    df1 = df[df.ns1.attr1==5]
    df = df1.filter(df.ns1.attr1==5) | df.filter(df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'should': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'bool': {
                            'filter': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                            ]
                        }
                    },
                    {
                        'bool': {
                            'filter': [
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


def test_and_filter_plus_conditions(df):
    df1 = df[df.ns1.attr1==5]
    df = df1.filter(df.ns1.attr1==5) & df.filter(df.ns1.attr2==8.0)
    assert df._body == {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {'ns1.attr1': 5}
                    },
                    {
                        'bool': {
                            'filter': [
                                {
                                    'term': {'ns1.attr1': 5}
                                },
                            ]
                        }
                    },
                    {
                        'bool': {
                            'filter': [
                                {
                                    'term': {'ns1.attr2': 8.0}
                                },
                            ]
                        }
                    },
                ]
            }
        }
    }
    list(df.collect())  # assert no query error
