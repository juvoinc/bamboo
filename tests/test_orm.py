import pytest

from bamboo.field import Field, Namespace


def test_dtypes(df):
    assert df.dtypes == {
        'ns1': {
            'attr1': 'integer',
            'attr2': 'float',
            'ns2': {
                'attr1': 'integer'
            }
        },
        'ns2': {
            'attr3': 'boolean',
            'os': 'string',
            'big_fee': 'decimal'
        },
        'ns3': {
            'test_date': 'date'
        },
        'ns4': {
            'attr4': 'float'
        },
        'attr2': 'integer'
    }


def test_undefined_attr_access_fails(df):
    # import pdb; pdb.set_trace()
    with pytest.raises(AttributeError):
        df.attr1


def test_list_global_fields(df):
    fields = set(map(str, df.fields))
    assert fields == {'attr2'}


def test_list_namespaces(df):
    namespaces = set(map(str, df.namespaces))
    assert namespaces == {'ns1', 'ns2', 'ns3', 'ns4'}


def test_list_chained_fields(df):
    fields = set(df.ns1.fields)
    assert fields == {'attr2', 'attr1'}


def test_list_chained_fields_2(df):
    fields = set(df.ns1.ns2.fields)
    assert fields == {'attr1'}


def test_get_namespace(df):
    namespace = df.get_namespace('ns1')
    assert isinstance(namespace, Namespace)
    fields = set(namespace.fields)
    assert fields == {'attr2', 'attr1'}


def test_get_dot_namespace(df):
    namespace = df.ns1
    assert isinstance(namespace, Namespace)
    fields = set(namespace.fields)
    assert fields == {'attr2', 'attr1'}


def test_get_namespace_attr(df):
    namespace = df['ns1']
    assert isinstance(namespace, Namespace)
    fields = set(namespace.fields)
    assert fields == {'attr2', 'attr1'}


def get_namespaced_attr(df):
    assert isinstance(df.ns1.attr2, Field)


def get_namespaced_attr_2(df):
    assert isinstance(df['ns1']['attr2'], Field)


def test_get_field_no_namespace(df):
    assert isinstance(df['attr2'], Field)


def test_dot_get_field_no_namespace(df):
    assert isinstance(df.attr2, Field)
