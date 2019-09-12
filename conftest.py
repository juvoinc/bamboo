"""Pytest fixtures."""
import pytest
from elasticsearch.helpers import bulk

from bamboo import DataFrame
from bamboo.config import config

TEST_INDEX = 'bamboo-test-index-'

TEST_DATA = [
    {'_index': TEST_INDEX, '_type': 'doc', '_source': i}
    for i in [
        {'ns1': {'attr1': 10}, 'attr2': 4},
        {'ns1': {'attr1': 1}},
        {'ns1': {'attr1': 5}},
        {'ns1': {'attr1': 5}},
        {'ns1': {'attr2': 5.0}},
        {'ns2': {'attr3': False}},
        {'ns2': {'os': 'mac'}},
        {'ns2': {'big_fee': 10.5999}},
        {'ns3': {'test_date': '2019-07-01'}},
        {'ns3': {'test_date': '2019-07-01 11:30:00'}},
        {'ns3': {'test_date': '2019-07-15T11:35:55.713594'}},
        {'ns4': {'attr4': 2.0}},
        {'ns4': {'attr4': 75.5}},
        {'ns4': {'attr4': 85.5}},
        {'ns4': {'attr4': 120.0}},
        {'attr2': 1},  # no namespace
        {'ns1': {'ns2': {'attr1': 1}}}  # nested namespace
    ]
]

TEST_ID = 'test_id'
TEST_DATA[0]['_id'] = TEST_ID  # for testing query by id

TEMPLATE = {
    "index_patterns": ["bamboo-test-index-"],
    "settings": {
        "number_of_shards": 7,
        "number_of_replicas": 2
    },
    "mappings": {
        "doc": {
            "dynamic": True,
            "numeric_detection": False,
            "date_detection": True,
            "dynamic_date_formats": [
                "date_hour_minute_second_fraction||date||yyyy-MM-dd HH:mm:ss",
                "date_optional_time"
            ],
            "dynamic_templates": [
                {
                    "fee_as_currency": {
                        "match_mapping_type": "double",
                        "match": "*_fee*",
                        "mapping": {
                            "type": "scaled_float",
                            "scaling_factor": 100
                        }
                    }
                },
                {
                    "integers": {
                        "match_mapping_type": "long",
                        "mapping": {
                            "type": "integer"
                        }
                    }
                },
                {
                    "floats": {
                        "match_mapping_type": "double",
                        "mapping": {
                            "type": "float"
                        }
                    }
                },
                {
                    "strings": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                },
                {
                    "dates_as_dates": {
                        "match": "*_date",
                        "mapping": {
                            "type": "date",
                        }
                    }
                }
            ]
        }
    }
}


@pytest.fixture(scope="session")
def test_id():
    """Return test id."""
    return TEST_ID


@pytest.fixture
def df(monkeypatch):
    """Init ElasticDataFrame using text index."""
    monkeypatch.setattr(DataFrame, '__repr__', lambda self: 'DataFrame')
    return DataFrame(TEST_INDEX)


@pytest.fixture(scope="session", autouse=True)
def elasticsearch_index():
    """Setup/teardown local test index."""
    template = 'test_template'
    config(hosts=['localhost'])
    es = config.connection
    try:
        es.indices.put_template(name=template, body=TEMPLATE)
        bulk(es, TEST_DATA)
        es.indices.refresh()
        yield es
    finally:
        es.indices.delete(index=TEST_INDEX)
        es.indices.delete_template(name=template)
