import pytest
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from bamboo import ElasticDataFrame
from fixtures import TEST_DATA, TEST_INDEX
from juvo.elasticsearch.templates import PROFILES_TEMPLATE


@pytest.fixture
def df():
    """Init ElasticDataFrame using text index."""
    return ElasticDataFrame(TEST_INDEX)


@pytest.fixture(scope="session", autouse=True)
def elasticsearch_index():
    template = 'test_template'
    es = Elasticsearch()
    ElasticDataFrame._es = es
    try:
        # es.indices.put_template(name=template, body=PROFILES_TEMPLATE)
        bulk(es, TEST_DATA)
        es.indices.refresh()
        yield es
    finally:
        es.indices.delete(index=TEST_INDEX)
        es.indices.delete_template(name=template)
