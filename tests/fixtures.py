TEST_INDEX = 'test-profiles-'

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
