# Bamboo

Bamboo is a high-level api for ElasticSearch that uses a pandas-style interface for querying and aggregations. It was created to provide data scientists and other developers a means of interacting with ElasticSearch without having to learn its cumbersome query language.

DataFrame attributes are created dynamically from field names at initialization time. This allows the team in charge of updating an index or its mapping to be separate from the team using the index for analytics. This can be a significant boon for indices with frequently changing mapping definitions.

## Features

-  Introspection of fields and types
-  Autocomplete for field names
-  Autocomplete for method-based operations based on field type
-  Common syntactic operators regardless of field type
-  Queries are built dynamically and only executed when data is requested

## Installation

```shell
$ pip install elasticsearch-bamboo
...
```

## Example use

Simple use-case:

```python
>>> from bamboo import DataFrame

# by default we connect to localhost:9200
>>> df = DataFrame(index='my_index')

>>> df = df[df.age > 21]
>>> df.count()
42
>>> df.take(10)
[{...}]
```

## Usage

### Configuration

Connection parameters for ElasticSearch and Bamboo can be set several ways.

#### Helper function

The helper function only needs to be called once. After that, its state will persist throughout your session. It accepts the same parameters as the low-level ElasticSearch api. If no config is declared then Bamboo will use the default ElasticSearch settings, i.e., 'localhost'.

```python
>>> from bamboo import config, DataFrame

>>> config(hosts=['localhost'],
           sniff_on_start=True,
           sniff_on_connection_fail=True,
           sniffer_timeout=60)
>>> df = DataFrame(index='my_index')
```

#### Directly passing to dataframe

If different dataframes needs different credentials then a config object can be passed directly to the dataframe during initialization. Otherwise, it uses the global config defined above.

```python
>>> from bamboo.config import Config

>>> config_a = Config(hosts=['esnode1', 'esnode2'])
>>> config_b = Config(hosts=['esnode3', 'esnode4'])

>>> df_a = DataFrame(index='index_a', config=config_a)
>>> df_b = DataFrame(index='index_b', config=config_b)
```

#### Environmental variables

TBI

#### Config file

TBI

### DataFrame

The DataFrame is a represenation of a single elasticsearch index. Field names are loaded dyamically at initialization time. Several additional methods are provided for instrospecting the cluster.

Nested objects are referred to as namespaces within Bamboo.

```python
>>> from bamboo import DataFrame

>>> DataFrame.list_indices()
['my_index', 'index_a', 'index_b']

>>> df = DataFrame(index='my_index')

# Count of documents in index
>>> df.count()
46155585

# Get the data-types for a dataframe
>>> df.dtypes
{
    'name': 'string',
    'age': 'integer',
    'is_cat': 'boolean',
    'account': {
        'status': 'string',
        'balance': 'decimal',
        'expiration_date': 'date'
    }
}

# List fields
>>> df.fields
['name', 'age', 'is_cat']

# List namespaces
>>> df.namespaces
['account', 'other_namespace']

# List the fields within a namespace
>>> df.account.fields
['status', 'balance', 'expiration_date']
```

#### Field reference

Bamboo supports both dot and string-reference when specifying fields.

```python
>>> df.age
Integer(age)

>>> df['age']
Integer(age)

>>> df.account.status
String(status)

>>> df['account']['status']
String(status)

>>> df.get_namespace('account')['status']
String(status)

>>> status = df.account.status
>>> status.name
'account.status'
>>> status.parent
Namespace(account)
>>> status.dtype
'string'
```

### Querying

Generally, filtering works the same as you would use in Pandas.

Autocomplete works for field names. You can type period then tab (or whatever key mapping your IDE supports) and the available fields will be listed.

```python
>>> df = df[df.age > 21]
>>> df = df[df.is_cat == True]  # us `==`, not `is`
>>> df = df[df.account.status == 'good']
>>> df.take(10)
[{...}]
```

#### Boolean conditions

```python
>>> adult = df.age > 21
>>> cat = df.is_cat == True
>>> good_cat = df.account.status == 'good'

# and is represented by `&` and `+`
>>> df[adult + cat + good_cat]

# `&` is and. `|` is or
>>> df[adult | (cat & good_cat)]

# negation
>>> df[~adult & ~cat]

# conditions support comparing two dataframe objects
>>> df[adult] & df[cat]

# boosting the score of queries
>>> df[adult.boost(2.0) & cat.boost(1.5)]

# or use optional boost helper function
>>> from bamboo import boost
>>> df[boost(adult, 2.0) & boost(cat, 1.5)]
```

#### Filter

The filter parameter from ElasticSearch's bool query is made available as a DataFrame method. This filters out any documents that do not match any of the conditions sent to the filter parameter. However, matches to these conditions do not contribute to the overall score of a document.

```python
# matching a single condition
>>> df = df[df.is_cat==True]
>>> list(df.collect(include_score=True))
[{'_score': 1.0,
  'name': 'Purrrnest Hemingway',
  'is_cat': True}]

# filtering a single condition
>>> df = df.filter(df.is_cat==True)
>>> list(df.collect(include_score=True))
[{'_score': 0.0,
  'name': 'Purrrnest Hemingway',
  'is_cat': True}]

# filtering multiple conditions
>>> df = df.filter(df.is_cat==True, df.age=4)

# combining matching and filters
>>> df = df[df.is_cat==True]
>>> df = df.filter(df.age==4)

# filtering on boolean conditions
>>> adult = df.age > 21
>>> cat = df.is_cat == True
>>> df = df.filter(adult | cat)
```

#### Supported operators

| Operator  | Field Type    | Description              |
| :-------- | :-----------: | -----------------------: |
| ==        | all           | equal                    |
| !=        | all           | not equal                |
| >         | numeric, date | greater than             |
| >=        | numeric, date | greater than or equal to |
| <         | numeric, date | less than                |
| <=        | numeric, date | less than or equal to    |
| &         | all           | boolean and              |
| \|        | all           | boolean or               |
| ~         | all           | boolean negation         |

Method-based operators support autocomplete according to the field type. You can type period then tab (or whatever key mapping your IDE supports) and you will be provided with all the methods supported for a particular field.

One more method that is supported by all data types is `.isin`. You can use this to query that the value for a field falls within some set of values.

`is` as a syntactic operator is not supported. `==` should always be used instead. This includes when checking whether a field matches a boolean or a None.

##### Date operations

```python
>>> df[df.account.expiration_date >= '2019-11-01']

# automatic conversion to age (in days)
>>> df[df.account.activation_date.age < 30]
```

##### String operations

```python
# query where equal to a term
>>> df[df.name == 'cat']

# query where not equal to a term
>>> df[df.name != 'cat']

# query where a term contains a substring
>>> df[df.name.contains('freckles')]

# query where a term ends with a substring
>>> df[df.name.endswith('eckles')]

# query where a term startswith a substring
>>> df[df.name.startswith('freck')]

# query where a term matches a regular expression
>>> df[df.name.regexp(r'.*?\s+the\s+cat')]

# query where a term is a member of a set of values
>>> df[df.name.isin(['bengal', 'tabby', 'maine coon'])
```

#### Scripted queries (using painless)

```python
>>> source = "doc['age'].value < 10"
>>> script_condition = df.query(source)
>>> cat = df[df.is_cat == True]
>>> df[script_condition & cat]
```

### Document retrieval

#### Retrieve a single document

```python
# get a document according to document id
>>> df('document_id')
{...}

# only return certain fields
>>> df('document_id', fields=['name'])
{'name': 'Purrrnest Hemingway'}

# get a random document
>>> df.take(1)
[{...}]
```

#### Retrieve a collection of documents

```python
>>> df = df[df.is_cat == True]

# collect a limited number of documents
>>> df.take(5)
[{...}]  # array of five documents matching condition

# collect all matching documents. returns a generator
>>> df.collect()
<generator object __hits at 0x7fd6418fd0f0>

# collect has several optional parameters
>>> df.collect(fields=[],  # field names to include
               limit=10,  # number of documents to return
               preserve_order=False,  # sort documents by score
               include_score=False,  # include the score as a field
               include_id=False,  # include the document id as a field
               **es_kwargs)  # optional ElasticSearch params
<generator object __hits at 0x7fd6418fd0f0>

# apply limits to the number retrieved
>>> just_ten = df.limit(10)
>>> just_ten.collect()
<generator object __hits at 0x7fd6418fd0f0>

# convert query results to a pandas dataframe
>>> pd_df = df.to_pandas(fields=['age', 'name'])
>>> type(pd_df)
<class 'pandas.core.frame.DataFrame'>

# use ElasticSearch query language directly
>>> df.execute(body={'query': {'match_all': {}}})
<generator object __hits at 0x7fd6418fd0f0>
```

### Statistics and aggregations

#### All field types

```python
cat = df[df.is_cat == True]

# count of documents that satisfy condition
>>> cat.count()
42

# unique value counts for a field
>>> cat.name.value_counts(
        n=10,  # return top n counts
        normalize=False,  # return relative frequencies
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
[('bengal', 10),
 ('tabby', 5),
 ('maine coon', 3)]

# count of distinct values
>>> cat.name.nunique(
        precision=3000,  # trade memory for accuracy
        **es_kwargs)  # optional ElasticSearch params
42
```

#### Numeric and date field types

```python
cat = df[df.is_cat == True]

# average
>>> cat.age.average(**es_kwargs)  # optional ElasticSearch params
10

# max
>>> cat.age.max(**es_kwargs)  # optional ElasticSearch params
22

# min
>>> cat.age.min(**es_kwargs)  # optional ElasticSearch params
1

# percentiles
>>> cat.age.percentiles(
        precision=100,  # trade memory for accuracy
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
[{'key': 1.0, 'value': 0.0},
 {'key': 5.0, 'value': 0.0},
 {'key': 25.0, 'value': 0.003896170080258522},
 {'key': 50.0, 'value': 0.1366778221122553},
 {'key': 75.0, 'value': 2.7940936508189047},
 {'key': 95.0, 'value': 22.297436483702977},
 {'key': 99.0, 'value': 78.48965187864174}]

# summary statistics
>>> cat.age.describe(
        extended=False,  # return additional statistics
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
{'avg': 10,
 'count': 42,
 'max': 22,
 'min': 1,
 'sum': 143}
```

#### Only numeric field types

```python
cat = df[df.is_cat == True]

# sum
>>> cat.age.sum(
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
143

# median absolute deviation
>>> cat.age.median_absolute_deviation(
        precision=100,  # trade memory for accuracy
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
3.056001642080668

# histogram
>>> cat.age.histogram(
        interval=50,  # bin size
        min_doc_count=1,  # minimum number of documents for a bin
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
[('[0.0, 5.0)', 10),
 ('[5.0, 10.0)', 15),
 ('[10.0, 15.0)', 5),
 ('[15.0, 20)', 3)]

# percentile ranks
>>> cat.age.percentile_ranks(
        values=[5, 10, 15],  # desired percentage values
        precision=100,  # trade memory for accuracybin
        missing=None,  # how to treat missing documents
        **es_kwargs)  # optional ElasticSearch params
[{'key': 5.0, 'value': 10},
 {'key': 10.0, 'value': 15},
 {'key': 15.0, 'value': 5}]
```
