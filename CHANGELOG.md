# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Filter parameter in bool query

## [0.2.5]

### Added

- MIT License.
- Flushed-out README.md
- Python3 compatibility

### Removed

- Upper limit on elasticsearch version.

## [0.2.4]

### Fixed

- Bug where aggregating a datetime field when there are no matching documents.

## [0.2.3]

### Fixed

- Bug in certain nested conditions.

## [0.2.2]

### Added

- HTML repr in notebook.

### Fixed

- Bug where conditions would leak between bool query types.
- Bug where an inversion in root order of operations could result in different query.

## [0.2.1]

### Fixed

- NameError in Field.isin

## [0.2.0] - 2019-09-16

### Added

- DataFrame as replacement for ElasticDataFrame.
- Boost method for queries.
- Boost helper function to wrap queries.
- Allow returning score in results.
- Allow returning id in results.
- Field.isin method for searching multiple terms.
- Query objects.
- Allow passing distinct config objects to dataframe in addition to setting a single global object used by all dataframes.
- DataFrame.query method for applying custom script queries.

### Changed

- Deprecated ElasticDataFrame.
- Moved query classes from utils.py to query.py
- Changed DataFrame.take from generator to return a list.
- Exposed DataFrame.execute as a public method.

### Removed

- Raw results option from DataFrame.collect.

### Fixed

- Bug where DataFrame.collect with `limit` set and `raw=True` would not return results.
- Bug where deeply nested queries returned incorrect results.
