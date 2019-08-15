"""Module for managing elasticsearch / bamboo configuration settings."""
from collections import MutableMapping

from elasticsearch import Elasticsearch


def set_connection(func):
    def wrapper(obj, *args, **kwargs):
        func(obj, *args, **kwargs)
        obj.connection = Elasticsearch(**obj)
    return wrapper


class Config(MutableMapping):
    """Configuration storage class."""

    @set_connection
    def __init__(self, **kwargs):
        """Init Config."""
        self.config_files = ('setup.cfg', 'tox.ini', '.bamboo')
        self.connection = None
        self._config = kwargs

    @set_connection
    def __call__(self, **kwargs):
        self._config.update(kwargs)

    def __iter__(self):
        return iter(self._config)

    def __len__(self):
        return len(self._config)

    @set_connection
    def __setitem__(self, key, value):
        self._config[key] = value

    def __getitem__(self, key):
        return self._config.get(key)

    @set_connection
    def __delitem__(self, key):
        del self._config[key]


config = Config()
