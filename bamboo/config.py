"""Module for managing elasticsearch / bamboo configuration settings."""
import os
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
        self._config = {}
        self.__search(kwargs)

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

    def __search(self, kwargs):
        self.__from_env()
        self.__from_files()
        self.update(kwargs)

    def __from_env(self):
        for k, v in os.environ.items():
            lower_k = k.lower()
            if lower_k.startswith('bamboo'):
                var_idx = lower_k.index('_') + 1
                # convert to array if spaces in env var string
                v = v.split(' ') if ' ' in v else v
                self[lower_k[var_idx:]] = v

    def __from_files(self):
        for fname in self.config_files:
            pass


config = Config()
