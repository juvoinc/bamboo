"""Module for managing elasticsearch / bamboo configuration settings."""
from collections import MutableMapping

from elasticsearch import Elasticsearch


class Config(MutableMapping):
    """Configuration storage class."""

    def __init__(self):
        """Init Config."""
        self.config_files = ('setup.cfg', 'tox.ini', '.bamboo')
        self._config = {}

    def __call__(self, **kwargs):
        self._config.update(kwargs)

    def __iter__(self):
        return self._config

    def __len__(self):
        return len(self._config)

    def __setitem__(self, key, value):
        self._config[key] = value

    def __getitem__(self, key):
        return self._config.get(key)

    def __delitem__(self, key):
        del self._config[key]

    @property
    def es(self):
        """Return elasticsearch connection instance."""
        return Elasticsearch(
            hosts=self['hosts'],
            timeout=self['timeout']
        )


config = Config()
es = config.es
