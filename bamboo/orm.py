"""
ORM cixin class providing orm-like functionality for an index.

Works with dynamic and static mappings.
"""
from abc import ABCMeta
from collections import defaultdict

from . import field
from .config import es
from .exceptions import FieldConflictError, MissingMappingError


class OrmMixin(object):
    """Provides dot-notation for accessing index field names.

    Attributes:
        fields: Fields that exist within an index at a root level
        namespaces: Namespaces that exist within an index at a root level
        dtypes: Dtypes of all fields (including namespaced)
    """

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        """Init OrmMixin."""
        self._load_mapping()

    @property
    def fields(self):
        """Fields that exist within an index at a root level."""
        return [i._name
                for i in vars(self).values()
                if isinstance(i, field.Field)]

    @property
    def namespaces(self):
        """Namespaces that exist within an index at a root level."""
        return [i._name
                for i in vars(self).values()
                if isinstance(i, field.Namespace)]

    @property
    def dtypes(self):
        """Dtypes of all fields (including namespaced)."""
        return self.__dtypes(self)

    @staticmethod
    def __dtypes(obj):
        """Recursively loads dtypes from nested namespaces."""
        dtypes = defaultdict(dict)
        for k, v in vars(obj).items():
            if k == 'parent':
                continue
            if isinstance(v, field.Field):
                dtypes[v._name] = v.dtype
            elif isinstance(v, field.Namespace):
                dtypes[v._name].update(OrmMixin.__dtypes(v))
        return dict(dtypes)

    def get_namespace(self, key):
        """Return the namespace.

        Equivalent to `ds.<namespace>` in dot notation

        Args:
            key (str): The name of the namespace

        Returns:
            field.Namespace
        """
        return getattr(self, key)

    def _load_mapping(self):
        """Acquire the index mapping from elasticsearch."""
        raw = es.indices.get_mapping(index=self.index,
                                     include_type_name=False)
        _, raw = raw.popitem()
        properties = raw['mappings'].get('properties')
        if properties:
            self.__parse_properties(properties)
            return properties
        raise MissingMappingError(self.index)

    def __parse_properties(self, properties, namespace=None):
        """Recursively create obj attributes from index properties."""
        for name, definition in properties.items():
            if 'type' in definition:
                self.__add_field(name, definition['type'], namespace)
            if 'properties' in definition:
                ns = self.__create_namespace(name, namespace)
                self.__parse_properties(definition['properties'], ns)

    def __create_namespace(self, name, namespace):
        """Create a namespace and adds it as an object attribute."""
        if namespace is not None:
            ns = field.Namespace(name, namespace)
            setattr(namespace, name, ns)
        elif hasattr(self, name):
            raise FieldConflictError(name)
        else:
            ns = field.Namespace(name, self)
            setattr(self, name, ns)
        return ns

    def __add_field(self, name, dtype, namespace):
        """Create a field and adds it as an object attribute."""
        Field = field._type_mapping.get(dtype, field.Dummy)
        if namespace is not None:
            f = Field(name, namespace)
            setattr(namespace, name, f)
        elif hasattr(self, name):
            raise FieldConflictError(name)
        else:
            f = Field(name, self)
            setattr(self, name, f)
