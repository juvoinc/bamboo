"""ElasticDataFrame exceptions."""


class FieldConflictError(Exception):
    """Raise when a root field conflicts with a namespace."""

    def __init__(self, name):
        """Init FieldConflictError.

        Args:
            name (str): The name of the conflicting field
        """
        msg = "Field name `{}` conflicts with a namespace in the mapping"
        msg = msg.format(name)
        super(FieldConflictError, self).__init__(msg)


class MissingMappingError(Exception):
    """Raise when no mapping could be found for an index."""

    def __init__(self, index):
        """Init MissingMappingError.

        Args:
            index (str): The name of the index
        """
        msg = "No mapping found for index `{}`".format(index)
        super(MissingMappingError, self).__init__(msg)


class MissingQueryError(ValueError):
    """Raise when no query has been defined.

    Usually occurs when user is attempting to use an operator like and/or etc
    and no query currently is defined in the Dataframe object
    """

    def __init__(self):
        """Init MissingQueryError."""
        msg = "No query found for operation"
        super(MissingMappingError, self).__init__(msg)


class BadOperatorError(TypeError):
    """Raise when an inappropriate operator is used.

    Mostly to guard against someone using `x is True` instead of
    column-level `x == True`
    """

    def __init__(self):
        """Init BadOperatorError."""
        super(BadOperatorError, self).__init__("Invalid operator")
