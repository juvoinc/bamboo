# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""Bamboo exceptions."""


class FieldConflictError(Exception):
    """Raise when a root field conflicts with a namespace."""

    msg = "Field name `{}` conflicts with a namespace in the mapping"

    def __init__(self, name):
        """Init FieldConflictError.

        Args:
            name (str): The name of the conflicting field
        """
        msg = self.msg.format(name)
        super(FieldConflictError, self).__init__(msg)


class MissingMappingError(Exception):
    """Raise when no mapping could be found for an index."""

    msg = "No mapping found for index `{}`"

    def __init__(self, index):
        """Init MissingMappingError.

        Args:
            index (str): The name of the index
        """
        msg = self.msg.format(index)
        super(MissingMappingError, self).__init__(msg)


class MissingQueryError(ValueError):
    """Raise when no query has been defined.

    Usually occurs when user is attempting to use an operator like and/or etc
    and no query currently is defined in the Dataframe object
    """

    msg = "No query found for operation"

    def __init__(self):
        """Init MissingQueryError."""
        super(MissingMappingError, self).__init__(self.msg)


class BadOperatorError(TypeError):
    """Raise when an inappropriate operator is used.

    Mostly to guard against someone using `x is True` instead of
    column-level `x == True`
    """

    msg = "Invalid operator: {}"

    def __init__(self, obj):
        """Init BadOperatorError.

        Args:
            obj (object): The object that caused this error to raise.
        """
        msg = self.msg.format(type(obj))
        super(BadOperatorError, self).__init__(msg)
