# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""General utility functions."""
import functools
import warnings


def deprecated(func):
    """Decorate function as deprecated.

    It will result in a warning being emitted when the function is used.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("{} is deprecated.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return wrapper


def dict_to_params(d):
    """Convert dictionary to string representation of parameters."""
    return ', ' .join('{}={}'.format(k, v) for k, v in d.items())
