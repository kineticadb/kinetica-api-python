"""Some useful utility pieces."""
from functools import wraps
from typing import Callable, Optional
from gpudb.dbapi.core.types import ReturnType
from gpudb.dbapi.core.exceptions import CONNECTION_CLOSED, ProgrammingError

__all__ = ["raise_if_closed", "ignore_transaction_error"]


def raise_if_closed(method: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
    """
    Wrap a connection/cursor method and raise a 'connection closed' error if
    the object is closed.

    """

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        """Raise if the connection/cursor is closed."""
        if self._closed:  # pylint: disable=protected-access
            raise CONNECTION_CLOSED
        return method(self, *args, **kwargs)

    return wrapped


def ignore_transaction_error(
    method: Callable[..., ReturnType]
) -> Callable[..., Optional[ReturnType]]:
    """
    Ignore transaction errors, returning `None` instead. Useful for
    `rollback`.

    """

    @wraps(method)
    def wrapped(*args, **kwargs):
        """Ignore transaction errors, returning `None` instead."""
        try:
            return method(*args, **kwargs)
        except (ProgrammingError, RuntimeError) as err:
            if str(err).endswith("no transaction is active"):
                return None
            raise

    return wrapped
