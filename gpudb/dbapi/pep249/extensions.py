"""
Optional extensions to the DB-API 2.0, as outlined in PEP 249.

https://www.python.org/dev/peps/pep-0249/#optional-db-api-extensions

"""
from abc import abstractmethod, ABCMeta
from typing import Iterator, Optional, Type, TYPE_CHECKING
from gpudb.dbapi.pep249.types import ResultRow

if TYPE_CHECKING:
    from .connection import BaseConnection


class ConnectionErrorsMixin(metaclass=ABCMeta):
    # pylint: disable=too-few-public-methods,invalid-name,missing-function-docstring
    """
    An optional extension to PEP 249, providing access to mandated
    exception types as members of the Connection class.

    """

    @property
    @abstractmethod
    def Error(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def InterfaceError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def DatabaseError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def DataError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def OperationalError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def IntegrityError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def InternalError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def ProgrammingError(self) -> Type[Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def NotSupportedError(self) -> Type[Exception]:
        raise NotImplementedError


# pylint: disable=too-few-public-methods
class CursorConnectionMixin(metaclass=ABCMeta):
    """
    An optional extension of PEP 249 which attaches a read only
    reference to the Connection object the cursor was created from.

    This could be implemented using `weakref`, to prevent errors if
    the connection is garbage collected before the cursor.

    """

    @property
    def connection(self) -> "BaseConnection":
        """The parent Connection of the implementing cursor."""
        raise NotImplementedError


class IterableCursorMixin:
    """
    An naive implementation of an optional extension to PEP 249 which
    turns the cursor into an iterator/iterable.

    This enables code like:

    ```python
    >>> cursor.execute(SQL_QUERY)
    >>> # An alternative to cursor.fetchone()
    >>> value = next(cursor)
    >>> # Iterate over the values
    >>> for value in cursor:
    ...    do_thing_with(value)
    ```
    """

    def __next__(self) -> Optional[ResultRow]:
        item = self.fetchone()
        if item is None:
            raise StopIteration
        return item

    def __iter__(self) -> Iterator[ResultRow]:
        return self
