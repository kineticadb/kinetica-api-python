"""
An abstract database connection implementation, conformant with PEP 249.

"""
from abc import ABCMeta, abstractmethod
from typing import TypeVar
from gpudb.dbapi.pep249.transactions import TransactionContextMixin, DummyTransactionContextMixin
from gpudb.dbapi.pep249.cursor import CursorType

ConnectionType = TypeVar(
    "ConnectionType", "Connection", "TransactionlessConnection"
)


class BaseConnection:  # pylint: disable=too-few-public-methods
    """A Connection without an associated context."""

    @abstractmethod
    def cursor(self: ConnectionType) -> CursorType:
        """Return a database cursor."""
        raise NotImplementedError


class Connection(TransactionContextMixin, BaseConnection, metaclass=ABCMeta):
    """A PEP 249 compliant Connection protocol."""


class TransactionlessConnection(
    DummyTransactionContextMixin, BaseConnection, metaclass=ABCMeta
):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """
