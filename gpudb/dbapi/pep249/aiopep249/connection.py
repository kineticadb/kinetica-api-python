"""
An abstract asynchronous database connection implementation, conformant
with PEP 249.

"""

from abc import ABCMeta, abstractmethod
from typing import TypeVar

from gpudb.dbapi.pep249.aiopep249.transactions import (
    AsyncTransactionContextMixin,
    AsyncDummyTransactionContextMixin,
)
from .cursor import AsyncCursorType  # pylint: disable=unused-import


AsyncConnectionType = TypeVar(
    "AsyncConnectionType",
    "AsyncConnection",
    "TransactionlessAsyncConnection",
)


# pylint: disable=too-few-public-methods
class BaseAsyncConnection(metaclass=ABCMeta):
    """An Async Connection without an associated context."""

    @abstractmethod
    async def cursor(self) -> AsyncCursorType:
        """Return an asynchronous database cursor."""
        raise NotImplementedError


class AsyncConnection(
    AsyncTransactionContextMixin, BaseAsyncConnection, metaclass=ABCMeta
):
    """A PEP 249 compliant Connection protocol."""


class TransactionlessAsyncConnection(
    AsyncDummyTransactionContextMixin, BaseAsyncConnection, metaclass=ABCMeta
):
    """
    A PEP 249 compliant Connection protocol for databases without
    transaction support.

    """
