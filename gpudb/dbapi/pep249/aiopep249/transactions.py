"""
Async transaction support (and non-support!) for PEP 249 compliant
async database API implementations. These mixins effectively provide
the 'context' component of an async database connection or cursor.

These abstract mixin classes provide an async context manager.

There are three implementations:
 - A transaction free context, which is intended for use in compliant
   async database cursors.
 - A transaction context, which is intended for use in compliant async
   database connections (and could also be used to implement transaction
   support for async cursors).
 - A dummy transaction context, which is intended for use in compliant
   async database connections for databases which do not support
   transactions.

"""
from abc import ABCMeta, abstractmethod
from types import TracebackType
from typing import Optional, Type, TypeVar

TransactionMixinType = TypeVar(
    "AsyncTransactionMixinType", bound="AsyncTransactionFreeContextMixin"
)


# pylint: disable=too-few-public-methods
class AsyncTransactionFreeContextMixin(metaclass=ABCMeta):
    """
    A PEP 249 compliant 'transaction' protocol which does not implement
    transactions. In the standard, this protocol would be implemented by a
    database Cursor.

    Classes which implement this protocol will need to implement a `close`
    method, and will gain a context manager implementation.

    For a transaction-free context for use with connections, see
    `DummyTransactionContextMixin`.

    """

    @abstractmethod
    async def close(self) -> None:
        """
        Close the connection.

        If there are un-commited changes, this should perform a rollback.

        """
        raise NotImplementedError

    async def __aenter__(self: TransactionMixinType) -> TransactionMixinType:
        """
        Allow the transaction-supporting object to be used as an async context
        manager.
        """
        return self

    async def __aexit__(
        self,
        error_type: Optional[Type[BaseException]] = None,
        error: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool:
        """Close the object upon exiting the context manager."""
        await self.close()
        return not error


class AsyncTransactionContextMixin(AsyncTransactionFreeContextMixin, metaclass=ABCMeta):
    """
    A PEP 249 compliant async transaction protocol which implements
    implicitly-started transactions. In the standard, this protocol would
    be implemented by an async database connection.

    Classes which implement this protocol will need to implement an async
    `close` method, an async `rollback` method, and an async `commit method`
    and they will gain an async context manager implementation which rolls
    back in event of an error.

    """

    @abstractmethod
    async def commit(self) -> None:
        """
        Commit changes made since the start of the pending transaction.

        """
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        """
        Roll back to the start of the pending transaction, discarding
        changes.

        """
        raise NotImplementedError

    async def __aexit__(
        self,
        error_type: Optional[Type[BaseException]] = None,
        error: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> bool:
        """
        Close the object upon exiting the async context manager.

        If no exceptions were raised: try to commit the changes, rolling
        back if there is an issue with the commit, and close the connection.

        If an exception was raised: roll back, close the connection, re-raise
        the exception (by returning True).
        """
        if not error:
            try:
                await self.commit()
            except:
                await self.rollback()
                raise
            finally:
                await self.close()
            return True

        try:
            await self.rollback()
        finally:
            await self.close()
        return False


class AsyncDummyTransactionContextMixin(
    AsyncTransactionContextMixin, metaclass=ABCMeta
):
    """
    A PEP 249 compliant async 'transaction' protocol which does not implement
    transactions, but pretends to. `commit` and `rollback` are both no-op async
    functions. In the standard, this protocol would be implemented by an async
    database connection without transaction support.

    Classes which implement this protocol will need to implement an async `close`
    method, and will gain an async context manager implementation.

    """

    async def commit(self) -> None:
        """
        A dummy implementation of commit, for databases without transaction
        support.

        """
        return

    async def rollback(self) -> None:
        """
        A dummy implementation of rollback, for databases without transaction
        support.

        """
        return
