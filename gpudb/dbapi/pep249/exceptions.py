"""
Exceptions outlined in PEP 249.

"""
from gpudb.dbapi.pep249.extensions import ConnectionErrorsMixin


class Error(BaseException):
    """Base error outlined in PEP 249."""


class InterfaceError(Error):
    """
    Interface error outlined in PEP 249.

    Raised for errors with the database interface.

    """


class DatabaseError(Error, RuntimeError):
    """
    Database error outlined in PEP 249.

    Raised for errors with the database.

    """


class DataError(DatabaseError):
    """
    Data error outlined in PEP 249.

    Raised for errors that are due to problems with processed data.

    """


class OperationalError(DatabaseError):
    """
    Operational error outlined in PEP 249.

    Raised for errors in the database's operation.

    """


class IntegrityError(DatabaseError):
    """
    Integrity error outlined in PEP 249.

    Raised when errors occur which affect the relational integrity of
    the database (e.g. constraint violations).

    """


class InternalError(DatabaseError):
    """
    Integrity error outlined in PEP 249.

    Raised when the database encounters an internal error.

    """


class ProgrammingError(DatabaseError):
    """
    Programming error outlined in PEP 249.

    Raised for SQL programming errors.

    """


class NotSupportedError(DatabaseError, NotImplementedError):
    """
    Not supported error outlined in PEP 249.

    Raised when an unsupported operation is attempted.

    """


class ConcreteErrorMixin(ConnectionErrorsMixin):
    """A concrete implementation of the Connection error mixin."""

    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
