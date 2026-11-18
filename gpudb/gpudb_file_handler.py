import bisect
import concurrent.futures
import contextlib
import dataclasses
import fnmatch
import glob
import itertools
import json
import logging
import os
import re
from pathlib import Path
import uuid
from enum import Enum
from typing import Callable, List, Optional, Tuple, Union

import gpudb
from gpudb import GPUdb

FILE_SIZE_THRESHOLD = 62914560  # 60 MB

# The largest file split size, and the defaults of the other handler options
_MAX_FILE_SIZE_TO_SPLIT = 62914560
DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE = 5
DEFAULT_MAX_UPLOAD_BYTES_IN_FLIGHT = 128 * 1024 * 1024
KIFS_PATH_SEPARATOR = "/"
KIFS_PATH_PREFIX = "kifs://"

# KIFS aliases this to the user's home directory
REMOTE_USER_HOME_DIR = "~"

# The wildcards of a KIFS pattern, * and ?, which are all KIFS takes; and of a
# local one, which Python's glob matches, where "[" opens a character class.
# A KIFS name with "[" in it is a name.
_KIFS_WILDCARDS = "*?"
_LOCAL_WILDCARDS = "*?["

_log = logging.getLogger("gpudb.GPUdbFileHandler")


def _has_kifs_wildcard(path: str) -> bool:
    return any(wildcard in path for wildcard in _KIFS_WILDCARDS)


def _has_local_wildcard(path: str) -> bool:
    return any(wildcard in path for wildcard in _LOCAL_WILDCARDS)


def _kifs_pattern_matches(name: str, pattern: str) -> bool:
    """Whether a KIFS name matches a pattern of * and ?, as KIFS matches it:
    * matches any run of characters, "/" included, ? any one character, and
    every other character -- "[" among them -- only itself.
    """
    regex = "".join(".*" if c == "*" else "." if c == "?" else re.escape(c) for c in pattern)
    return re.fullmatch(regex, name, re.DOTALL) is not None


def _message(ex: Exception) -> str:
    return str(ex.message if isinstance(ex, gpudb.GPUdbException) else ex)


def _batches(items: list, limit: int) -> list:
    """Items -- each ending in its size, none larger than `limit` -- in order,
    in batches whose sizes add up to at most `limit`.
    """
    batches, batch, total = [], [], 0
    for item in items:
        size = item[-1]
        if batch and (total + size > limit):
            batches.append(batch)
            batch, total = [], 0
        batch.append(item)
        total += size
    if batch:
        batches.append(batch)
    return batches


def _same_kifs_path(asked: str, reported: str) -> bool:
    """Whether a KIFS path the server reports is the one asked for: the same
    but for "~", which the server reports as "~<user>".
    """
    asked_parts = asked.rstrip(KIFS_PATH_SEPARATOR).split(KIFS_PATH_SEPARATOR)
    reported_parts = reported.rstrip(KIFS_PATH_SEPARATOR).split(KIFS_PATH_SEPARATOR)
    if len(asked_parts) != len(reported_parts):
        return False
    same_root = (asked_parts[0] == reported_parts[0]) or \
                ((asked_parts[0] == REMOTE_USER_HOME_DIR) and reported_parts[0].startswith(REMOTE_USER_HOME_DIR))
    return same_root and (asked_parts[1:] == reported_parts[1:])


class _KifsListing:
    """The files one KIFS listing gave, indexed to find, quickly, the files it
    gave for each name: the file itself, the files under a directory, or the
    files a wildcard pattern (*, ? -- * matching "/" too) matches.  Names are
    taken as the server reports them ("~" as "~<user>").
    """

    def __init__(self, files: list):
        self.sizes = dict(files)
        self.paths = sorted(self.sizes)

    def files_for(self, name: str) -> list:
        name = name.rstrip(KIFS_PATH_SEPARATOR)
        if _has_kifs_wildcard(name):
            matches = [path for path in self.paths if _kifs_pattern_matches(path, name)]
        elif name in self.sizes:
            matches = [name]
        else:
            prefix = name + KIFS_PATH_SEPARATOR
            first = bisect.bisect_left(self.paths, prefix)
            matches = list(itertools.takewhile(lambda path: path.startswith(prefix), self.paths[first:]))
        return [(path, self.sizes[path]) for path in matches]


def _transfer_error(action: str, total: int, failures: dict) -> "gpudb.GPUdbException":
    """The error for a transfer of several files that failed for some: every
    failed file with its error, also kept as the error's `failures` attribute
    (a dict of file name to error message).
    """
    details = "; ".join(f"{name}: {error}" for name, error in failures.items())
    ex = gpudb.GPUdbException(f"Could not {action} {len(failures)} of {total} files: {details}")
    ex.failures = dict(failures)
    return ex


def _check_status(resp: dict, action: str) -> None:
    """Raise the server's error, if the response is one.  An ERROR response
    carries only its status, so its other fields cannot be read.
    """
    status_info = resp["status_info"]
    if status_info["status"] == "ERROR":
        raise gpudb.GPUdbException(f"Could not {action}: {status_info['message']}")


@contextlib.contextmanager
def _open_download_file(local_file_name: str):
    """Open a local file to download into, and delete it again if the download
    fails, rather than leave a partial file behind.
    """
    # Opened outside the try: a file that cannot be opened holds no partial
    # download, and may be the caller's
    f = open(local_file_name, mode="bw")
    try:
        with f:
            yield f
    except BaseException:
        with contextlib.suppress(OSError):
            os.remove(local_file_name)
        raise


class MultipartOperation(Enum):
    """Enum for MultipartOperation stage indicators
    """
    NONE = "none"
    INIT = "init"
    UPLOAD_PART="upload_part"
    COMPLETE="complete"
    CANCEL="cancel"

    
class OpMode(Enum):
    """Enum indicating whether the operation is an upload or download
    """
    UPLOAD = "upload"
    DOWNLOAD = "download"

    
class KifsFileInfo(object):
    """KifsFileInfo - the class for storing/accessing the KIFS
        file name and size, and, from :meth:`GPUdbFileHandler.show_files`,
        who created it, when, and the listing's info.
    """
    def __init__(self, file_name: str, file_size: int, created_by: Optional[str] = None,
                 creation_time: Optional[int] = None, info: Optional[dict] = None):
        self.file_name = file_name
        self.file_size = file_size
        self.created_by = created_by
        self.creation_time = creation_time
        self.info = dict(info or {})
        
    @property
    def file_name(self):
        """The file_name property."""
        return self._file_name
    
    @file_name.setter
    def file_name(self, value):
        self._file_name = value
        
    @property
    def file_size(self):
        """The file_size property."""
        return self._file_size
    
    @file_size.setter
    def file_size(self, value):
        self._file_size = value

    def __repr__(self):
        return (f"KifsFileInfo(file_name={self.file_name!r}, file_size={self.file_size!r}, "
                f"created_by={self.created_by!r}, creation_time={self.creation_time!r})")


@dataclasses.dataclass
class KifsDirectoryInfo:
    """A KIFS directory, as :meth:`GPUdbFileHandler.show_directory` and the
    others report it: its name, who
    created it, when (milliseconds since the epoch), and the permission of the
    user asking.
    """
    kifs_path: str
    created_by: str
    creation_time: int
    permission: str
    info: dict = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class TransferResult:
    """One step of an upload or download, as a progress callback is given
    it: a whole file, or one part of a file transferred in parts, which
    is followed by one for the whole file once it is done.

    Attributes:
        op_mode (OpMode): Upload or download
        file_name (str): The file transferred: the local file of an upload,
            the KIFS file of a download
        destination (str): Where it went: the KIFS path of an upload, the
            local file of a download
        size (int): Bytes in this step: the part's, or the whole file's
        multi_part (bool): Whether the file is transferred in parts
        part_number (int): The part, from 1, for a step that is one part;
            None for a whole file
        total_parts (int): How many parts the file has, for a part
        successful (bool): Whether the step succeeded
        skipped (bool): Whether the download was skipped, the local file
            existing and overwriting being off
        error_message (str): Why it failed, if it did
    """
    op_mode: OpMode
    file_name: str
    destination: str
    size: int
    multi_part: bool
    part_number: Optional[int] = None
    total_parts: Optional[int] = None
    successful: bool = True
    skipped: bool = False
    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# Ingesting files: the options of /insert/records/fromfiles, as IngestOptions
# and TableCreationOptions give them.  Each option is sent in
# the form the server documents.
# ---------------------------------------------------------------------------

class FileType(str, Enum):
    """The format of the files to ingest (the file_type ingest option)."""
    AVRO = "avro"
    DELIMITED_TEXT = "delimited_text"
    GDB = "gdb"
    JSON = "json"
    PARQUET = "parquet"
    SHAPEFILE = "shapefile"


class ErrorHandlingMode(str, Enum):
    """What to do with a record that cannot be ingested (the error_handling
    ingest option); the server's default is ABORT.
    """
    PERMISSIVE = "permissive"
    SKIP = "skip"
    IGNORE_BAD_RECORDS = "ignore_bad_records"
    ABORT = "abort"


class TypeInferenceMode(str, Enum):
    """How the type of a new table is inferred from the files (the
    type_inference_mode ingest option); the server's default is ACCURACY.
    """
    ACCURACY = "accuracy"
    SPEED = "speed"


class PartitionType(str, Enum):
    """The partitioning of a new table (the partition_type table creation
    option).
    """
    RANGE = "RANGE"
    INTERVAL = "INTERVAL"
    LIST = "LIST"
    HASH = "HASH"
    SERIES = "SERIES"


def _enum_value(enum_class, value, name: str) -> str:
    """The server's text for `value`: a member of `enum_class`, or its value as
    text, in any case.
    """
    if isinstance(value, enum_class):
        return value.value
    if isinstance(value, str):
        for member in enum_class:
            if member.value.lower() == value.lower():
                return member.value
    raise gpudb.GPUdbException(
            f"Option '{name}' must be one of {[member.value for member in enum_class]}; given '{value}'")


def _option_text(value) -> str:
    """An option value as the server takes it."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Enum):
        return value.value
    return str(value)


def _joined(value, separator: str, name: str) -> str:
    """A list of names joined as the server takes it; text is taken as is."""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
        return separator.join(value)
    raise gpudb.GPUdbException(f"Option '{name}' must be a list of strings or a string; given '{value}'")


def _json_text(value, name: str) -> str:
    """A dict as the JSON text the server takes; text is taken as is."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return json.dumps(value)
    raise gpudb.GPUdbException(f"Option '{name}' must be a dict or a JSON string; given '{value}'")


class _Options:
    """What :class:`IngestOptions` and :class:`TableCreationOptions` share:
    any other option of the endpoint, given by :meth:`put` or in `options`,
    and their conversion to the endpoint's options.
    """

    def put(self, key: str, value):
        """Sets any option of /insert/records/fromfiles by its name; it takes
        precedence over the attribute of the same name.  Returns this object.
        """
        self.options[key] = value
        return self

    def _to_dict(self, converters: dict) -> dict:
        result = {}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if (field.name == "options") or (value is None):
                continue
            convert = converters.get(field.name, lambda value, name: _option_text(value))
            result[field.name] = convert(value, field.name)
        for key, value in (self.options or {}).items():
            result[key] = _option_text(value)
        return result


@dataclasses.dataclass
class IngestOptions(_Options):
    """Options for ingesting files with :meth:`GPUdbFileHandler.ingest`:
    each is the /insert/records/fromfiles option
    of the same name, and one left None is left to the server.  Any other
    option of that endpoint can be given in `options`, or with :meth:`put`.

    Lists of columns are sent comma-separated, in the order given, and dicts
    as JSON, as the server takes them.
    """
    bad_record_table_name: Optional[str] = None
    bad_record_table_limit: Optional[int] = None
    column_formats: Optional[Union[dict, str]] = None
    columns_to_load: Optional[Union[List[str], str]] = None
    columns_to_skip: Optional[Union[List[str], str]] = None
    default_column_formats: Optional[Union[dict, str]] = None
    error_handling: Optional[Union[ErrorHandlingMode, str]] = None
    file_type: Optional[Union[FileType, str]] = None
    primary_keys: Optional[Union[List[str], str]] = None
    shard_keys: Optional[Union[List[str], str]] = None
    text_comment_string: Optional[str] = None
    text_delimiter: Optional[str] = None
    text_escape_character: Optional[str] = None
    text_has_header: Optional[bool] = None
    text_header_property_delimiter: Optional[str] = None
    text_null_string: Optional[str] = None
    text_quote_character: Optional[str] = None
    truncate_table: Optional[bool] = None
    type_inference_mode: Optional[Union[TypeInferenceMode, str]] = None
    options: dict = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict:
        """The options of /insert/records/fromfiles these set, as the server
        takes them.

        Raises:
            GPUdbException: if an option is invalid, or both columns_to_load
            and columns_to_skip are given
        """
        if (self.columns_to_load is not None) and (self.columns_to_skip is not None):
            raise gpudb.GPUdbException("Options 'columns_to_load' and 'columns_to_skip' are mutually exclusive")
        comma_joined = lambda value, name: _joined(value, ",", name)
        return self._to_dict({
            "column_formats": _json_text,
            "default_column_formats": _json_text,
            "columns_to_load": comma_joined,
            "columns_to_skip": comma_joined,
            "primary_keys": comma_joined,
            "shard_keys": comma_joined,
            "error_handling": lambda value, name: _enum_value(ErrorHandlingMode, value, name),
            "file_type": lambda value, name: _enum_value(FileType, value, name),
            "type_inference_mode": lambda value, name: _enum_value(TypeInferenceMode, value, name),
        })


@dataclasses.dataclass
class TableCreationOptions(_Options):
    """Options for the table :meth:`GPUdbFileHandler.ingest` creates, if it
    does not exist: each is the
    create_table_options entry of /insert/records/fromfiles of the same name,
    and one left None is left to the server.  Any other can be given in
    `options`, or with :meth:`put`.

    foreign_keys are sent semicolon-separated, and partition_keys and
    partition_definitions comma-separated, as the server takes them.
    """
    type_id: Optional[str] = None
    no_error_if_exists: Optional[bool] = None
    is_replicated: Optional[bool] = None
    foreign_keys: Optional[Union[List[str], str]] = None
    foreign_shard_key: Optional[str] = None
    partition_type: Optional[Union[PartitionType, str]] = None
    partition_keys: Optional[Union[List[str], str]] = None
    partition_definitions: Optional[Union[List[str], str]] = None
    is_automatic_partition: Optional[bool] = None
    ttl: Optional[int] = None
    is_result_table: Optional[bool] = None
    options: dict = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict:
        """The create_table_options these set, as the server takes them.

        Raises:
            GPUdbException: if an option is invalid
        """
        return self._to_dict({
            "foreign_keys": lambda value, name: _joined(value, ";", name),
            "partition_keys": lambda value, name: _joined(value, ",", name),
            "partition_definitions": lambda value, name: _joined(value, ",", name),
            "partition_type": lambda value, name: _enum_value(PartitionType, value, name),
        })


@dataclasses.dataclass
class IngestResult:
    """What :meth:`GPUdbFileHandler.ingest` did: the table ingested into,
    and the records inserted, skipped
    and updated.
    """
    table_name: str
    type_id: str
    count_inserted: int
    count_skipped: int
    count_updated: int
    files: List[str]
    info: dict


class GPUdbFileHandler(object):
    """This class exposes convenience methods to upload/download
    files to/from KIFS from local/KIFS directory.

    Methods
    -------
    1. upload_files - Upload a list of files - :meth:`upload_files`
    2. upload_file - Upload a single file - :meth:`upload_file`
    3. download_files - Download a list of files - :meth:`download_files`
    4. download_file - Download a single file - :meth:`download_file`
    5. download_dir - Download a KIFS directory - :meth:`download_dir`
    6. ingest - Upload files and ingest them into a table - :meth:`ingest`
    7. show_files, show_directory, show_directories, show_all_directories -
       Information on KIFS files and directories
    8. create_directory, delete_directory, delete_files, delete_files_in_dir
    9. kifs_file_exists, kifs_directory_exists, kifs_directories_exist

    Files no larger than the split size (:class:`GPUdbFileHandler.Options`) are
    transferred in batches, many files a request, and uploaded several batches
    at once; larger files are transferred in parts, one file at a time.

    Example
    ::
    
        file_handler = GPUdbFileHandler.from_url_info(host = "http://127.0.0.1.2:9191", username="user", password="password")
        file_handler.upload_file(file_name="/home/user/some_file_name", kifs_path="~anonymous")
        file_handler.download_file(file_name="~anonymous/some_file_name", local_dir="/home/user/download")

    
    """
    
    @dataclasses.dataclass
    class Options:
        """How a :class:`GPUdbFileHandler` transfers files.

        Attributes:
            file_size_to_split (int): Files up to this size, in bytes, are
                transferred in batches of up to this size; larger ones, in parts
                of this size.  Default and largest 60 MB.
            full_file_dispatcher_threadpool_size (int): How many batches of
                files are uploaded at once; default 5.  1 uploads them one after
                another.  (Downloads are made one batch at a time.)
            max_upload_bytes_in_flight (int): The most bytes of batches being
                uploaded held in memory at once; default 128 MB.  A batch waits
                for others to finish rather than go over it.
        """
        file_size_to_split: int = dataclasses.field(default_factory=lambda: FILE_SIZE_THRESHOLD)
        full_file_dispatcher_threadpool_size: int = DEFAULT_FULL_FILE_DISPATCHER_THREADPOOL_SIZE
        max_upload_bytes_in_flight: int = DEFAULT_MAX_UPLOAD_BYTES_IN_FLIGHT

        def __post_init__(self):
            def positive_int(value):
                return isinstance(value, int) and (not isinstance(value, bool)) and (value > 0)

            if (not positive_int(self.file_size_to_split)) or (self.file_size_to_split > _MAX_FILE_SIZE_TO_SPLIT):
                raise gpudb.GPUdbException(f"file_size_to_split must be a positive number of bytes up to "
                                           f"{_MAX_FILE_SIZE_TO_SPLIT}; given '{self.file_size_to_split}'")
            if not positive_int(self.full_file_dispatcher_threadpool_size):
                raise gpudb.GPUdbException(f"full_file_dispatcher_threadpool_size must be a positive number; "
                                           f"given '{self.full_file_dispatcher_threadpool_size}'")
            if not positive_int(self.max_upload_bytes_in_flight):
                raise gpudb.GPUdbException(f"max_upload_bytes_in_flight must be a positive number of bytes; "
                                           f"given '{self.max_upload_bytes_in_flight}'")
    # end class Options


    def __init__(self, db: GPUdb, options: Optional["GPUdbFileHandler.Options"] = None) -> None:
        """ Initialize a :class:`GPUdbFileHandler`.

        Args:
            db (GPUdb): A :class:`GPUdb` instance
            options (GPUdbFileHandler.Options): How to transfer files; None
                for the defaults
        """

        self._db = db
        if options is None:
            options = GPUdbFileHandler.Options()
        elif not isinstance(options, GPUdbFileHandler.Options):
            raise gpudb.GPUdbException(f"options must be a GPUdbFileHandler.Options; given '{type(options)}'")
        self._options = dataclasses.replace(options)


    @property
    def options(self) -> "GPUdbFileHandler.Options":
        """How this handler transfers files (a copy)."""
        return dataclasses.replace(self._options)


    @classmethod
    def __from(cls, db: GPUdb, options: Optional["GPUdbFileHandler.Options"] = None):
        """ Create an instance of :class:`GPUdbFileHandler` from a :class:`GPUdb` instance.

        Args:
            db (GPUdb): A :class:`GPUdb` instance
            options (GPUdbFileHandler.Options): How to transfer files

        Returns:
            GPUdbFileHandler: a :class:`GPUdbFileHandler` instance
        """
        return cls(db, options)
    

    @classmethod
    def from_url_info(cls, host: str = "http://127.0.0.1:9191", username: str = None, password: str = None,
                      options: Optional["GPUdbFileHandler.Options"] = None):
        """ Create a :class:`GPUdbFileHandler` instance from a host URL, user name, and password.

        Args:
            host (str, optional): A Kinetica host URL. Defaults to "http://127.0.0.1:9191".
            username (str, optional): Kinetica user name. Defaults to None.
            password (str, optional): Password for the user. Defaults to None.
            options (GPUdbFileHandler.Options, optional): How to transfer files.
                Defaults to None, for the defaults.

        Returns:
            GPUdbFileHandler: a :class:`GPUdbFileHandler` instance
        """
        db_options = GPUdb.Options()
        db_options.username = username
        db_options.password = password

        db = GPUdb(host=host, options=db_options)
        return cls.__from( db, options )

    
    @classmethod
    def from_db_instance(cls, db: GPUdb, options: Optional["GPUdbFileHandler.Options"] = None):
        """ Create a :class:`GPUdbFileHandler` from a :class:`GPUdb` instance.

        Args:
            db (GPUdb): a :class:`GPUdb` instance
            options (GPUdbFileHandler.Options, optional): How to transfer files

        Returns:
            GPUdbFileHandler: a :class:`GPUdbFileHandler` instance
        """
        return cls.__from(db, options)


    @staticmethod
    def __notify(callback: Optional[Callable], result: TransferResult) -> None:
        """Give a progress callback a step; one that raises is logged, so that
        the transfer goes on.
        """
        if callback is None:
            return
        try:
            callback(result)
        except Exception as ex:
            _log.warning(f"The progress callback raised, and was passed over: {ex}")

    
    def __is_multi_part(self, file_name: str, op_mode: OpMode) -> Tuple[bool, int]:
        split = self._options.file_size_to_split
        if op_mode == OpMode.UPLOAD:
            size = self.__get_local_file_size(file_name)
            return size > split, size
        else:
            sf_resp = self._db.show_files([file_name])
            _check_status(sf_resp, f"find KIFS file '{file_name}'")
            size = sf_resp["sizes"][0]
            return size > split, size

    
    def __upload_multi_part(self, file_name: str, kifs_file_name: str, options: Optional[dict] = None,
                            callback: Optional[Callable] = None):
        split = self._options.file_size_to_split
        total_parts = max(1, -(-self.__get_local_file_size(file_name) // split))
        upload_id = uuid.uuid4()
        completed = False

        # Cancel unless the upload completes, whatever stops it -- an ERROR, a
        # request that raises, a failed local read -- or the server holds the
        # upload open until it times out
        try:
            self.__upload_multi_part_init(kifs_file_name, upload_id, options)

            with open(file_name, mode="rb") as f:
                part_number = 0
                # The last part is what remains, and no empty part follows it
                chunk: bytes = f.read(split)
                while chunk:
                    part_number += 1
                    self.__upload_multi_part_part(kifs_file_name, upload_id, part_number, chunk, options)
                    self.__notify(callback, TransferResult(OpMode.UPLOAD, file_name, kifs_file_name, len(chunk),
                                                           True, part_number, total_parts))
                    chunk = f.read(split)

            if part_number == 0:
                raise gpudb.GPUdbException("No data found")

            self.__upload_multi_part_complete(kifs_file_name, upload_id, options)
            completed = True
        finally:
            if not completed:
                self.__upload_multi_part_cancel(kifs_file_name, upload_id)
                

    def __upload_multi_part_init(self, file_name: str, upload_id: uuid.UUID, options: Optional[dict] = None) -> None:
        # Copy; never alias.  These write operation keys into the mapping, and
        # the mutable default they used to carry was shared by every call.
        # Benign while calls are sequential and every key is rewritten, but two
        # concurrent uploads would interleave writes into one dict.
        options = dict(options) if options else {}
        options["multipart_upload_uuid"] = upload_id
        options["multipart_operation"] = MultipartOperation.INIT.value
        resp = self._db.upload_files([file_name], [], options)
        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            raise gpudb.GPUdbException(status_message)

    
    def __upload_multi_part_part(self, file_name: str, id: uuid.uuid4, part_number: int, data: bytes, options: Optional[dict] = None) -> None:
        # Copy; never alias.  These write operation keys into the mapping, and
        # the mutable default they used to carry was shared by every call.
        # Benign while calls are sequential and every key is rewritten, but two
        # concurrent uploads would interleave writes into one dict.
        options = dict(options) if options else {}
        options["multipart_upload_uuid"] = id
        options["multipart_upload_part_number"] = part_number
        options["multipart_operation"] = MultipartOperation.UPLOAD_PART.value

        resp = self._db.upload_files([file_name], [data], options)

        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            raise gpudb.GPUdbException(status_message)


    def __upload_multi_part_complete(self, file_name: str, id: uuid.uuid4, options: Optional[dict] = None) -> None:
        # Copy; never alias.  These write operation keys into the mapping, and
        # the mutable default they used to carry was shared by every call.
        # Benign while calls are sequential and every key is rewritten, but two
        # concurrent uploads would interleave writes into one dict.
        options = dict(options) if options else {}
        options["multipart_upload_uuid"] = id
        options["multipart_operation"] = MultipartOperation.COMPLETE.value
        resp = self._db.upload_files([file_name], [], options)
        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            raise gpudb.GPUdbException(status_message)

    
    def __upload_multi_part_cancel(self, file_name: str, id: uuid.uuid4, options: Optional[dict] = None) -> None:
        # Copy; never alias.  These write operation keys into the mapping, and
        # the mutable default they used to carry was shared by every call.
        # Benign while calls are sequential and every key is rewritten, but two
        # concurrent uploads would interleave writes into one dict.
        options = dict(options) if options else {}
        options["multipart_upload_uuid"] = id
        options["multipart_operation"] = MultipartOperation.CANCEL.value

        # Sent while the failure that stopped the upload propagates; that is
        # the error to report, so this one is only logged
        try:
            resp = self._db.upload_files([file_name], None, options)
            status_info = resp["status_info"]
            error = status_info["message"] if status_info["status"] == "ERROR" else None
        except Exception as e:
            error = str(e)

        if error is not None:
            _log.warning(f"Could not cancel the multipart upload of '{file_name}': {error}")


    def __upload_full(self, file_name: str, kifs_file_name: str, options: Optional[dict] = None) -> None:
        # Sent even when empty: a zero-byte file is a file
        with open(file_name, mode="rb") as f:
            chunk: bytes = f.read(self.__get_local_file_size(file_name))
            resp: dict = self._db.upload_files([kifs_file_name], [chunk], dict(options or {}))
            status = resp["status_info"]["status"]

            if status == "ERROR":
                status_message = resp["status_info"]["message"]
                raise gpudb.GPUdbException(status_message)


    def __upload(self, file_name: str, kifs_file_name: str, ttl: Optional[int],
                 callback: Optional[Callable] = None) -> None:
        """Upload a local file to the given KIFS path, in parts if it is large;
        the TTL goes with every request of it, a multipart upload's included.
        The callback is given
        each part, and then the whole file, whether or not it succeeded.
        """
        options = {} if ttl is None else {"ttl": str(ttl)}
        is_multi_part, size = self.__is_multi_part(file_name, op_mode=OpMode.UPLOAD)

        try:
            if is_multi_part:
                self.__upload_multi_part(file_name, kifs_file_name, options, callback)
            else:
                self.__upload_full(file_name, kifs_file_name, options)
        except Exception as ex:
            self.__notify(callback, TransferResult(OpMode.UPLOAD, file_name, kifs_file_name, size, is_multi_part,
                                                   successful=False, error_message=_message(ex)))
            raise
        self.__notify(callback, TransferResult(OpMode.UPLOAD, file_name, kifs_file_name, size, is_multi_part))


    def __upload_all(self, uploads: list, ttl: Optional[int], callback: Optional[Callable]) -> dict:
        """Upload local files to their KIFS paths, and return the failures, by
        local file.  Files up to the split size go in batches of up to that
        size, a request each, several batches at once, holding at most
        max_upload_bytes_in_flight in memory; larger ones in parts, one file
        at a time.  A batch that fails is sent again file by file, so that the
        failures name the files that fail.  Callbacks are made on this thread.
        """
        options = {} if ttl is None else {"ttl": str(ttl)}
        split = self._options.file_size_to_split
        failures = {}

        def failed(local_file, kifs_file_name, size, multi_part, error):
            failures[local_file] = error
            self.__notify(callback, TransferResult(OpMode.UPLOAD, local_file, kifs_file_name, size, multi_part,
                                                   successful=False, error_message=error))

        whole, large = [], []
        for local_file, kifs_file_name in uploads:
            try:
                size = self.__get_local_file_size(local_file)
            except OSError as ex:
                failed(local_file, kifs_file_name, 0, False, str(ex))
                continue
            (large if size > split else whole).append((local_file, kifs_file_name, size))

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._options.full_file_dispatcher_threadpool_size,
                thread_name_prefix="GPUdbFileHandler-upload") as pool:
            pending = {}            # future -> (batch, bytes)
            in_flight = [0]

            def finish(futures):
                for future in futures:
                    batch, batch_bytes = pending.pop(future)
                    in_flight[0] -= batch_bytes
                    self.__finish_upload_batch(future, batch, options, failed, callback)

            for batch in _batches(whole, split):
                batch_bytes = sum(size for _, _, size in batch)
                while pending and (in_flight[0] + batch_bytes > self._options.max_upload_bytes_in_flight):
                    done, _ = concurrent.futures.wait(list(pending), return_when=concurrent.futures.FIRST_COMPLETED)
                    finish(done)

                sent, data = [], []
                for local_file, kifs_file_name, size in batch:
                    try:
                        with open(local_file, mode="rb") as f:
                            data.append(f.read())
                        sent.append((local_file, kifs_file_name, size))
                    except OSError as ex:
                        failed(local_file, kifs_file_name, size, False, str(ex))
                if not sent:
                    continue

                future = pool.submit(self._db.upload_files, [kifs_file_name for _, kifs_file_name, _ in sent],
                                     data, dict(options))
                pending[future] = (sent, batch_bytes)
                in_flight[0] += batch_bytes

            finish(list(pending))

        for local_file, kifs_file_name, size in large:
            try:
                self.__upload(local_file, kifs_file_name, ttl, callback)
            except Exception as ex:
                failures[local_file] = _message(ex)

        return failures


    def __finish_upload_batch(self, future, batch: list, options: dict, failed: Callable,
                              callback: Optional[Callable]) -> None:
        try:
            resp = future.result()
            error = resp["status_info"]["message"] if resp["status_info"]["status"] == "ERROR" else None
        except Exception as ex:
            error = _message(ex)

        if error is None:
            for local_file, kifs_file_name, size in batch:
                self.__notify(callback, TransferResult(OpMode.UPLOAD, local_file, kifs_file_name, size, False))
            return

        if len(batch) == 1:
            failed(*batch[0], False, error)
            return

        # The server does not say which file failed: each is sent again alone
        _log.debug(f"A batch of {len(batch)} files failed to upload ({error}); sending each alone")
        for local_file, kifs_file_name, size in batch:
            try:
                self.__upload_full(local_file, kifs_file_name, options)
            except Exception as ex:
                failed(local_file, kifs_file_name, size, False, _message(ex))
            else:
                self.__notify(callback, TransferResult(OpMode.UPLOAD, local_file, kifs_file_name, size, False))


    @staticmethod
    def __check_ttl(ttl) -> None:
        if (ttl is not None) and (isinstance(ttl, bool) or (not isinstance(ttl, int)) or (ttl <= 0)):
            raise gpudb.GPUdbException(f"TTL must be a positive number of minutes, or None; given '{ttl}'")

    
    def upload_file(self, file_name: str, kifs_path: str, ttl: Optional[int] = None,
                    callback: Optional[Callable[[TransferResult], None]] = None) -> None:
        """ Upload a single file to a KIFS directory.

        Args:
            file_name (str): Full path to the local file to upload
            kifs_path (str): A KIFS directory to upload (must be existing)
            ttl (int): Minutes the uploaded file is kept for; None, the default,
                to keep it until deleted
            callback (callable): Given a :class:`TransferResult` for each part
                of a file uploaded in parts, then for the file
        """
        self.__check_ttl(ttl)
        if not self.__check_local_file(file_name):
            raise gpudb.GPUdbException(f"${file_name} is not valid, cannot upload ...")

        self.__upload(file_name, kifs_path + KIFS_PATH_SEPARATOR + Path(file_name).name, ttl, callback)

    
    def upload_files(self, file_names, kifs_path: str, recursive: bool = False, ttl: Optional[int] = None,
                     callback: Optional[Callable[[TransferResult], None]] = None) -> None:
        """ Upload local files, directories and glob patterns to a KIFS
        directory.

        Each name may be:

        * a file, uploaded to `kifs_path` under its own name;
        * a directory, whose files are uploaded to `kifs_path` -- with
          `recursive`, those of its subdirectories too, to the same
          subdirectories of `kifs_path`;
        * a glob pattern (e.g. "/data/*.csv", "/data/**/*.csv"), whose files
          are uploaded to `kifs_path` under their paths from the pattern's
          last directory without wildcards ("/data"); with `recursive`, a
          pattern for file names ("/data/*.csv") is matched in every
          subdirectory too.

        Every file is uploaded once, and the rest are uploaded when one fails;
        then every failure is reported.  Files up to the split size
        (:class:`GPUdbFileHandler.Options`) are uploaded in batches, several
        at once; larger ones in parts, one at a time.

        Args:
            file_names (list or str): Local files, directories or glob patterns
            kifs_path (str): Name of an existent KIFS directory
            recursive (bool): Whether directories, and patterns for file names,
                take in their subdirectories; default False
            ttl (int): Minutes the uploaded files are kept for; None, the
                default, to keep them until deleted
            callback (callable): Given a :class:`TransferResult` for each file
                uploaded or failed, and for each part of a file uploaded in
                parts, on the calling thread

        Raises:
            gpudb.GPUdbException: before anything is uploaded, if a name matches
                no file, or two files would be uploaded to the same KIFS path;
                after, if any upload failed, naming every file that failed (and
                giving them as the error's `failures`, a dict of file name to
                error message)
        """
        self.__check_ttl(ttl)
        uploads = self.__resolve_uploads(file_names, kifs_path, recursive)

        failures = self.__upload_all(uploads, ttl, callback)
        if failures:
            raise _transfer_error("upload", len(uploads), failures)


    def __resolve_uploads(self, file_names, kifs_path: str, recursive: bool) -> list:
        """The local files the given files, directories and glob patterns name,
        each once, with the KIFS path each is uploaded to.  Raises a
        GPUdbException naming every name that matches no file, or every set of
        files that would be uploaded to the same KIFS path.
        """
        if isinstance(file_names, str):
            file_names = [file_names]
        if not file_names:
            raise gpudb.GPUdbException("List of files to upload cannot be empty")

        found, not_found = [], []
        for file_name in file_names:
            if (not isinstance(file_name, str)) or (not file_name):
                raise gpudb.GPUdbException("Name of a local file to upload cannot be empty")
            # A pattern, unless a file of that very name exists: "[" opens a
            # character class in a pattern, but a file may be named with one
            if _has_local_wildcard(file_name) and not os.path.isfile(file_name):
                matches = self.__match_local_pattern(file_name, recursive)
            elif os.path.isdir(file_name):
                matches = self.__list_local_dir(file_name, recursive)
            elif self.__check_local_file(file_name):
                matches = [(file_name, Path(file_name).name)]
            else:
                matches = []
            if not matches:
                not_found.append(file_name)
            found.extend(matches)

        if not_found:
            raise gpudb.GPUdbException(f"Files not found: {not_found}")

        # Each local file once, and each KIFS path from one file only
        uploads, by_kifs_path = {}, {}
        for local_file, relative_path in found:
            if os.path.abspath(local_file) in uploads:
                continue
            kifs_file_name = kifs_path + KIFS_PATH_SEPARATOR + relative_path
            uploads[os.path.abspath(local_file)] = (local_file, kifs_file_name)
            by_kifs_path.setdefault(kifs_file_name, []).append(local_file)

        same_path = [files for files in by_kifs_path.values() if len(files) > 1]
        if same_path:
            raise gpudb.GPUdbException(f"Files would be uploaded to the same KIFS path: {same_path}")

        return list(uploads.values())


    @staticmethod
    def __match_local_pattern(pattern: str, recursive: bool) -> list:
        """The files a glob pattern matches, with their paths from its last
        directory without wildcards.  "**" matches any number of
        subdirectories; with `recursive`, a pattern for file names is matched
        in every subdirectory too.
        """
        parts = Path(pattern).parts
        first_wildcard = next(i for i, part in enumerate(parts) if _has_local_wildcard(part))
        base = Path(*parts[:first_wildcard]) if first_wildcard else Path(".")
        rest = parts[first_wildcard:]

        if recursive and (len(rest) == 1):
            matches = [match for match in base.rglob("*") if fnmatch.fnmatchcase(match.name, rest[0])]
        else:
            matches = [Path(match) for match in glob.glob(pattern, recursive=True)]
        return sorted((str(match), match.relative_to(base).as_posix()) for match in matches if match.is_file())


    @staticmethod
    def __list_local_dir(directory: str, recursive: bool) -> list:
        """A directory's files, with their paths from it; with `recursive`,
        those of its subdirectories too.
        """
        base = Path(directory)
        files = base.rglob("*") if recursive else base.glob("*")
        return sorted((str(path), path.relative_to(base).as_posix()) for path in files if path.is_file())

    
    def __download_full(self, file_name: str, file_size: int, local_file_name: str) -> None:
        resp: dict = self._db.download_files([file_name], [], [], {})
        _check_status(resp, f"download KIFS file '{file_name}'")
        
        with _open_download_file(local_file_name) as f:
            written = f.write(resp["file_data"][0])
            if written != file_size:
                raise gpudb.GPUdbException(f"Failed to write file {file_name}")
    
    
    def __download_multi_part(self, file_name: str, file_size: int, local_file_name: str,
                              callback: Optional[Callable] = None) -> None:
        split = self._options.file_size_to_split
        total_parts = max(1, -(-file_size // split))
        offset: int = 0
        part_number: int = 1
        
        # Stops at the first part that fails
        with _open_download_file(local_file_name) as f:
            while offset < file_size:
                part = f"part {part_number} of KIFS file '{file_name}'"
                resp: dict = self._db.download_files([file_name], [offset], [split])
                _check_status(resp, f"download {part}")

                # Each part fills its read, but the last, which holds the rest
                data: bytes = resp["file_data"][0] if resp["file_data"] else b""
                expected = min(split, file_size - offset)
                if len(data) < expected:
                    raise gpudb.GPUdbException(
                        f"Could not download {part}: got {len(data)} bytes; expected {expected}")

                f.write(data)
                self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name, len(data),
                                                       True, part_number, total_parts))
                offset += split
                part_number += 1


    def __skip_existing(self, file_name: str, file_size: int, local_file_name: str, overwrite: bool,
                        callback: Optional[Callable]) -> bool:
        """Whether to keep a local file that exists rather than download over
        it; checked before downloading.
        """
        if overwrite or (not os.path.exists(local_file_name)):
            return False
        _log.warning(f"Not downloading KIFS file '{file_name}': '{local_file_name}' exists, "
                     f"and overwriting is off")
        self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name, file_size,
                                               file_size > self._options.file_size_to_split, skipped=True))
        return True


    def __download(self, file_name: str, file_size: int, local_file_name: str, overwrite: bool,
                   callback: Optional[Callable] = None) -> bool:
        """Download a KIFS file of the given size to the given local file, in
        parts if it is large; a file already there is kept, with a warning,
        unless `overwrite`.  The callback is given each part, then the whole
        file.  Returns whether it was downloaded.
        """
        if self.__skip_existing(file_name, file_size, local_file_name, overwrite, callback):
            return False

        multi_part = file_size > self._options.file_size_to_split
        try:
            if multi_part:
                self.__download_multi_part(file_name, file_size, local_file_name, callback)
            else:
                self.__download_full(file_name, file_size, local_file_name)
        except Exception as ex:
            self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name, file_size,
                                                   multi_part, successful=False, error_message=_message(ex)))
            raise
        self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name, file_size, multi_part))
        return True
    
    
    def download_file(self, file_name: str, local_dir: str, overwrite: bool = True,
                      callback: Optional[Callable[[TransferResult], None]] = None) -> None:
        """ Download a single file to a local directory.
            A large file greater than 60MB in size will be downloaded in parts.

        Args:
            file_name (str): Name of the file to download (full KIFS path)
            local_dir (str): Name of the local directory to save the file in
            overwrite (bool): Whether to replace a local file of the same name;
                default True.  If False, such a file is kept, with a warning,
                and the KIFS file is not downloaded
            callback (callable): Given a :class:`TransferResult` for each part
                of a file downloaded in parts, then for the file

        Raises:
            gpudb.GPUdbException: In case of an exception thrown by the server or in case the 
                                    local directory doesn't exist
        """
        if not self.__check_local_dir(local_dir):
            raise gpudb.GPUdbException("Local directory does not exist; cannot download ...")
        
        _, size = self.__is_multi_part(file_name, op_mode=OpMode.DOWNLOAD)
        local_file_name = local_dir + os.sep + file_name.split(KIFS_PATH_SEPARATOR)[-1]
        self.__download(file_name, size, local_file_name, overwrite, callback)

    
    def download_files(self, file_names, local_dir: str, overwrite: bool = True,
                       callback: Optional[Callable[[TransferResult], None]] = None) -> None:
        """ Download KIFS files, directories and wildcard patterns to a local
        directory.

        Each name may be:

        * a KIFS file, saved in `local_dir` under its own name;
        * a KIFS directory, whose files are saved in `local_dir` under their
          paths in it, subdirectories created as needed;
        * a wildcard pattern (e.g. "mydir/*.csv"; * and ? as KIFS takes them,
          * matching across subdirectories too), whose files are saved under
          their paths from the pattern's last directory without wildcards.

        Every file is downloaded once, and the rest are downloaded when one
        fails; then every failure is reported.  Files up to the split size
        (:class:`GPUdbFileHandler.Options`) are downloaded in batches, one
        batch at a time; larger ones in parts, one at a time.

        Args:
            file_names (list or str): KIFS files, directories or wildcard patterns
            local_dir (str): Name of the local directory to save the files in
            overwrite (bool): Whether to replace local files of the same names;
                default True.  If False, such files are kept, with a warning,
                and the KIFS files not downloaded
            callback (callable): Given a :class:`TransferResult` for each file
                downloaded, failed or skipped, and for each part of a file
                downloaded in parts

        Raises:
            gpudb.GPUdbException: before anything is downloaded, if the local
                directory does not exist, a name matches no KIFS file, or two
                KIFS files would be saved as the same local file; after, if
                any download failed, naming every file that failed (and giving
                them as the error's `failures`, a dict of KIFS file name to
                error message)
        """
        if not self.__check_local_dir(local_dir):
            raise gpudb.GPUdbException("Local directory does not exist; cannot download ...")

        downloads = self.__resolve_downloads(file_names, local_dir)
        self.__download_all(downloads, overwrite, callback)


    def download_dir(self, kifs_dir: str, local_dir: str, overwrite: bool = True,
                     callback: Optional[Callable[[TransferResult], None]] = None) -> None:
        """ Download every file in a KIFS directory to a local directory, under
        their paths in it.  An empty directory downloads nothing.

        Args:
            kifs_dir (str): Name of the KIFS directory
            local_dir (str): Name of the local directory to save the files in
            overwrite (bool): Whether to replace local files of the same names;
                default True
            callback (callable): as :meth:`download_files` takes it

        Raises:
            gpudb.GPUdbException: as :meth:`download_files` does; and if the
                KIFS directory does not exist
        """
        if (not isinstance(kifs_dir, str)) or (not kifs_dir) or _has_kifs_wildcard(kifs_dir):
            raise gpudb.GPUdbException(f"Name of the KIFS directory must be a name without wildcards; given '{kifs_dir}'")
        if not self.__check_local_dir(local_dir):
            raise gpudb.GPUdbException("Local directory does not exist; cannot download ...")

        downloads = self.__resolve_downloads([kifs_dir], local_dir, allow_empty = True)
        self.__download_all(downloads, overwrite, callback)


    def __download_all(self, downloads: list, overwrite: bool, callback: Optional[Callable]) -> None:
        """Download KIFS files to their local files: those up to the split size
        in batches of up to that size, a request each, one batch at a time;
        larger ones in parts, one file at a time.  A batch that fails is
        downloaded again file by file, so that the failures name the files
        that fail.  Then every failure is reported.
        """
        split = self._options.file_size_to_split
        failures = {}

        def failed(file_name, local_file_name, size, multi_part, error):
            failures[file_name] = error
            self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name, size, multi_part,
                                                   successful=False, error_message=error))

        whole, large = [], []
        for file_name, file_size, local_file_name in downloads:
            if not self.__skip_existing(file_name, file_size, local_file_name, overwrite, callback):
                (large if file_size > split else whole).append((file_name, local_file_name, file_size))

        for batch in _batches(whole, split):
            self.__download_batch(batch, failed, callback)

        for file_name, local_file_name, file_size in large:
            try:
                os.makedirs(os.path.dirname(local_file_name), exist_ok=True)
                self.__download(file_name, file_size, local_file_name, True, callback)
            except Exception as ex:
                failures[file_name] = _message(ex)

        if failures:
            raise _transfer_error("download", len(downloads), failures)


    def __download_batch(self, batch: list, failed: Callable, callback: Optional[Callable]) -> None:
        try:
            resp = self._db.download_files([file_name for file_name, _, _ in batch], [], [], {})
            if resp["status_info"]["status"] == "ERROR":
                error = resp["status_info"]["message"]
            elif len(resp["file_data"]) != len(batch):
                error = f"got {len(resp['file_data'])} files; expected {len(batch)}"
            else:
                error = None
        except Exception as ex:
            error = _message(ex)

        if error is not None:
            if len(batch) == 1:
                failed(*batch[0], False, error)
                return
            # The server does not say which file failed: each is fetched alone
            _log.debug(f"A batch of {len(batch)} files failed to download ({error}); fetching each alone")
            for file_name, local_file_name, file_size in batch:
                try:
                    os.makedirs(os.path.dirname(local_file_name), exist_ok=True)
                    self.__download_full(file_name, file_size, local_file_name)
                except Exception as ex:
                    failed(file_name, local_file_name, file_size, False, _message(ex))
                else:
                    self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name,
                                                           file_size, False))
            return

        # In the order asked for
        for (file_name, local_file_name, file_size), data in zip(batch, resp["file_data"]):
            try:
                os.makedirs(os.path.dirname(local_file_name), exist_ok=True)
                with _open_download_file(local_file_name) as f:
                    if f.write(data) != file_size:
                        raise gpudb.GPUdbException(f"Failed to write file {file_name}")
            except Exception as ex:
                failed(file_name, local_file_name, file_size, False, _message(ex))
            else:
                self.__notify(callback, TransferResult(OpMode.DOWNLOAD, file_name, local_file_name,
                                                       file_size, False))


    def __as_reported(self, kifs_path: str) -> str:
        """A KIFS path as the server reports it: "~" as "~<user>"."""
        root, separator, rest = kifs_path.partition(KIFS_PATH_SEPARATOR)
        if root != REMOTE_USER_HOME_DIR:
            return kifs_path
        if getattr(self, "_home_directory", None) is None:
            resp = self._db.show_directories(REMOTE_USER_HOME_DIR, {})
            _check_status(resp, "find the user's KIFS home directory")
            self._home_directory = resp["directories"][0]
        return self._home_directory + separator + rest


    def __resolve_downloads(self, file_names, local_dir: str, allow_empty: bool = False) -> list:
        """The KIFS files the given files, directories and wildcard patterns
        name, each once, with their sizes and the local file each is saved
        as.  Raises a GPUdbException naming every name that matches no file
        (unless `allow_empty`), or every set of files that would be saved as
        one.
        """
        if isinstance(file_names, str):
            file_names = [file_names]
        if not file_names:
            raise gpudb.GPUdbException("List of KIFS files to download cannot be empty")

        for file_name in file_names:
            if (not isinstance(file_name, str)) or (not file_name):
                raise gpudb.GPUdbException("Name of a KIFS file to download cannot be empty")

        # One listing for all the names; where the server refuses it (a name
        # not found), each name is listed alone, so that every one not found
        # is named.  (A listing a name, rather than one for all, made the
        # listing most of the time taken to download many small files.)
        listings = {}
        resp = self._db.show_files(list(file_names), {})
        if resp["status_info"]["status"] == "OK":
            listing = _KifsListing(zip(resp["file_names"], resp["sizes"]))
            for file_name in file_names:
                listings[file_name] = listing.files_for(self.__as_reported(file_name))
        else:
            for file_name in file_names:
                resp = self._db.show_files([file_name], {})
                listings[file_name] = ( resp["status_info"]["message"] if resp["status_info"]["status"] == "ERROR"
                                        else list(zip(resp["file_names"], resp["sizes"])) )

        found, not_found = {}, {}
        for file_name in file_names:
            listing = listings[file_name]
            if isinstance(listing, str):
                not_found[file_name] = listing
                continue
            if (not listing) and (not allow_empty):
                not_found[file_name] = "no file matches"
                continue

            # A file's path from the name given: what follows the name's
            # segments up to its first wildcard (for a file named in full, its
            # own name).  KIFS reports the path in full ("~" as "~user"),
            # segment for segment.
            segments = file_name.rstrip(KIFS_PATH_SEPARATOR).split(KIFS_PATH_SEPARATOR)
            kept = next((i for i, segment in enumerate(segments) if _has_kifs_wildcard(segment)), len(segments))
            for path, size in listing:
                path_segments = path.split(KIFS_PATH_SEPARATOR)
                relative = path_segments[kept:] if len(path_segments) > kept else path_segments[-1:]
                found.setdefault(path, (size, os.path.join(local_dir, *relative)))

        if not_found:
            raise gpudb.GPUdbException(f"KIFS files not found: {not_found}")

        by_local_file = {}
        for path, (size, local_file_name) in found.items():
            by_local_file.setdefault(os.path.abspath(local_file_name), []).append(path)
        same_file = [paths for paths in by_local_file.values() if len(paths) > 1]
        if same_file:
            raise gpudb.GPUdbException(f"KIFS files would be saved as the same local file: {same_file}")

        return [(path, size, local_file_name) for path, (size, local_file_name) in found.items()]

    def ingest(self, file_names, table_name: str,
               ingest_options: Optional[IngestOptions] = None,
               create_table_options: Optional[TableCreationOptions] = None) -> IngestResult:
        """ Upload local files to KIFS and ingest them into a table, creating
        it if it does not exist.

        The files are staged in a new directory under the user's KIFS home
        directory (~/<uuid>), ingested with /insert/records/fromfiles, and the
        staged copies deleted afterwards, whether or not the upload or the
        ingest succeeded.

        Args:
            file_names (list or str): Local files, directories or glob patterns
                to ingest, as :meth:`upload_files` takes them (without
                recursion); each file is ingested once
            table_name (str): The table to ingest into
            ingest_options (IngestOptions): How to read and ingest the files;
                None for the server's defaults
            create_table_options (TableCreationOptions): How to create the
                table, if it does not exist; None for the server's defaults

        Returns:
            IngestResult: the table, and the records inserted, skipped and
            updated

        Raises:
            GPUdbException: if a name matches no file, two files would be
                staged as one (two files of the same name not under one
                directory or pattern), an option is invalid, or the upload or
                ingest fails
        """
        if isinstance(file_names, str):
            file_names = [file_names]
        if not file_names:
            raise gpudb.GPUdbException("List of files to ingest cannot be empty")
        if (not isinstance(table_name, str)) or (not table_name):
            raise gpudb.GPUdbException("Name of the table to ingest into cannot be empty")

        # Staged where upload_files() would upload them, so that two files
        # would be staged as one only if they would be uploaded as one, which
        # is refused.  These pairs are what is uploaded, ingested and deleted:
        # resolving the local files again, as plain files, would stage a file
        # a pattern finds in a subdirectory ("data/*/x.csv") under its base
        # name, where the ingest does not look, and refuse two such files of
        # one name.
        staging_dir = REMOTE_USER_HOME_DIR + KIFS_PATH_SEPARATOR + str(uuid.uuid4())
        uploads = self.__resolve_uploads(file_names, staging_dir, recursive=False)
        staged_files = [staged_file for _, staged_file in uploads]

        # Checked before anything is uploaded
        ingest_options = (ingest_options or IngestOptions()).to_dict()
        create_table_options = (create_table_options or TableCreationOptions()).to_dict()

        try:
            failures = self.__upload_all(uploads, None, None)
            if failures:
                raise _transfer_error("upload", len(uploads), failures)

            resp = self._db.insert_records_from_files(
                    table_name, [KIFS_PATH_PREFIX + staged_file for staged_file in staged_files],
                    {}, create_table_options, ingest_options)
            _check_status(resp, f"ingest into table '{table_name}'")

            return IngestResult(table_name = resp["table_name"],
                                type_id = resp["type_id"],
                                count_inserted = resp["count_inserted"],
                                count_skipped = resp["count_skipped"],
                                count_updated = resp["count_updated"],
                                files = list(resp["files"]),
                                info = dict(resp["info"]))
        finally:
            self.__delete_staged_files(staged_files)


    def __delete_staged_files(self, staged_files: list) -> None:
        """Delete the staged copies of ingested files; those never uploaded are
        passed over.  They are deleted by name: their directory, being under
        the user's home directory, cannot be deleted as a directory, and goes
        with its last file.  Sent while any failure of the upload or ingest
        propagates, so a failure here is only logged.
        """
        try:
            resp = self._db.delete_files(staged_files, {"no_error_if_not_exists": "true"})
            status_info = resp["status_info"]
            error = status_info["message"] if status_info["status"] == "ERROR" else None
        except Exception as e:
            error = str(e)

        if error is not None:
            _log.warning(f"Could not delete the staged ingest files {staged_files}: {error}")


    # -----------------------------------------------------------------------
    # KIFS files and directories.  The home directory may be named "~", which
    # the server reports as "~<user>"; the exists checks take that into
    # account.
    # -----------------------------------------------------------------------

    @staticmethod
    def __names(names, what: str) -> list:
        if isinstance(names, str):
            names = [names]
        if (not names) or any((not isinstance(name, str)) or (not name) for name in names):
            raise gpudb.GPUdbException(f"{what} must be a non-empty name or list of names; given '{names}'")
        return list(names)


    def show_files(self, kifs_paths) -> List[KifsFileInfo]:
        """ Information on KIFS files: those named, and those in the
        directories and wildcard patterns named (* and ?).

        Args:
            kifs_paths (list or str): KIFS files, directories or patterns

        Returns:
            list of KifsFileInfo: each file's name, size, creator and creation
            time (milliseconds since the epoch)

        Raises:
            gpudb.GPUdbException: if the server cannot list them (a name not
                found, say)
        """
        paths = self.__names(kifs_paths, "KIFS paths")
        resp = self._db.show_files(paths, {})
        _check_status(resp, f"show KIFS files {paths}")
        return [KifsFileInfo(name, size, user, creation_time, resp.get("info"))
                for name, size, user, creation_time
                in zip(resp["file_names"], resp["sizes"], resp["users"], resp["creation_times"])]


    def __show_directories(self, dir_name: str) -> List[KifsDirectoryInfo]:
        resp = self._db.show_directories(dir_name, {})
        _check_status(resp, f"show KIFS directory '{dir_name}'" if dir_name else "show KIFS directories")
        return [KifsDirectoryInfo(path, user, creation_time, permission, dict(resp.get("info") or {}))
                for path, user, creation_time, permission
                in zip(resp["directories"], resp["users"], resp["creation_times"], resp["permissions"])]


    def show_directory(self, dir_name: str) -> List[KifsDirectoryInfo]:
        """ Information on a KIFS directory ("~" for the user's home).

        Raises:
            gpudb.GPUdbException: if the server cannot show it (it does not
                exist, say)
        """
        return self.__show_directories(self.__names(dir_name, "KIFS directory name")[0])


    def show_directories(self, dir_names) -> List[KifsDirectoryInfo]:
        """ Information on KIFS directories, in the order named.

        Raises:
            gpudb.GPUdbException: if the server cannot show one
        """
        result = []
        for dir_name in self.__names(dir_names, "KIFS directory names"):
            result.extend(self.__show_directories(dir_name))
        return result


    def show_all_directories(self) -> List[KifsDirectoryInfo]:
        """ Information on every KIFS directory the user can see."""
        return self.__show_directories("")


    def create_directory(self, dir_name: str, no_error_if_exists: bool = True) -> None:
        """ Create a KIFS directory; one that exists is left as it is, unless
        `no_error_if_exists` is False, which makes it an error.
        """
        resp = self._db.create_directory(self.__names(dir_name, "KIFS directory name")[0],
                                         {"no_error_if_exists": _option_text(bool(no_error_if_exists))})
        _check_status(resp, f"create KIFS directory '{dir_name}'")


    def delete_directory(self, dir_name: str, recursive: bool = True, no_error_if_not_exists: bool = True) -> None:
        """ Delete a KIFS directory, and, with `recursive` (the default), the
        files under it; one that does not exist is no error unless
        `no_error_if_not_exists` is False.
        """
        resp = self._db.delete_directory(self.__names(dir_name, "KIFS directory name")[0],
                                         {"recursive": _option_text(bool(recursive)),
                                          "no_error_if_not_exists": _option_text(bool(no_error_if_not_exists))})
        _check_status(resp, f"delete KIFS directory '{dir_name}'")


    def delete_files(self, file_names, no_error_if_not_exists: bool = True) -> None:
        """ Delete KIFS files; ones that do not exist are no error unless
        `no_error_if_not_exists` is False.
        """
        names = self.__names(file_names, "KIFS file names")
        resp = self._db.delete_files(names, {"no_error_if_not_exists": _option_text(bool(no_error_if_not_exists))})
        _check_status(resp, f"delete KIFS files {names}")


    def delete_files_in_dir(self, dir_name: str, no_error_if_not_exists: bool = True) -> None:
        """ Delete every file in a KIFS directory, leaving the directory.  A
        directory that does not exist is no error unless
        `no_error_if_not_exists` is False.
        """
        dir_name = self.__names(dir_name, "KIFS directory name")[0]
        if no_error_if_not_exists and (not self.kifs_directory_exists(dir_name)):
            return

        resp = self._db.show_files([dir_name], {})
        _check_status(resp, f"list KIFS directory '{dir_name}'")
        if resp["file_names"]:
            self.delete_files(resp["file_names"], no_error_if_not_exists)


    def kifs_file_exists(self, file_name: str) -> bool:
        """ Whether a KIFS file exists ("~" for the user's home directory may
        start its path).
        """
        file_name = self.__names(file_name, "KIFS file name")[0]
        if _has_kifs_wildcard(file_name):
            raise gpudb.GPUdbException(f"KIFS file name must not have wildcards; given '{file_name}'")
        resp = self._db.show_files([file_name], {})
        return (resp["status_info"]["status"] == "OK") and \
               any(_same_kifs_path(file_name, name) for name in resp["file_names"])


    def kifs_directory_exists(self, dir_name: str) -> bool:
        """ Whether a KIFS directory exists ("~" for the user's home).  A
        "subdirectory" in a file's path is not a KIFS directory.
        """
        dir_name = self.__names(dir_name, "KIFS directory name")[0]
        if _has_kifs_wildcard(dir_name):
            raise gpudb.GPUdbException(f"KIFS directory name must not have wildcards; given '{dir_name}'")
        resp = self._db.show_directories(dir_name, {})
        return (resp["status_info"]["status"] == "OK") and \
               any(_same_kifs_path(dir_name, name) for name in resp["directories"])


    def kifs_directories_exist(self, dir_names) -> bool:
        """ Whether every one of the given KIFS directories exists."""
        names = self.__names(dir_names, "KIFS directory names")
        resp = self._db.show_directories("", {})
        _check_status(resp, "show KIFS directories")
        home = None
        if any(name.rstrip(KIFS_PATH_SEPARATOR) == REMOTE_USER_HOME_DIR for name in names):
            home = self.kifs_directory_exists(REMOTE_USER_HOME_DIR)
        return all(home if (name.rstrip(KIFS_PATH_SEPARATOR) == REMOTE_USER_HOME_DIR)
                   else any(_same_kifs_path(name, existing) for existing in resp["directories"])
                   for name in names)


    def __check_local_dir(self, local_dir: str) -> bool:
        """ Determine whether the given path is an existing local directory.

        Args:
            local_dir (str): Local directory path, whose existence on the file system will be
                             determined

        Returns:
            bool: Whether or not the given directory exists on the local file system
        """
        return os.path.isdir(local_dir)
    
    
    def __check_local_file(self, file_path: str) -> bool:
        return os.path.isfile(file_path)

    
    def __get_local_file_size(self, file_name: str) -> int:
        return os.stat(file_name).st_size
