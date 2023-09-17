import os
from pathlib import Path
import uuid
from enum import Enum
from typing import Tuple

import gpudb
from gpudb import GPUdb

FILE_SIZE_THRESHOLD = 62914560  # 60 MB
KIFS_PATH_SEPARATOR = "/"

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
        file name and size.
    """
    def __init__(self, file_name: str, file_size: int):
        self.file_name = file_name
        self.file_size = file_size
        
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

class GPUdbFileHandler(object):
    """This class exposes convenience methods to upload/download
    files to/from KIFS from local/KIFS directory.

    Methods
    -------
    1. upload_files - Upload a list of files - :py:meth:`gpudb.GPUdbFileHandler.upload_files(file_names: list, kifs_path: str)`
    2. upload_file - Upload a single file - :py:meth:`upload_file(file_name: str, kifs_path: str)`
    3. download_files - Download a list of files - :py:meth:`download_files(file_names: list, local_dir: str)`
    4. download_file - Download a single file - :py:meth:`download_file(file_name: str, local_dir: str)`

    Example
    ::
    
        file_handler = GPUdbFileHandler.from_url_info(host = "http://127.0.0.1.2:9191", username="user", password="password")
        file_handler.upload_file(file_name="/home/user/some_file_name", kifs_path="~anonymous")
        file_handler.download_file(file_name="~anonymous/some_file_name", local_dir="/home/user/download")

    
    """
    
    def __init__(self, db: GPUdb) -> None:
        """
        Args:
            db (GPUdb): A GPUdb instance
            
        """

        self._db = db


    @classmethod
    def __from(cls, db: GPUdb):
        """Create an instance of GPUdbFileHandler

        Args:
            db (GPUdb): A GPUdb instance

        Returns:
            GPUdbFileHandler: a GPUdbFileHandler instance
        """
        return cls(db)
    

    @classmethod
    def from_url_info(cls, host: str = "http://127.0.0.1:9191", username: str = None, password: str = None):
        """Method to create a GPUdbFileHandler instance given a host string, user name and password

        Args:
            host (str, optional): A Kinetica host URL. Defaults to "http://127.0.0.1:9191".
            username (str, optional): Kinetica user name. Defaults to None.
            password (str, optional): Password for the user. Defaults to None.

        Returns:
            GPUdbFileHandler: a GPUdbFileHandler instance
        """
        options = GPUdb.Options()
        options.username = username
        options.password = password

        db = GPUdb(host=host, options=options)
        return cls.__from( db )

    
    @classmethod
    def from_db_instance(cls, db: GPUdb):
        """Method to create a GPUdbFileHandler instance given a GPUdb instance

        Args:
            db (GPUdb): a GPUdb instance

        Returns:
            GPUdbFileHandler: a GPUdbFileHandler instance
        """
        return cls.__from(db)

    
    def __is_multi_part(self, file_name: str, op_mode: OpMode) -> Tuple[bool, int]:
        if op_mode == OpMode.UPLOAD:
            size = self.__get_local_file_size(file_name)
            return size > FILE_SIZE_THRESHOLD, size
        else:
            sf_resp = self._db.show_files([file_name])
            size = sf_resp["sizes"][0]
            return size > FILE_SIZE_THRESHOLD, size

    
    def __upload_multi_part(self, file_name: str, kifs_path: str):
        kifs_file_name = kifs_path + KIFS_PATH_SEPARATOR + Path(file_name).name

        upload_id = self.__upload_multi_part_init(kifs_file_name)
        buffer_size = FILE_SIZE_THRESHOLD
        part_number = 1

        with open(file_name, mode="rb") as f:
            chunk: bytes = f.read(buffer_size)
            if chunk:
                self.__upload_multi_part_part(kifs_file_name, upload_id, part_number, chunk)
            else:
                self.__upload_multi_part_cancel(kifs_file_name, upload_id)
                raise gpudb.GPUdbException("No data found")
            while chunk:
                chunk: bytes = f.read(buffer_size)
                part_number += 1
                self.__upload_multi_part_part(kifs_file_name, upload_id, part_number, chunk)
            
            self.__upload_multi_part_complete(kifs_file_name, upload_id)
                

    def __upload_multi_part_init(self, file_name: str, options: dict = {}) -> uuid.uuid4:
        upload_id: uuid.uuid4 = uuid.uuid4()
        options["multipart_upload_uuid"] = upload_id
        options["multipart_operation"] = MultipartOperation.INIT.value
        resp = self._db.upload_files([file_name], [], options)
        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            self.__upload_multi_part_cancel(file_name, upload_id)
            raise gpudb.GPUdbException(status_message)

        return upload_id

    
    def __upload_multi_part_part(self, file_name: str, id: uuid.uuid4, part_number: int, data: bytes, options: dict = {}) -> None:
        options["multipart_upload_uuid"] = id
        options["multipart_upload_part_number"] = part_number
        options["multipart_operation"] = MultipartOperation.UPLOAD_PART.value

        resp = self._db.upload_files([file_name], [data], options)

        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            self.__upload_multi_part_cancel(file_name, id)
            raise gpudb.GPUdbException(status_message)


    def __upload_multi_part_complete(self, file_name: str, id: uuid.uuid4, options: dict = {}) -> None:
        options["multipart_upload_uuid"] = id
        options["multipart_operation"] = MultipartOperation.COMPLETE.value
        resp = self._db.upload_files([file_name], [], options)
        status = resp["status_info"]["status"]

        if status == "ERROR":
            status_message = resp["status_info"]["message"]
            self.__upload_multi_part_cancel(file_name, id)
            raise gpudb.GPUdbException(status_message)

    
    def __upload_multi_part_cancel(self, file_name: str, id: uuid.uuid4, options: dict = {}) -> None:
        options["multipart_upload_uuid"] = id
        options["multipart_operation"] = MultipartOperation.CANCEL.value
        self._db.upload_files([file_name], None, options)


    def __upload_full(self, file_name: str, kifs_path: str) -> None:
        kifs_file_name = kifs_path + KIFS_PATH_SEPARATOR + Path(file_name).name
        
        with open(file_name, mode="rb") as f:
            chunk: bytes = f.read(self.__get_local_file_size(file_name))
            if chunk:
                resp: dict = self._db.upload_files([kifs_file_name], [chunk])
                status = resp["status_info"]["status"]

                if status == "ERROR":
                    status_message = resp["status_info"]["message"]
                    raise gpudb.GPUdbException(status_message)

    
    def upload_file(self, file_name: str, kifs_path: str) -> None:
        """API to upload a single file to a KIFS directory

        Args:
            file_name (str): Full path to the local file to upload
            kifs_path (str): A KIFS directory to upload (must be existing)
        """
        if not self.__check_local_file(file_name):
            raise gpudb.GPUdbException(f"${file_name} is not valid, cannot upload ...")

        is_multi_part, _ = self.__is_multi_part(file_name, op_mode=OpMode.UPLOAD)
        
        if is_multi_part:
            self.__upload_multi_part(file_name, kifs_path)
        else:
            self.__upload_full(file_name, kifs_path)

    
    def upload_files(self, file_names: list, kifs_path: str) -> None:
        """API to upload a list of files to a KIFS directory

        Args:
            file_names (list): List of full local file paths
            kifs_path (str): Name of an existent KIFS directory
        """
        for file in file_names:
            self.upload_file(file, kifs_path)

    
    def __download_full(self, file_name: str, file_size: int, local_dir: str) -> None:
        resp: dict = self._db.download_files([file_name], [], [], {})
        local_file_name = local_dir + os.sep + file_name.split(KIFS_PATH_SEPARATOR)[-1]
        
        with open(local_file_name, mode="bw") as f:
            written = f.write(resp["file_data"][0])
            if written != file_size:
                raise gpudb.GPUdbException(f"Failed to write file ${file_name}")
    
    
    def __download_multi_part(self, file_name: str, file_size: int, local_dir: str) -> None:
        local_file_name: str = local_dir + os.sep + file_name.split(KIFS_PATH_SEPARATOR)[-1]
        offset: int = 0
        
        with open(local_file_name, mode="bw") as f:
            while offset < file_size:
                resp: dict = self._db.download_files([file_name], [offset], [FILE_SIZE_THRESHOLD])
                if resp["file_data"]:
                    f.write(resp["file_data"][0])
                offset += FILE_SIZE_THRESHOLD
            pass
    
    
    def download_file(self, file_name: str, local_dir: str) -> None:
        """API to download a single file to a local directory
            A large file greater than 60MB in size will be downloaded in parts.

        Args:
            file_name (str): Name of the file to download (full KIFS path)
            local_dir (str): Name of the local directory to save the file in

        Raises:
            gpudb.GPUdbException: In case of an exception thrown by the server or in case the 
                                    local directory doesn't exist
        """
        if not self.__check_local_dir(local_dir):
            raise gpudb.GPUdbException("Local directory does not exist; cannot download ...")
        
        is_multi_part, size = self.__is_multi_part(file_name, op_mode=OpMode.DOWNLOAD)
        if is_multi_part:
            self.__download_multi_part(file_name, size, local_dir)
        else:
            self.__download_full(file_name, size, local_dir)

    
    def download_files(self, file_names: list, local_dir: str) -> None:
        """API to download a list of file from KIFS

        Args:
            file_names (list): A list of file names (full KIFS paths)
            local_dir (str): Name of the local directory to save the files in

        Raises:
            gpudb.GPUdbException: _description_
        """
        if not self.__check_local_dir(local_dir):
            raise gpudb.GPUdbException("Local directory does not exist; cannot download ...")

        for file in file_names:
            self.download_file(file, local_dir)

    
    def __check_local_dir(self, local_dir: str) -> bool:
        """used for downloading files

        Args:
            local_dir (str): _description_

        Returns:
            bool: _description_
        """
        return os.path.isdir(local_dir)
    
    
    def __check_local_file(self, file_path: str) -> bool:
        return os.path.isfile(file_path)

    
    def __get_local_file_size(self, file_name: str) -> int:
        return os.stat(file_name).st_size
    
    
    def __get_kifs_file_info(self, file_name: str) -> KifsFileInfo:
        resp = self._db.show_files([file_name])
        
        kifs_file_info = KifsFileInfo()
        kifs_file_info.file_name = resp["file_names"][0]
        kifs_file_info.file_size = resp["sizes"][0]
        
        return kifs_file_info
    