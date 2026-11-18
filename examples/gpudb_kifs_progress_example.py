#!/usr/bin/env python3
"""
Example Python program that uploads many small files to KIFS with
GPUdbFileHandler, following the progress with a callback, and then looks the
files and directories up.

Small files are sent many to a request, several requests at once; how many,
and how much memory they may hold, are GPUdbFileHandler.Options.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os
import tempfile
import uuid

from gpudb import GPUdb, GPUdbFileHandler

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")


class Progress:
    """Counts the files and bytes a transfer has done, from its callback."""

    def __init__(self):
        self.files = self.bytes = self.failed = 0

    def __call__(self, result):
        if result.part_number is not None:
            return                      # a part of a large file; its whole file follows
        if result.successful:
            self.files += 1
            self.bytes += result.size
        else:
            self.failed += 1
            print(f"  {result.file_name} failed: {result.error_message}")


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    # Four upload requests at once, holding at most 64 MB
    handler = GPUdbFileHandler(kdb, GPUdbFileHandler.Options(full_file_dispatcher_threadpool_size=4,
                                                             max_upload_bytes_in_flight=64 * 1024 * 1024))
    kifs_dir = "example_progress_" + uuid.uuid4().hex[:8]
    handler.create_directory(kifs_dir)

    try:
        with tempfile.TemporaryDirectory() as local:
            for i in range(500):
                with open(os.path.join(local, f"reading_{i:03d}.json"), "w") as f:
                    f.write(f'{{"sensor": {i % 7}, "value": {i * 1.5}}}\n')

            progress = Progress()
            handler.upload_files(os.path.join(local, "*.json"), kifs_dir, callback=progress)
            print(f"Uploaded {progress.files} files, {progress.bytes} bytes; {progress.failed} failed")

        print(f"Directory '{kifs_dir}' exists: {handler.kifs_directory_exists(kifs_dir)}")
        print(f"Home directory '~' exists: {handler.kifs_directory_exists('~')}")
        print(f"reading_007.json exists: {handler.kifs_file_exists(kifs_dir + '/reading_007.json')}")

        files = handler.show_files(kifs_dir + "/reading_00?.json")
        print(f"Files matching reading_00?.json: {len(files)}, "
              f"{sum(info.file_size for info in files)} bytes, created by {files[0].created_by}")

        [directory] = handler.show_directory(kifs_dir)
        print(f"Directory {directory.kifs_path}: created by {directory.created_by}, "
              f"permission {directory.permission}")

        handler.delete_files_in_dir(kifs_dir)
        print(f"After deleting its files, reading_007.json exists: "
              f"{handler.kifs_file_exists(kifs_dir + '/reading_007.json')}")
    finally:
        handler.delete_directory(kifs_dir)


if __name__ == "__main__":
    print("Kinetica Python API KIFS Progress Example")
    print("=" * 50)
    main()
