#!/usr/bin/env python3
"""
Example Python program that copies a local directory tree to KIFS and back
with GPUdbFileHandler: directories and glob patterns are uploaded under their
paths, and KIFS directories and wildcard patterns downloaded under theirs.

Run with KINETICA_URL, USERNAME and PASSWORD set for your database.
"""
import os
import tempfile
import uuid

from gpudb import GPUdb, GPUdbException, GPUdbFileHandler

KINETICA_URL = os.getenv("KINETICA_URL", "http://localhost:9191")
USERNAME = os.getenv("USERNAME", "")
PASSWORD = os.getenv("PASSWORD", "")


def write_tree(root):
    """A small tree of files to copy."""
    files = {
        "readme.txt": "Sales data, by region\n",
        "east/2026-09.csv": "region,amount\neast,10\n",
        "east/2026-10.csv": "region,amount\neast,12\n",
        "west/2026-10.csv": "region,amount\nwest,7\n",
    }
    for name, text in files.items():
        path = os.path.join(root, *name.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(text)


def list_tree(root):
    return sorted(os.path.relpath(os.path.join(directory, name), root)
                  for directory, _, names in os.walk(root) for name in names)


def main():
    options = GPUdb.Options()
    options.username = USERNAME
    options.password = PASSWORD
    kdb = GPUdb(host=KINETICA_URL, options=options)
    print(f"Connected to Kinetica at {KINETICA_URL}")

    handler = GPUdbFileHandler(kdb)
    kifs_dir = "example_transfer_" + uuid.uuid4().hex[:8]
    kdb.create_directory(kifs_dir, {})

    try:
        with tempfile.TemporaryDirectory() as local:
            source = os.path.join(local, "sales")
            write_tree(source)

            # A whole directory tree, kept for a day (the TTL is in minutes)
            handler.upload_files(source, kifs_dir, recursive=True, ttl=24 * 60)
            print(f"Uploaded {len(list_tree(source))} files to KIFS directory '{kifs_dir}'")

            # The whole directory back, subdirectories and all
            copy = os.path.join(local, "copy")
            os.mkdir(copy)
            handler.download_dir(kifs_dir, copy)
            print(f"Downloaded the directory: {list_tree(copy)}")

            # Only the October files, wherever they are; * matches across
            # subdirectories in KIFS
            october = os.path.join(local, "october")
            os.mkdir(october)
            handler.download_files(kifs_dir + "/*2026-10.csv", october)
            print(f"Downloaded by pattern: {list_tree(october)}")

            # Files already there are kept when overwriting is off
            handler.download_files(kifs_dir + "/*2026-10.csv", october, overwrite=False)
            print("Downloaded again without overwriting: the local files were kept")

            # A name that matches nothing is reported before anything is
            # transferred
            try:
                handler.download_files([kifs_dir + "/readme.txt", kifs_dir + "/*.parquet"], october)
            except GPUdbException as ex:
                print(f"Refused: {str(ex)[:70]}...")
    finally:
        kdb.delete_directory(kifs_dir, {"recursive": "true", "no_error_if_not_exists": "true"})


if __name__ == "__main__":
    print("Kinetica Python API KIFS Transfer Example")
    print("=" * 50)
    main()
