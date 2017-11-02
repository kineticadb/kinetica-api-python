# Install the GPUdb python API to the system module path.

from setuptools import setup, find_packages

from distutils.core import setup
import os
import sys


# Get a list of files in the subdirectories only.
# File paths are relative the input directory.
def package_files(directory):
    directory = os.path.normpath(directory)
    num_paths = directory.count(os.path.sep)+1
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        if os.path.normpath(path) == directory :
            continue

        if (sys.version_info.major >= 3) and ("avro_py2" in path):
            continue

        if (sys.version_info.major == 2) and ("avro_py3" in path):
            continue

        for filename in filenames:
            file_basename, file_extension = os.path.splitext(filename)
            if file_extension and (file_extension.lower() != ".pyc"):
                local_path = path.split(os.path.sep, num_paths)[num_paths]
                local_filepath = os.path.join(local_path, filename)
                paths.append(local_filepath)

    return paths


# Package up the files in the subdirectories too.
current_path = os.path.dirname(os.path.abspath(__file__))
extra_files = package_files(current_path+'/gpudb')


setup(
    name = 'gpudb',
    packages = ['gpudb'],
    version = '6.1.0',
    description = 'Python client for GPUdb',
    author = 'Kinetica DB Inc.',
    author_email = 'mmahmud@kinetica.com',
    package_data = {'gpudb': extra_files},
    url = 'http://www.kinetica.com',
    download_url = 'https://github.com/kineticadb/kinetica-api-python/archive/6.1.0.tar.gz'
    # download_url = 'https://github.com/kineticadb/kinetica-api-python'
)
