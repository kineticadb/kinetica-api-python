# Install the GPUdb python API to the system module path.

from distutils.core import setup
import os

# Get a list of files in the subdirectories only.
# File paths are relative the input directory.
def package_files(directory):
    directory = os.path.normpath(directory)
    num_paths = directory.count(os.path.sep)+1
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        if os.path.normpath(path) == directory :
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
    name='gpudb',
    version='5.4.0',
    description='Python client for GPUdb',
    packages=['gpudb'],
    package_data={'gpudb': extra_files},
    url='http://gpudb.com',
)
