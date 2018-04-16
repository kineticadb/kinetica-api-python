# Install the GPUdb python API to the system module path.

from setuptools import setup, find_packages, Extension


from distutils.core import setup
import distutils
import distutils.sysconfig
import os
import subprocess
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


# Check whether GCC is available
gcc_proc         = subprocess.Popen(["which", "gcc"], stdout = subprocess.PIPE)
gcc_proc_results = gcc_proc.communicate()
does_gcc_exist   = (gcc_proc_results[0] != "")
if not does_gcc_exist:
    print ("")
    print ("********************************************************")
    print ("WARNING: Could not find gcc; please install it before")
    print ("         attempting to install the GPUdb Python API")
    print ("         module.")
    print ("********************************************************")
    print ("")


# Check whether the Python developer's package is available
# (i.e. if Python.h exists)
python_dev_path          = distutils.sysconfig.get_python_inc()
python_header_filename   = os.path.join( python_dev_path, "Python.h" )
does_python_header_exist = os.path.isfile( python_header_filename )
if not does_python_header_exist:
    print ("")
    print ("********************************************************")
    print ("WARNING: Could not find Python.h; please install the")
    print ("         Python developers' package before attempting" )
    print ("         to install the GPUdb Python API module.")
    print ("********************************************************")
    print ("")



# The c-extension avro module
c_avro_module = Extension( "gpudb.protocol",
                           sources = ["protocol/avro.c",
                                      "protocol/bufferrange.c",
                                      "protocol/common.c",
                                      "protocol/dt.c",
                                      "protocol/protocol.c",
                                      "protocol/record.c",
                                      "protocol/schema.c"] )

setup(
    name = 'gpudb',
    packages = ['gpudb'],
    version = '6.2.0.4',
    description = 'Python client for GPUdb',
    long_description = "The client-side Python API for Kinetica.  Create, store, retrieve, and query data with ease and speed.",
    author = 'Kinetica DB Inc.',
    author_email = 'mmahmud@kinetica.com',
    package_data = {'gpudb': extra_files},
    url = 'http://www.kinetica.com',
    download_url = 'https://github.com/kineticadb/kinetica-api-python/archive/v6.2.0.4.tar.gz',
    install_requires = [ "future" ],
    ext_modules = [ c_avro_module ]
)
