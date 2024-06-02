# Install the GPUdb python API to the system module path.
from setuptools import setup, Extension

setup(
    ext_modules = [
        Extension( name = "gpudb.protocol",
            sources = ["protocol/avro.c",
                        "protocol/bufferrange.c",
                        "protocol/common.c",
                        "protocol/dt.c",
                        "protocol/protocol.c",
                        "protocol/record.c",
                        "protocol/schema.c"],
            extra_link_args=["-Wl,-rpath,$ORIGIN/../../.."] ) ]
)
