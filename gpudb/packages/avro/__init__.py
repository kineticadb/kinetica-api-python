#! /usr/bin/env python

import sys

if sys.version_info[0] == 2:
    from .avro_py2 import schema
    from .avro_py2 import io
    from .avro_py2 import protocol
    from .avro_py2 import ipc
    from .avro_py2 import datafile
    from .avro_py2 import tool
    #from .avro_py2 import txipc
else:
    from .avro_py3 import schema
    from .avro_py3 import io
    from .avro_py3 import protocol
    from .avro_py3 import ipc
    from .avro_py3 import datafile
    from .avro_py3 import tool
    #from .avro_py3 import txipc
