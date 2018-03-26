#!/usr/bin/env python

import argparse
import decimal
import gpudb
import itertools
import os
import struct
import socket
import sys
import tempfile

if sys.version_info < (3,):
    def _decode_char(b):
        return b[::-1].rstrip(b"\x00").decode("utf-8", errors="replace")

    def _decode_string(b):
        return b.decode("utf-8", errors="replace")

    def _encode_char(s, size):
        return s.encode("utf-8", errors="replace").ljust(size, b"\x00")[size - 1::-1]

    def _encode_string(s):
        return s.encode("utf-8", errors="replace")
else:
    def _decode_char(b):
        return b[::-1].rstrip(b"\x00").decode(errors="replace")

    def _decode_string(b):
        return b.decode(errors="replace")

    def _encode_char(s, size):
        return s.encode(errors="replace").ljust(size, b"\x00")[size - 1::-1]

    def _encode_string(s):
        return s.encode(errors="replace")


def _decode_date(value):
    return datetime.date(1900 + (value >> 21), (value >> 17) & 0b1111, (value >> 12) & 0b11111)


def _decode_datetime(value):
    return datetime.datetime(1900 + (value >> 53), (value >> 49) & 0b1111, (value >> 44) & 0b11111,
                             (value >> 39) & 0b11111, (value >> 33) & 0b111111, (value >> 27) & 0b111111, ((value >> 17) & 0b1111111111) * 1000)


def _decode_time(value):
    return datetime.time(value >> 26, (value >> 20) & 0b111111, (value >> 14) & 0b111111, ((value >> 4) & 0b1111111111) * 1000)


def _encode_date(value):
    return ((value.year - 1900) << 21) | (value.month << 17) | (value.day << 12)


def _encode_datetime(value):
    return ((value.year - 1900) << 53) | (value.month << 49) | (value.day << 44) \
           | (value.hour << 39) | (value.minute << 33) | (value.second << 27) | ((value.microsecond // 1000) << 17)


def _encode_time(value):
    return (value.hour << 26) | (value.minute << 20) | (value.second << 14) | ((value.microsecond // 1000) << 4)


_char1_struct = struct.Struct("c")
_char2_struct = struct.Struct("2s")
_char4_struct = struct.Struct("4s")
_char8_struct = struct.Struct("8s")
_char16_struct = struct.Struct("16s")
_char32_struct = struct.Struct("32s")
_char64_struct = struct.Struct("64s")
_char128_struct = struct.Struct("128s")
_char256_struct = struct.Struct("256s")
_double_struct = struct.Struct("=d")
_float_struct = struct.Struct("=f")
_int8_struct = struct.Struct("=b")
_int16_struct = struct.Struct("=h")
_int32_struct = struct.Struct("=i")
_int64_struct = struct.Struct("=q")
_uint32_struct = struct.Struct("=I")
_uint64_struct = struct.Struct("=Q")


# File functions

def read_dict(f):
    result = {}
    length = read_uint64(f)

    while length > 0:
        key = read_string(f)
        result[key] = read_string(f)
        length = length - 1

    return result


def read_string(f):
    value_len = read_uint64(f)
    value = f.read(value_len)

    if len(value) < value_len:
        raise RuntimeError("EOF reached")

    return _decode_string(value)


def read_uint64(f):
    value = f.read(8)

    if len(value) < 8:
        raise RuntimeError("EOF reached")

    return _uint64_struct.unpack(value)[0]


def write_dict(f, value):
    write_uint64(f, len(value))

    for k, v in value.items():
        write_string(f, k)
        write_string(f, v)


def write_string(f, value):
    value = _encode_string(value)
    write_uint64(f, len(value))
    f.write(value)


def write_uint64(f, value):
    f.write(_uint64_struct.pack(value))


# Table functions

class ColumnType(object):
    BYTES     = 0x0000002
    CHAR1     = 0x0080000
    CHAR2     = 0x0100000
    CHAR4     = 0x0001000
    CHAR8     = 0x0002000
    CHAR16    = 0x0004000
    CHAR32    = 0x0200000
    CHAR64    = 0x0400000
    CHAR128   = 0x0800000
    CHAR256   = 0x1000000
    DATE      = 0x2000000
    DATETIME  = 0x0000200
    DECIMAL   = 0x8000000
    DOUBLE    = 0x0000010
    FLOAT     = 0x0000020
    INT       = 0x0000040
    INT8      = 0x0020000
    INT16     = 0x0040000
    IPV4      = 0x0008000
    LONG      = 0x0000080
    STRING    = 0x0000001
    TIME      = 0x4000000
    TIMESTAMP = 0x0010000


def get_column_dt(column):
    if column.column_type == gpudb.GPUdbRecordColumn._ColumnType.BYTES:
        return ColumnType.BYTES
    elif column.column_type == gpudb.GPUdbRecordColumn._ColumnType.DOUBLE:
        return ColumnType.DOUBLE
    elif column.column_type == gpudb.GPUdbRecordColumn._ColumnType.FLOAT:
        return ColumnType.FLOAT
    elif column.column_type == gpudb.GPUdbRecordColumn._ColumnType.INT:
        if gpudb.GPUdbColumnProperty.INT8 in column.column_properties:
            return ColumnType.INT8
        elif gpudb.GPUdbColumnProperty.INT16 in column.column_properties:
            return ColumnType.INT16
        else:
            return ColumnType.INT
    elif column.column_type == gpudb.GPUdbRecordColumn._ColumnType.LONG:
        if gpudb.GPUdbColumnProperty.TIMESTAMP in column.column_properties:
            return ColumnType.TIMESTAMP
        else:
            return ColumnType.LONG
    else:
        if gpudb.GPUdbColumnProperty.CHAR1 in column.column_properties:
            return ColumnType.CHAR1
        elif gpudb.GPUdbColumnProperty.CHAR2 in column.column_properties:
            return ColumnType.CHAR2
        elif gpudb.GPUdbColumnProperty.CHAR4 in column.column_properties:
            return ColumnType.CHAR4
        elif gpudb.GPUdbColumnProperty.CHAR8 in column.column_properties:
            return ColumnType.CHAR8
        elif gpudb.GPUdbColumnProperty.CHAR16 in column.column_properties:
            return ColumnType.CHAR16
        elif gpudb.GPUdbColumnProperty.CHAR32 in column.column_properties:
            return ColumnType.CHAR32
        elif gpudb.GPUdbColumnProperty.CHAR64 in column.column_properties:
            return ColumnType.CHAR64
        elif gpudb.GPUdbColumnProperty.CHAR128 in column.column_properties:
            return ColumnType.CHAR128
        elif gpudb.GPUdbColumnProperty.CHAR256 in column.column_properties:
            return ColumnType.CHAR256
        elif gpudb.GPUdbColumnProperty.DATE in column.column_properties:
            return  ColumnType.DATE
        elif gpudb.GPUdbColumnProperty.DATETIME in column.column_properties:
            return ColumnType.DATETIME
        elif gpudb.GPUdbColumnProperty.DECIMAL in column.column_properties:
            return ColumnType.DECIMAL
        elif gpudb.GPUdbColumnProperty.IPV4 in column.column_properties:
            return ColumnType.IPV4
        elif gpudb.GPUdbColumnProperty.TIME in column.column_properties:
            return ColumnType.TIME
        else:
            return ColumnType.STRING


def get_dt_size(dt):
    return {
        ColumnType.BYTES:     8,
        ColumnType.CHAR1:     1,
        ColumnType.CHAR2:     2,
        ColumnType.CHAR4:     4,
        ColumnType.CHAR8:     8,
        ColumnType.CHAR16:   16,
        ColumnType.CHAR32:   32,
        ColumnType.CHAR64:   64,
        ColumnType.CHAR128: 128,
        ColumnType.CHAR256: 256,
        ColumnType.DATE:      4,
        ColumnType.DATETIME:  8,
        ColumnType.DECIMAL:   8,
        ColumnType.DOUBLE:    8,
        ColumnType.FLOAT:     4,
        ColumnType.INT:       4,
        ColumnType.INT8:      1,
        ColumnType.INT16:     2,
        ColumnType.IPV4:      4,
        ColumnType.LONG:      8,
        ColumnType.STRING:    8,
        ColumnType.TIME:      4,
        ColumnType.TIMESTAMP: 8
    }[dt]


def read_column(f, column):
    result = {}
    result["name"] = column.name

    if read_string(f) != column.name:
        return None

    dt = get_column_dt(column)
    result["dt"] = dt

    if read_uint64(f) != dt:
        return None

    result["data"] = open(read_string(f), "rb")
    filename = read_string(f)

    if gpudb.GPUdbColumnProperty.NULLABLE in column.column_properties:
        if not filename:
            return None
        else:
            result["null_data"] = open(filename, "rb")
    elif filename:
        return None
    else:
        result["null_data"] = None

    filename = read_string(f)

    if dt == ColumnType.BYTES or dt == ColumnType.STRING:
        if not filename:
            return None
        else:
            result["var_size"] = os.path.getsize(filename)
            result["var_data"] = open(filename, "rb")
            result["var_pos"] = -1
    elif filename:
        return None
    else:
        result["var_data"] = None

    result["size"] = get_dt_size(dt)

    if not result["var_data"]:
        result["decode_data"] = {
            ColumnType.CHAR1: lambda value: _decode_char(_char1_struct.unpack(value)[0]),
            ColumnType.CHAR2: lambda value: _decode_char(_char2_struct.unpack(value)[0]),
            ColumnType.CHAR4: lambda value: _decode_char(_char4_struct.unpack(value)[0]),
            ColumnType.CHAR8: lambda value: _decode_char(_char8_struct.unpack(value)[0]),
            ColumnType.CHAR16: lambda value: _decode_char(_char16_struct.unpack(value)[0]),
            ColumnType.CHAR32: lambda value: _decode_char(_char32_struct.unpack(value)[0]),
            ColumnType.CHAR64: lambda value: _decode_char(_char64_struct.unpack(value)[0]),
            ColumnType.CHAR128: lambda value: _decode_char(_char128_struct.unpack(value)[0]),
            ColumnType.CHAR256: lambda value: _decode_char(_char256_struct.unpack(value)[0]),
            ColumnType.DATE: lambda value: _decode_date(_int32_struct.unpack(value)[0]).strftime("%Y-%m-%d"),
            ColumnType.DATETIME: lambda value: _decode_datetime(_int64_struct.unpack(value)[0]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            ColumnType.DECIMAL: lambda value: decimal.Decimal(_int64_struct.unpack(value)[0]).scaleb(-4),
            ColumnType.DOUBLE: lambda value: _double_struct.unpack(value)[0],
            ColumnType.FLOAT: lambda value: _float_struct.unpack(value)[0],
            ColumnType.INT: lambda value: _int32_struct.unpack(value)[0],
            ColumnType.INT8: lambda value: _int8_struct.unpack(value)[0],
            ColumnType.INT16: lambda value: _int16_struct.unpack(value)[0],
            ColumnType.IPV4: lambda value: socket.inet_ntoa(_int32_struct.unpack(value)[0]),
            ColumnType.LONG: lambda value: _int64_struct.unpack(value)[0],
            ColumnType.TIME: lambda value: _decode_time(_int32_struct.unpack(value)[0]).strftime("%H:%M:%S.%f")[:-3],
            ColumnType.TIMESTAMP: lambda value: _int64_struct.unpack(value)[0]
        }[dt]

    return result


def read_table(f, db):
    table = read_string(f)

    res = db.show_table(table_name=table, options={"no_error_if_not_exists": "true"})

    if res["status_info"]["status"] != "OK":
        raise RuntimeError(res["status_info"]["message"])

    if not res["table_name"]:
        raise RuntimeError("Table " + table + " does not exist")

    type = gpudb.GPUdbRecordType(schema_string=res["type_schemas"][0], column_properties=res["properties"][0])
    columns = []

    if read_uint64(f) != len(type.columns):
        raise RuntimeError("Table " + table + " type mismatch")

    for type_column in type.columns:
        column = read_column(f, type_column)

        if column is None:
            raise RuntimeError("Table " + table + " type mismatch")

        columns.append(column)

    records = []
    record_count = 0

    while True:
        record = []

        for column in columns:
            dt = column["dt"]
            data = column["data"]
            null_data = column["null_data"]
            var_data = column["var_data"]

            if var_data:
                var_pos = column["var_pos"]

                if var_pos == -1:
                    var_pos = data.read(8)

                    if len(var_pos) < 8:
                        break

                    var_pos = _uint64_struct.unpack(var_pos)[0]

                next_var_pos = data.read(8)

                if len(next_var_pos) < 8:
                    next_var_pos = column["var_size"]
                else:
                    next_var_pos = _uint64_struct.unpack(next_var_pos)[0]

                column["var_pos"] = next_var_pos

                if null_data:
                    null_value = null_data.read(1)

                    if len(null_value) < 1:
                        break

                    if null_value == b"\x01":
                        record.append(None)
                        continue

                if next_var_pos < var_pos:
                    break

                if var_pos == next_var_pos:
                    value = b""
                else:
                    value_len = next_var_pos - var_pos
                    value = var_data.read(value_len)

                    if len(value) < value_len:
                        break

                if dt == ColumnType.STRING:
                    value = _decode_string(value[:-1])

                record.append(value)
            else:
                size = column["size"]
                value = data.read(size)

                if len(value) < size:
                    break

                if null_data:
                    null_value = null_data.read(1)

                    if len(null_value) < 1:
                        break

                    if null_value == b"\x01":
                        record.append(None)
                        continue

                record.append(column["decode_data"](value))

        if len(record) < len(columns):
            break

        records.append(gpudb.GPUdbRecord(type, record).binary_data)

        if len(records) == 10000:
            if not args.dryrun:
                res = db.insert_records(table_name=table, data=records, list_encoding="binary", options={})

                if res["status_info"]["status"] != "OK":
                    raise RuntimeError(res["status_info"]["message"])

            record_count = record_count + len(records)
            records = []

    if len(records) > 0:
        if not args.dryrun:
            res = db.insert_records(table_name=table, data=records, list_encoding="binary", options={})

            if res["status_info"]["status"] != "OK":
                raise RuntimeError(res["status_info"]["message"])

        record_count = record_count + len(records);

    print(table + ": " + str(record_count) + " records")


def write_column(f, column):
    result = {}
    result["name"] = column.name
    dt = get_column_dt(column)
    result["dt"] = dt
    result["data"] = tempfile.NamedTemporaryFile(prefix="kinetica-udf-sim-", dir=args.path, delete=False)

    if gpudb.GPUdbColumnProperty.NULLABLE in column.column_properties:
        result["null_data"] = tempfile.NamedTemporaryFile(prefix="kinetica-udf-sim-", dir=args.path, delete=False)
    else:
        result["null_data"] = None

    if dt == ColumnType.BYTES or dt == ColumnType.STRING:
        result["var_data"] = tempfile.NamedTemporaryFile(prefix="kinetica-udf-sim-", dir=args.path, delete=False)
    else:
        result["var_data"] = None

    result["size"] = get_dt_size(dt)

    if not result["var_data"]:
        result["encode_data"] = {
            ColumnType.CHAR1: lambda value: _char1_struct.pack(_encode_char(value, 1)),
            ColumnType.CHAR2: lambda value: _char2_struct.pack(_encode_char(value, 2)),
            ColumnType.CHAR4: lambda value: _char4_struct.pack(_encode_char(value, 4)),
            ColumnType.CHAR8: lambda value: _char8_struct.pack(_encode_char(value, 8)),
            ColumnType.CHAR16: lambda value: _char16_struct.pack(_encode_char(value, 16)),
            ColumnType.CHAR32: lambda value: _char32_struct.pack(_encode_char(value, 32)),
            ColumnType.CHAR64: lambda value: _char64_struct.pack(_encode_char(value, 64)),
            ColumnType.CHAR128: lambda value: _char128_struct.pack(_encode_char(value, 128)),
            ColumnType.CHAR256: lambda value: _char256_struct.pack(_encode_char(value, 256)),
            ColumnType.DATE: lambda value: _int32_struct.pack(_encode_date(datetime.datetime.strptime(value, "%Y-%m-%d"))),
            ColumnType.DATETIME: lambda value: _int64_struct.pack(_encode_datetime(datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f"))),
            ColumnType.DECIMAL: lambda value: _int64_struct.pack(decimal.Decimal(value) * 10000),
            ColumnType.DOUBLE: lambda value: _double_struct.pack(value),
            ColumnType.FLOAT: lambda value: _float_struct.pack(value),
            ColumnType.INT: lambda value: _int32_struct.pack(value),
            ColumnType.INT8: lambda value: _int8_struct.pack(value),
            ColumnType.INT16: lambda value: _int16_struct.pack(value),
            ColumnType.IPV4: lambda value: _int32_struct.pack(socket.inet_aton(value)),
            ColumnType.LONG: lambda value: _int64_struct.pack(value),
            ColumnType.TIME: lambda value: _int32_struct.pack(_encode_time(datetime.datetime.strptime(value, "%H:%M:%S.%f"))),
            ColumnType.TIMESTAMP: lambda value: _int64_struct.pack(value)
        }[dt]

    write_string(f, result["name"])
    write_uint64(f, result["dt"])
    write_string(f, result["data"].name)
    write_string(f, result["null_data"].name if result["null_data"] else "")
    write_string(f, result["var_data"].name if result["var_data"] else "")
    return result


def write_column_data(column, data):
    name = column["name"]
    dt = column["dt"]
    data_file = column["data"]
    var_file = column["var_data"]
    null_file = column["null_data"]
    size = column["size"]

    if not var_file:
        encode_data = column["encode_data"]

    for record in data:
        value = record[name]

        if value is None:
            null_file.write(b"\x01")

            if var_file:
                write_uint64(data_file, var_file.tell())
            else:
                data_file.write(b"\x00" * size)
        else:
            if null_file:
                null_file.write(b"\x00")

            if var_file:
                write_uint64(data_file, var_file.tell())

                if dt == ColumnType.BYTES:
                    var_file.write(value)
                else:
                    var_file.write(_encode_string(value))
                    var_file.write(b"\x00")
            else:
                data_file.write(encode_data(value))


def write_table(f, db, table, write_data):
    res = db.show_table(table_name=table[0], options={"no_error_if_not_exists": "true"})

    if res["status_info"]["status"] != "OK":
        raise RuntimeError(res["status_info"]["message"])

    if not res["table_names"]:
        raise RuntimeError("Table " + table[0] + " does not exist")

    type = gpudb.GPUdbRecordType(schema_string=res["type_schemas"][0], column_properties=res["properties"][0])

    write_string(f, table[0])
    columns = []

    if len(table) > 1:
        write_uint64(f, len(table) - 1)

        for column in table[1:]:
            for type_column in type.columns:
                if type_column.name == column:
                    columns.append(write_column(f, type_column))
                    break
            else:
                raise RuntimeError("Table " + table[0] + " column " + column + " does not exist")
    else:
        write_uint64(f, len(type.columns))

        for type_column in type.columns:
            columns.append(write_column(f, type_column))

    if write_data:
        i = 0

        while True:
            res = db.get_records(table_name=table[0], offset=i, limit=10000)

            if res["status_info"]["status"] != "OK":
                raise RuntimeError(res["status_info"]["message"])

            if len(res["records_binary"]) == 0:
                break

            data = gpudb.GPUdbRecord.decode_binary_data(res["type_schema"], res["records_binary"])

            for column in columns:
                write_column_data(column, data)

            if not res["has_more_records"] or len(res["records_binary"]) < 10000:
                break

            i = i + len(res["records_binary"])


# Main

def execute():
    if args.distributed and args.nondistributed:
        parser.error("-d/--distributed and -n/--nondistributed are mutually exclusive")

    if args.nondistributed and args.input:
        parser.error("-n/--nondistribtued and -i/--input are mutually exclusive")

    if args.nondistributed and args.output:
        parser.error("-n/--nondistributed and -o/--output are mutually exclusive")

    if args.input or args.output:
        args.distributed = True
    elif not args.distributed:
        args.nondistributed = True

    icf = tempfile.NamedTemporaryFile(prefix="kinetica-udf-sim-icf-", dir=args.path, delete=False)

    write_uint64(icf, 1)

    icf_info = {}
    icf_info["run_id"] = "0"
    icf_info["proc_name"] = "proc"

    if args.distributed:
        icf_info["rank_number"] = "1"
        icf_info["tom_number"] = "0"
    else:
        icf_info["rank_number"] = "0"

    icf_info["data_segment_id"] = "0"
    icf_info["data_segment_number"] = "0"
    icf_info["data_segment_count"] = "1"
    icf_info["head_url"] = args.url
    icf_info["username"] = args.username
    icf_info["password"] = args.password
    write_dict(icf, icf_info)

    write_dict(icf, {})

    icf_params = {}

    if args.param:
        for param in args.param:
            icf_params[param[0]] = param[1]

    write_dict(icf, icf_params)

    write_dict(icf, {})

    if args.input or args.output:
        db = gpudb.GPUdb(encoding="BINARY", host=args.url, username=args.username, password=args.password)

    if args.input:
        write_uint64(icf, len(args.input))

        for table in args.input:
            write_table(icf, db, table, True)
    else:
        write_uint64(icf, 0)

    if args.output:
        write_uint64(icf, len(args.output))

        for table in args.output:
            write_table(icf, db, [table], False)
    else:
        write_uint64(icf, 0)

    write_string(icf, tempfile.NamedTemporaryFile(prefix="kinetica-udf-sim-", dir=args.path, delete=False).name)

    print("export KINETICA_PCF=" + icf.name)


def output():
    if "KINETICA_PCF" not in os.environ:
        raise RuntimeError("No control file specified")

    icf = os.environ["KINETICA_PCF"]

    if not os.path.exists(icf):
        raise RuntimeError("Specified control file does not exist")

    icf = open(icf, "rb")

    if read_uint64(icf) != 1:
        raise RuntimeError("Unrecognized control file version")

    read_dict(icf)
    read_dict(icf)
    read_dict(icf)
    read_dict(icf)

    for io in range(0, 2):
        for i in range(0, read_uint64(icf)):
            read_string(icf)

            for j in range(0, read_uint64(icf)):
                read_string(icf)
                read_uint64(icf)
                read_string(icf)
                read_string(icf)
                read_string(icf)

        if io == 0:
            output_pos = icf.tell()

    ocf = read_string(icf)

    if os.path.getsize(ocf) == 0:
        raise RuntimeError("No output detected")

    ocf = open(ocf, "rb")

    if read_uint64(ocf) != 1:
        raise RuntimeError("Unrecognized output control file version")

    results = read_dict(ocf)

    if results:
        print("Results:")
        print("")

        for key, value in results.items():
            print(key + ": " + value)

        print("")
    else:
        print("No results")

    icf.seek(output_pos)

    table_count = read_uint64(icf)

    if table_count > 0:
        print("Output:")
        print("")

        db = gpudb.GPUdb(encoding="BINARY", host=args.url, username=args.username, password=args.password)

        for i in range(0, table_count):
            read_table(icf, db)
    else:
        print("No output")


def clean():
    if "KINETICA_PCF" not in os.environ:
        raise RuntimeError("No control file specified")

    icf_name = os.environ["KINETICA_PCF"]

    if not os.path.exists(icf_name):
        raise RuntimeError("Specified control file does not exist")

    icf = open(icf_name, "rb")

    if read_uint64(icf) != 1:
        raise RuntimeError("Unrecognized control file version")

    read_dict(icf)
    read_dict(icf)
    read_dict(icf)
    read_dict(icf)

    for io in range(0, 2):
        for i in range(0, read_uint64(icf)):
            read_string(icf)

            for j in range(0, read_uint64(icf)):
                read_string(icf)
                read_uint64(icf)

                filename = read_string(icf)

                if filename and os.path.exists(filename):
                    os.remove(filename)

                filename = read_string(icf)

                if filename and os.path.exists(filename):
                    os.remove(filename)

                filename = read_string(icf)

                if filename and os.path.exists(filename):
                    os.remove(filename)

    filename = read_string(icf)

    if filename and os.path.exists(filename):
        os.remove(filename)

    icf.close()
    os.remove(icf_name)


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

execute_parser = subparsers.add_parser("execute", help="Simulate proc execution")
execute_parser.set_defaults(func=execute)

group = execute_parser.add_argument_group(title="Basic parameters")
group.add_argument("-f", "--path", default=".", metavar="PATH", help="Control file path")
group.add_argument("-p", "--param", action="append", metavar=("NAME","VALUE"), nargs=2, help="Proc parameter")

group = execute_parser.add_argument_group(title="Distributed")
group.add_argument("-d", "--distributed", action="store_true", help="Simulate distributed proc execution")
group.add_argument("-i", "--input", action="append", metavar=("TABLE","COLUMN"), nargs="+", help="Input table (optionally followed by column list)")
group.add_argument("-o", "--output", action="append", metavar="TABLE", help="Output table")

group = execute_parser.add_argument_group(title="Nondistributed")
group .add_argument("-n", "--nondistributed", action="store_true", help="Simulate nondistributed proc exeuction")

group = execute_parser.add_argument_group(title="Kinetica connection")
group.add_argument("-K", "--url", default="http://localhost:9191", help="Kinetica URL")
group.add_argument("-U", "--username", default="", help="Kinetica username")
group.add_argument("-P", "--password", default="", help="Kinetica password")

output_parser = subparsers.add_parser("output", help="Process proc output")
output_parser.set_defaults(func=output)

group = output_parser.add_argument_group(title="Basic parameters")
group.add_argument("-d", "--dry-run", dest="dryrun", action="store_true", help="Display output only, do not write to Kinetica")

group = output_parser.add_argument_group(title="Kinetica connection")
group.add_argument("-K", "--url", default="http://localhost:9191", help="Kinetica URL")
group.add_argument("-U", "--username", default="", help="Kinetica username")
group.add_argument("-P", "--password", default="", help="Kinetica password")

clean_parser = subparsers.add_parser("clean", help="Clean up files")
clean_parser.set_defaults(func=clean)

args = parser.parse_args()
args.func()