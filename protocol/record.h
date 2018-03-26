/*----------------------------------------------------------------------------*/
/* record.h: RecordColumn, RecordType, and Record Python classes and related  */
/* types.                                                                     */
/*----------------------------------------------------------------------------*/

#ifndef _RECORD_H_
#define _RECORD_H_

#include <Python.h>

#include "avro.h"

/*----------------------------------------------------------------------------*/

/* ColumnDataType: an enumeration of data types for a RecordColumn object.
   These correspond to the Kinetica data types of the same names. The Python
   data types used for each are noted. */

typedef enum
{
    CDT_BYTES,     /* bytes (Python 3) or str (Python 2) */
    CDT_CHAR1,     /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR2,     /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR4,     /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR8,     /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR16,    /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR32,    /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR64,    /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR128,   /* str (Python 3) or unicode (Python 2) */
    CDT_CHAR256,   /* str (Python 3) or unicode (Python 2) */
    CDT_DATE,      /* datetime.date */
    CDT_DATETIME,  /* datetime.datetime */
    CDT_DOUBLE,    /* float */
    CDT_FLOAT,     /* float */
    CDT_INT,       /* int */
    CDT_INT8,      /* int */
    CDT_INT16,     /* int */
    CDT_LONG,      /* int (Python 3) or long (Python 2) */
    CDT_STRING,    /* str (Python 3) or unicode (Python 2) */
    CDT_TIME,      /* datetime.time */
    CDT_TIMESTAMP, /* int (Python 3) or long (Python 2) */
    CDT_MAX        /* Not a valid type; used for loop termination. */
}
ColumnDataType;

/*----------------------------------------------------------------------------*/

/* ColumnDef: struct identifying the complete data type of a RecordColumn
   object. */

typedef struct
{
    /* The data type of the column when not null. */
    ColumnDataType data_type;

    /* Whether the column is nullable (0 = not nullable, 1 = nullable). */
    char is_nullable;
}
ColumnDef;

/*----------------------------------------------------------------------------*/

/* ColumnValueBase: a union containing the raw value of a column in a Record
   object. The column's data type determines which field is used. */

typedef union
{
    /* Pointer to a buffer containing a string (UTF-8) or bytes type value (not
       null terminated). Used for bytes, string, and charN with N > 8. The
       length of the value is stored in the enclosing ColumnValue struct.

       Note: In order to conserve memory, if the value has been decoded into a
       Python bytes or str object (Python 3) or str object (Python 2), this
       pointer points to the internal buffer of that object and must not be
       modified or freed. Otherwise, it points to a buffer allocated and owned
       by the Record object. */
    char* data;

    /* Buffer of up to 8 characters (not null terminated). Used for CharN with
       N <= 8. */
    char c[8];

    /* Used for double columns. */
    double d;

    /* Used for float columns. */
    float f;

    /* Used for date, int8, int16, int, and time columns. For date and time,
       Kinetica date and time formats are used. */
    long i;

    /* Used for datetime, long and timestamp columns. For datetime, Kinetica
       datetime format is used. */
    PY_LONG_LONG l;
}
ColumnValueBase;

/*----------------------------------------------------------------------------*/

/* ColumnValue: a struct containing the value of a column in a Record object. */

typedef struct
{
    /* Raw value of the column when not null. */
    ColumnValueBase value;

    /* For variable-length columns (bytes, string, charN), the length of the
       value, or -1 if the value is null. Otherise, 0 if the value is not null,
       or -1 if the value is null. */
    Py_ssize_t len;
}
ColumnValue;

/*----------------------------------------------------------------------------*/

/* RecordColumn: an immutable Python class representing the definition of a
   column in a Kinetica record type. */

typedef struct
{
    PyObject_HEAD

    /* Unicode object containing the name of the column. */
    PyObject* name;

    /* Unicode object containing the name of the data type of the column. This
       is returned when the data_type member is accessed.

       Note that this refers to the Kinetica data type, not the Avro data type,
       and those may differ (e.g. "date" for date data type is a "string" in
       the corresponding Avro schema). */
    PyObject* data_type_name;

    /* Tuple containing unicode objects for the properties of the column in
       Kinetica.

       Note that properties denoting data types which are implied by the
       data_type_name field (e.g. "date") are optional in this tuple and may
       be excluded. */
    PyObject* properties;

    /* The data type of the column. Not exposed via Python. */
    ColumnDataType data_type;

    /* Whether the column is nullable (0 = not nullable, 1 = nullable). */
    char is_nullable;
}
RecordColumn;

/* Python type object structure and type check macro. */
extern PyTypeObject RecordColumn_type;
#define RecordColumn_check(o) PyObject_TypeCheck(o, &RecordColumn_type)

/*----------------------------------------------------------------------------*/

/* RecordType: an immutable Python class representing the definition of a
   Kinetica record type.

   Note: This is a variable-length class. */

typedef struct
{
    PyObject_VAR_HEAD

    /* Unicode object containing the type label. */
    PyObject* label;

    /* Tuple contiaining RecordColumn objects for the columns of the record
       type in definition order. Not directly exposed via Python. */
    PyObject* columns;

    /* Dict mapping unicode objects containing column names to BufferRange
       objects containing the corresponding column indices in their start
       members. Used for efficient lookup of columns by name. Not directly
       exposed via Python. */
    PyObject* column_indices;

    /* The first entry in a variable-length array of ColumnDef structs, one
       for each column. Used for determining column data types without having
       to traverse Python objects and thus hold the GIL. */
    ColumnDef column_defs;
}
RecordType;

/* Python type object structure and type check macro. */
extern PyTypeObject RecordType_type;
#define RecordType_check(o) PyObject_TypeCheck(o, &RecordType_type)

/*----------------------------------------------------------------------------*/

/* Record: a Python class containing the values of a Kinetica record of a
   specific record type. The values are mutable, but the record type cannot
   be changed after creation.

   General principle of operation: A Record instance holds a raw (C-typed)
   value and, optionally, a Python object value for each column defined in the
   record type. When the record is decoded from an Avro buffer, only the raw
   values are initially populated; these values are converted into Python
   objects on demand when accessed via Python. This enables decoding to occur
   without holding the GIL, and prevents unnecessarily creating Python objects
   that may not be used. When a column value is set from Python, both the
   raw and Python object values are retained.

   For variable-length data types that may be larger than 8 bytes (bytes,
   string, and charN with N > 8), a separate buffer outside of the Record
   instance must be allocated to hold the raw value. If possible, when
   converted into a Python bytes or str object, this buffer is freed and the
   Python object's internal buffer is used instead, to avoid holding two copies
   of the value in memory. Because Python bytes and str objects are immutable,
   this buffer can be accessed outside of the GIL during Avro encoding since it
   cannot be altered from another thread. (This is not possible for unicode
   objects in Python 2.x since their internal buffers do not store UTF-8.)

   Note: This is a variable-length class. */

typedef struct
{
    PyObject_VAR_HEAD

    /* The RecordType object defining the type of the record. */
    RecordType* type;

    /* List of Python object values for the columns of the record, in
       definition order. Some values may be null pointers if they have not
       yet been created from raw values. Not directly exposed via Python. */
    PyObject* values;

    /* Cached size in bytes of the Avro-encoded binary form of the record. This
       is computed during the encoding process and retained until a value in
       the record is subsequently changed. A value of 0 indicates that the size
       has not been computed since the last value change. Not directly exposed
       via Python. */
    Py_ssize_t size;

    /* The first entry in a variable-length array of ColumnValue structs, one
       for each column. These hold the raw values of the columns in the
       record. Used for encoding and decoding without using Python objects
       or holding the GIL. */
    ColumnValue column_values;
}
Record;

/* Internal function to read an Avro-encoded record into an empty Record
   object. May be called without holding the GIL.

   pos: Pointer to a pointer to the start of the Avro-encoded record data.
        This will be updated to point to the position immediately following
        the record data on return. If an error occurs, this may point to an
        arbitrary location between the initial value and max on return.

   max: Pointer to the end of the buffer containing the Avro-encoded record
        data (must be >= *pos, and may be beyond the end of the record data).
        If this is reached before the record data is completely read, ERR_EOF
        is returned.

   record: Pointer to the Record object to read into. The Avro-encoded record
           data is assumed to match the record type of this object. The object
           must not contain any existing values; to read into a previously
           used object it must be cleared first.

   Returns an error code, or ERR_NONE if successful. On error any values that
   were read successfully are cleared before returning. */
AvroErrorCode read_record(uint8_t** pos, uint8_t* max, Record* record);

/* Internal function to compute the size in bytes of the Avro-encoded binary
   form of a Record object. The size is cached in the object and the cached
   value is returned in subsequent calls until values in the Record are
   changed. May be called without holding the GIL. */
Py_ssize_t size_record(Record* record);

/* Internal function to write a Record object into a buffer in Avro-encoded
   binary form. May be called without holding the GIL.

   pos: Pointer to a pointer to the position within the buffer to write the
        Avro-encoded record data. This will be updated to point to the
        position immediately following the record data on return. If an error
        occurs, this may point to an arbitrary location between the initial
        value and max on return.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the record data is completely written, ERR_EOF is returned.

   record: Pointer to the Record object to write.

   Returns an error code, or ERR_NONE if successful. */
AvroErrorCode write_record(uint8_t** pos, uint8_t* max, Record* record);

/* Python type object structure and type check macro. */
extern PyTypeObject Record_type;
#define Record_check(o) PyObject_TypeCheck(o, &Record_type)

/*----------------------------------------------------------------------------*/

/* File initialization: called during module initialization. */
int init_record(PyObject* module);

#endif /* _RECORD_H_ */
