/*----------------------------------------------------------------------------*/
/* schema.h: Schema Python class and related types.                           */
/*----------------------------------------------------------------------------*/

#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include <Python.h>

/*----------------------------------------------------------------------------*/

/* SchemaDataType: an enumeration of data types for a Schema object.

   Some data types are simple data types, and some have one or more fields which
   are Schema objects describing contained sub-values. */

typedef enum
{
    /* A nullable value, represented in Avro as a union of some type and null,
       and in Python as either that type or None. Must have exactly one field
       describing the type when the value is not null. */
    SDT_NULLABLE,

    /* A Boolean value, represented in Python as a bool. Must have no fields. */
    SDT_BOOLEAN,

    /* A binary value, represented in Python as a bytes (Python 3) or str
       (Python 2). Must have no fields. */
    SDT_BYTES,

    /* An IEEE double-precision floating point value, represented in Python as
       a float. Must have no fields. */
    SDT_DOUBLE,

    /* An IEEE single-precision floating point value, represented in Python as
       a float. Must have no fields. */
    SDT_FLOAT,

    /* A 32-bit integer value, represented in Python as an int. Must have no
       fields. */
    SDT_INT,

    /* A 64-bit integer value, represented in Python as an int (Python 3) or
       long (Python 2). Must have no fields. */
    SDT_LONG,

    /* A Unicode string value, represented in Python as a str (Python 3) or
       unicode (Python 2). Must have no fields. */
    SDT_STRING,

    /* An array, represented in Python as a list. Must have exactly one field
       describing the items in the array. */
    SDT_ARRAY,

    /* A map of strings to values, represented in Python as a dict. Must have
       exactly one field describing the values in the map. */
    SDT_MAP,

    /* A record, represented in Python as a dict. Must have at least one field,
       and all fields must be named. */
    SDT_RECORD,

    /* An embedded object encoded with an unrelated schema, represented in Avro
       as a binary value. Represented differently in Python depending on
       whether the object is being read or written:

       When read, the object is represented as a BufferRange containing the
       start position and length of the object's encoded binary value within
       the buffer being read from.

       When being written, the object is represented as a tuple of two values:
       a Schema or RecordType describing the schema of the object, and the
       Python representation of the object. If the schema is a RecordType, the
       object must be a Record of the same RecordType. An empty tuple can also
       be used, representing an empty object (an empty binary value in Avro). */
    SDT_OBJECT,

    /* An array of embedded objects encoded with an unrelated schema,
       represented in Avro as an array of binary values. Represented
       differently in Python depending on whether the objects are being read
       or written:

       When read, the array is represented as a list of BufferRanges, each
       containing the start position and length of one object's encoded binary
       value within the buffer being read from.

       When being written, the array is represented as a tuple of two values:
       a Schema or RecordType describing the schema of the objects, and a list
       of Python representations of the objects. If the schema is a RecordType,
       all of the objects must be Records of the same RecordType. An empty
       tuple can also be used, representing an empty array containing no
       objects. */
    SDT_OBJECT_ARRAY,

    /* Not a valid type; used for loop termination. */
    SDT_MAX
}
SchemaDataType;

/*----------------------------------------------------------------------------*/

/* Schema: an immutable Python class representing an Avro schema. */

typedef struct
{
    PyObject_HEAD

    /* Unicode object containing the name of the data type of the schema.
       This is returned when the data_type member is accessed. */
    PyObject* data_type_name;

    /* Unicode object containing the name of the field within a parent record
       schema that this schema represents. None if not applicable. */
    PyObject* name;

    /* The default value of the field within a parent record schema that this
       schema represents. None if not applicable or no default value. During
       encoding, this value will be filled in automatically if the field is
       absent or set to None. */
    PyObject* default_value;

    /* Tuple containing a Schema object for each field that is part of this
       schema. See explanation at SchemaDataType above. */
    PyObject* fields;

    /* The data type of the schema. Not exposed via Pyhton. */
    SchemaDataType data_type;
}
Schema;

/* Python type object structure and type check macro. */
extern PyTypeObject Schema_type;
#define Schema_check(o) PyObject_TypeCheck(o, &Schema_type)

/*----------------------------------------------------------------------------*/

/* File initialization: called during module initialization. */
int init_schema(PyObject* module);

#endif /* _SCHEMA_H_ */
