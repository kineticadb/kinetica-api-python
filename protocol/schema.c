/*----------------------------------------------------------------------------*/
/* schema.c: Schema Python class and related types.                           */
/*----------------------------------------------------------------------------*/

#include "schema.h"

#include "structmember.h"

#include "avro.h"
#include "bufferrange.h"
#include "common.h"
#include "protocol.h"
#include "record.h"

/*----------------------------------------------------------------------------*/

/* Forward declarations for schema validation and I/O functions. */

static Schema* validate_schema(Schema* schema);
static PyObject* read_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max);
static PyObject* prepare_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size);
static int write_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value);

/*----------------------------------------------------------------------------*/

/* Schema class implementation. */

/* Internal function that returns a tuple containing the values necessary to
   reconstruct a Schema. Used for implementing __repr__. */
static PyObject* _Schema_repr_object(Schema* self)
{
    PyObject* tuple = NULL;

    Py_ssize_t field_count;
    Py_ssize_t count;

    field_count = PyTuple_GET_SIZE(self->fields);
    count = (self->name != Py_None ? 1 : 0) + (self->default_value != Py_None ? 1 : 0) + (field_count > 0 ? 1 : 0);

    if (count == 0)
    {
        Py_INCREF(self->data_type_name);
        return self->data_type_name;
    }

    tuple = PyTuple_New(count + 1);
    CHECK(tuple, error)
    count = 0;

    if (self->name != Py_None)
    {
        Py_INCREF(self->name);
        PyTuple_SET_ITEM(tuple, count, self->name);
        ++count;
    }

    Py_INCREF(self->data_type_name);
    PyTuple_SET_ITEM(tuple, count, self->data_type_name);
    ++count;

    if (self->default_value != Py_None)
    {
        Py_INCREF(self->default_value);
        PyTuple_SET_ITEM(tuple, count, self->default_value);
        ++count;
    }

    if (field_count > 0)
    {
        PyObject* field_list;
        Py_ssize_t i;

        field_list = PyList_New(field_count);
        CHECK(field_list, error)
        PyTuple_SET_ITEM(tuple, count, field_list);

        for (i = 0; i < field_count; ++i)
        {
            PyObject* field_repr = _Schema_repr_object((Schema*)PyTuple_GET_ITEM(self->fields, i));
            CHECK(field_repr, error)
            PyList_SET_ITEM(field_list, i, field_repr);
        }
    }

    return tuple;

error:
    Py_XDECREF(tuple);
    return NULL;
}

/* Python Schema object deallocator. */
static void Schema_dealloc(Schema* self)
{
    Py_XDECREF(self->data_type_name);
    Py_XDECREF(self->name);
    Py_XDECREF(self->default_value);
    Py_XDECREF(self->fields);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/* Python Schema.decode method. Decodes an object from an Avro-encoded binary
   buffer according to the schema definition.

   Parameters:
       buffer (buffer)
           The buffer containing the Avro-encoded binary data.

       range (BufferRange, optional)
           Range of bytes within the buffer containing the object. If not
           specified, the entire buffer is used. If the object's data does
           not take up the entire range, any extra data is ignored.

    Returns:
        The decoded object. */
static PyObject* Schema_decode(Schema* self, PyObject* args, PyObject* kwargs)
{
    Py_buffer buffer = { NULL };

    PyObject* arg_range = NULL;
    static char* keywords[] = { "buffer", "range", NULL };

    PyObject* result;

    uint8_t* pos;
    uint8_t* max;

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "s*|O", keywords, &buffer, &arg_range), error)

    if (arg_range)
    {
        Py_ssize_t start;
        Py_ssize_t length;

        CHECK_STRING(BufferRange_check(arg_range), PyExc_TypeError, "range must be BufferRange", error)
        start = ((BufferRange*)arg_range)->start;
        CHECK_STRING(start >= 0 && start <= buffer.len, PyExc_ValueError, "start index out of range", error)
        pos = (uint8_t*)buffer.buf + start;
        length = ((BufferRange*)arg_range)->length;
        CHECK_STRING(length >= 0 && start + length <= buffer.len, PyExc_ValueError, "length out of range", error)
        max = pos + length;
    }
    else
    {
        pos = (uint8_t*)buffer.buf;
        max = pos + buffer.len;
    }

    result = read_schema(self, (uint8_t*)buffer.buf, &pos, max);
    PyBuffer_Release(&buffer);
    return result;

error:
    if (buffer.buf)
    {
        PyBuffer_Release(&buffer);
    }

    return NULL;
}

/* Python Schema.encode method. Encodes an object into Avro-encoded binary
   format according to the schema definition.

   Parameters:
       value (object):
           The object to encode. Must conform to the schema definition.

   Returns:
       Bytes (Python 3) or str (Python 2) containing the Avro-encoded binary
       data. */
static PyObject* Schema_encode(Schema* self, PyObject* value)
{
    PyObject* prepared = NULL;
    PyObject* result = NULL;

    PyObject* path = NULL;
    Py_ssize_t size = 0;

    uint8_t* pos;
    uint8_t* max;

    /* First, prepare the value. This validates that the value is valid
       according to the schema's data type, does any necessary data type
       conversions, and precomputes the required buffer size. */

    prepared = prepare_schema(self, value, &path, &size);

    if (!prepared)
    {
        if (path)
        {
            prefix_exception(path);
            Py_DECREF(path);
            goto error;
        }
    }

    /* Allocate a buffer of the correct size to hold the result. */

    #if PY_MAJOR_VERSION >= 3
        result = PyBytes_FromStringAndSize(NULL, size);
        CHECK(result, error)
        pos = (uint8_t*)PyBytes_AS_STRING(result);
    #else
        result = PyString_FromStringAndSize(NULL, size);
        CHECK(result, error)
        pos = (uint8_t*)PyString_AS_STRING(result);
    #endif

    max = pos + size;

    /* Encode the prepared value into the buffer. */

    CHECK(write_schema(self, &pos, max, prepared), error)

    Py_DECREF(prepared);
    return result;

error:
    Py_XDECREF(prepared);
    Py_XDECREF(result);
    return NULL;
}

/* Python Schema object constructor.

   Parameters:
       name (unicode, optional)
           Name of the field within a parent record schema that this schema
           represents. Defaults to None.

       data_type (unicode)
           Name of the data type of the schema. Must be a valid data type.

       default_value (object, optional)
           The default value of the field within a parent record schema that
           this schema represents. Defaults to None. If provided, must be a
           valid value for the data type of this schema.

       fields (sequence of Schema or unicode, optional)
           Schema objects for fields that are part of this schema. Unicode
           strings can also be used if a field only requires a data type to
           be specified (without a name, default value, or subfields). Defaults
           to None. Requirements vary based on the data type of this schema. */
static Schema* Schema_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* name = NULL;
    PyObject* data_type_name = NULL;
    PyObject* default_value = NULL;
    PyObject* field_seq = NULL;
    PyObject* fields = NULL;

    PyObject* arg_0;
    PyObject* arg_1 = NULL;
    PyObject* arg_2 = NULL;
    PyObject* arg_3 = NULL;
    PyObject* arg_data_type;
    static char* keywords[] = { "name", "data_type", "default_value", "fields", NULL };

    SchemaDataType data_type;
    Schema* self;

    ProtocolState* state = GET_STATE();
    CHECK(state, error)

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", keywords, &arg_0, &arg_1, &arg_2, &arg_3), error);

    if (!kwargs)
    {
        /* Since all parameters except the second (data_type) are optional, if
           keywords are not used, it isn't possible to know which parameter is
           which based on order alone. So analyze what was provided to find
           the data_type. */

        if (!arg_1)
        {
            /* Only one parameter was passed, so it must be data_type. */

            arg_data_type = arg_0;
            arg_0 = NULL;
        }
        else if (!arg_3 && !IS_STRING(arg_1))
        {
            /* Two or three parameters were passed. The second one is not a
               string, so it can't be data_type, which means name wasn't
               passed and the first parameter must be data_type. */

            arg_data_type = arg_0;
            arg_0 = NULL;

            /* Since name, which would normally be the first parameter, was
               not passed, the second and third position parameters (arg_1 and
               arg_2), if present, actually belong in positions three and four
               (arg_2 and arg_3), so bump them forward. */

            if (arg_2)
            {
                arg_3 = arg_2;
            }

            arg_2 = arg_1;
            arg_1 = NULL;
        }
        else
        {
            /* All four parameters were passed, or two or three were passed and
               the second one is a string. In either case, it can be assumed
               that a name was provided as the first parameter and data_type is
               the second.

               (The combination of data_type, default_value and optionally
               fields could in theory also result in the second parameter being
               a string, but this combination is not supported, since
               default_value is only used for record fields, which must have a
               name anyway.) */

            arg_data_type = arg_1;
            arg_1 = NULL;
        }
    }
    else
    {
        /* Since keywords were used, the keyword decoder will have put the
           parameters in the correct order, and no special detection is
           required. */

        arg_data_type = arg_1;
        CHECK_STRING(arg_data_type, PyExc_TypeError, "Required argument 'data_type' (pos 2) not found", error)
    }

    /* Look up data type by name in the schema data type name list. */

    CHECK_STRING(IS_STRING(arg_data_type), PyExc_TypeError, "data type must be string", error)
    data_type_name = TO_STRING(arg_data_type);
    CHECK(data_type_name, error)
    data_type = lookup_string(data_type_name, state->schema_data_type_names, SDT_MAX);
    CHECK_OBJECT(data_type != SDT_MAX, PyExc_ValueError, format_string("unknown data type %S", data_type_name), error)

    if (!kwargs)
    {
        /* Keywords were not used, so at this point, the third parameter
           position (arg_2) could be either default_value or fields. If both
           the third and fourth positions are present, then the third parameter
           must be default_value. Otherwise, if the data_type requires fields,
           assume the third position is fields, and bump it to the fourth
           position. */

        if (arg_2 && !arg_3 && (data_type == SDT_NULLABLE || data_type == SDT_ARRAY || data_type == SDT_MAP || data_type == SDT_RECORD))
        {
            arg_3 = arg_2;
            arg_2 = NULL;
        }
    }

    /* At this point, the first parameter position (arg_0) is name, the
       third (arg_2) is default_value, and the fourth (arg_3) is fields. */

    if (arg_0 && arg_0 != Py_None)
    {
        CHECK_STRING(IS_STRING(arg_0), PyExc_TypeError, "name must be string", error)
        name = TO_STRING(arg_0);
        CHECK(name, error)
    }
    else
    {
        Py_INCREF(Py_None);
        name = Py_None;
    }

    if (arg_2)
    {
        Py_INCREF(arg_2);
        default_value = arg_2;
    }
    else
    {
        Py_INCREF(Py_None);
        default_value = Py_None;
    }

    if (arg_3)
    {
        Py_ssize_t field_count;
        Py_ssize_t i;

        field_seq = PySequence_Fast(arg_3, "fields must be iterable");
        CHECK(field_seq, error)
        field_count = PySequence_Fast_GET_SIZE(field_seq);
        fields = PyTuple_New(field_count);
        CHECK(fields, error)

        for (i = 0; i < field_count; ++i)
        {
            PyObject* item = PySequence_Fast_GET_ITEM(field_seq, i);
            PyObject* field;

            if (PyTuple_Check(item))
            {
                /* The field is a tuple, so assume it contains the parameters
                   for the Schema constructor and call it recursively. */

                field = PyObject_Call((PyObject*)&Schema_type, item, NULL);
                CHECK(field, error)
            }
            else if (IS_STRING(item))
            {
                /* The field is a string, so assume it contains a data type.
                   Create a tuple out of it, and call the Schema constructor
                   recursively. */

                PyObject* temp = PyTuple_Pack(1, item);
                field = PyObject_Call((PyObject*)&Schema_type, temp, NULL);
                Py_DECREF(temp);
                CHECK(field, error)
            }
            else
            {
                /* The field is not a tuple or string, so it must be a
                   Schema. */

                CHECK_STRING(PyObject_TypeCheck(item, &Schema_type), PyExc_TypeError, "field must be Schema", error)
                Py_INCREF(item);
                field = item;
            }

            PyTuple_SET_ITEM(fields, i, field);
        }

        Py_CLEAR(field_seq);
    }
    else
    {
        fields = PyTuple_New(0);
        CHECK(fields, error)
    }

    self = (Schema*)type->tp_alloc(type, 0);
    CHECK(self, error)

    self->name = name;
    self->data_type_name = data_type_name;
    self->default_value = default_value;
    self->fields = fields;
    self->data_type = data_type;

    /* Make sure the resulting Schema object is valid. */

    return validate_schema(self);

error:
    Py_XDECREF(name);
    Py_XDECREF(data_type_name);
    Py_XDECREF(default_value);
    Py_XDECREF(field_seq);
    Py_XDECREF(fields);
    return NULL;
}

/* Python Schema.__repr__ method. */
static PyObject* Schema_repr(Schema* self)
{
    return generic_repr((PyObject*)self, (reprfunc)_Schema_repr_object);
}

/* Python Schema rich compare function (supports == and != operators). */
static PyObject* Schema_richcompare(PyObject* a, PyObject* b, int op)
{
    PyObject* result;
    int eq;

    result = generic_richcompare(&Schema_type, a, b, op);

    if (result != (PyObject*)&Schema_type)
    {
        return result;
    }

    eq = ((Schema*)a)->data_type == ((Schema*)b)->data_type;

    if (eq)
    {
        eq = PyObject_RichCompareBool(((Schema*)a)->name, ((Schema*)b)->name, Py_EQ);
        CHECK(eq != -1, error)

        if (eq)
        {
            eq = PyObject_RichCompareBool(((Schema*)a)->default_value, ((Schema*)b)->default_value, Py_EQ);
            CHECK(eq != -1, error)

            if (eq)
            {
                eq = PyObject_RichCompareBool(((Schema*)a)->fields, ((Schema*)b)->fields, Py_EQ);
                CHECK(eq != -1, error)
            }
        }
    }

    result = eq ? (op == Py_EQ ? Py_True : Py_False) : (op == Py_EQ ? Py_False : Py_True);
    Py_INCREF(result);
    return result;

error:
    return NULL;
}

static PyMemberDef Schema_members[] =
{
    { "name", T_OBJECT_EX, offsetof(Schema, name), READONLY, NULL },
    { "data_type", T_OBJECT_EX, offsetof(Schema, data_type_name), READONLY, NULL },
    { "default_value", T_OBJECT_EX, offsetof(Schema, default_value), READONLY, NULL },
    { "fields", T_OBJECT_EX, offsetof(Schema, fields), READONLY, NULL },
    { NULL }
};

static PyMethodDef Schema_methods[] =
{
    { "decode", (PyCFunction)Schema_decode, METH_VARARGS | METH_KEYWORDS, NULL },
    { "encode", (PyCFunction)Schema_encode, METH_O, NULL },
    { NULL }
};

PyTypeObject Schema_type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "kinetica.protocol.Schema",      /* tp_name */
    sizeof(Schema),                  /* tp_basicsize */
    0,                               /* tp_itemsize */
    (destructor)Schema_dealloc,      /* tp_dealloc */
    0,                               /* tp_print */
    0,                               /* tp_getattr */
    0,                               /* tp_setattr */
    0,                               /* tp_compare */
    (reprfunc)Schema_repr,           /* tp_repr */
    0,                               /* tp_as_number */
    0,                               /* tp_as_sequence */
    0,                               /* tp_as_mapping */
    0,                               /* tp_hash */
    0,                               /* tp_call */
    0,                               /* tp_str */
    0,                               /* tp_getattro */
    0,                               /* tp_setattro */
    0,                               /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,              /* tp_flags */
    0,                               /* tp_doc */
    0,                               /* tp_traverse */
    0,                               /* tp_clear */
    (richcmpfunc)Schema_richcompare, /* tp_richcompare */
    0,                               /* tp_weaklistoffset */
    0,                               /* tp_iter */
    0,                               /* tp_iternext */
    Schema_methods,                  /* tp_methods */
    Schema_members,                  /* tp_members */
    0,                               /* tp_getset */
    0,                               /* tp_base */
    0,                               /* tp_dict */
    0,                               /* tp_descr_get */
    0,                               /* tp_descr_set */
    0,                               /* tp_dictoffset */
    0,                               /* tp_init */
    0,                               /* tp_alloc */
    (newfunc)Schema_new,             /* tp_new */
};

/*----------------------------------------------------------------------------*/

/* Schema validation functions. */

/* Function type for a function that validates a Schema object with a specific
   data type. Returns 1 if the Schema is valid, or 0 (with exception set) if
   not. */
typedef int (*ValidateSchemaTypeFunc)(Schema*);

/* Forward declaration of validation function dispatch table. Each entry in
   the table contains a pointer to the Schema validation function for one
   data type. */
static ValidateSchemaTypeFunc validate_schema_types[SDT_MAX];

/* Validation function for Schemas with data types that have no fields. */
static int validate_zero_field_schema(Schema* schema)
{
    CHECK_OBJECT(PyTuple_GET_SIZE(schema->fields) == 0, PyExc_ValueError, format_string("%U must not have fields", schema->data_type_name), error)
    return 1;

error:
    return 0;
}

/* Validation function for Schemas with data types that require one field. */
static int validate_one_field_schema(Schema* schema)
{
    CHECK_OBJECT(PyTuple_GET_SIZE(schema->fields) == 1, PyExc_ValueError, format_string("%U must have exactly one field", schema->data_type_name), error)
    return 1;

error:
    return 0;
}

/* Validation type for Schemas with record data type. */
static int validate_record_schema(Schema* schema)
{
    PyObject* name_set;

    Py_ssize_t field_count;

    Py_ssize_t i;

    name_set = PySet_New(NULL);
    CHECK(name_set, error)

    field_count = PyTuple_GET_SIZE(schema->fields);
    CHECK_STRING(field_count > 0, PyExc_ValueError, "record must have at least one field", error)

    for (i = 0; i < field_count; ++i)
    {
        Schema* field;
        int r;

        field = (Schema*)PyTuple_GET_ITEM(schema->fields, i);
        CHECK_OBJECT(field->name != Py_None, PyExc_ValueError, format_string("record field %zd must have name", i), error)
        r = PySet_Contains(name_set, field->name);
        CHECK(r >= 0, error)
        CHECK_OBJECT(!r, PyExc_ValueError, format_string("duplicate record field name %S", field->name), error)
        CHECK(PySet_Add(name_set, field->name) == 0, error)
    }

    Py_DECREF(name_set);
    return 1;

error:
    Py_XDECREF(name_set);
    return 0;
}

/* Validation function dispatch table. */
static ValidateSchemaTypeFunc validate_schema_types[SDT_MAX] =
{
    validate_one_field_schema,  /* SDT_NULLABLE */
    validate_zero_field_schema, /* SDT_BOOLEAN */
    validate_zero_field_schema, /* SDT_BYTES */
    validate_zero_field_schema, /* SDT_DOUBLE */
    validate_zero_field_schema, /* SDT_FLOAT */
    validate_zero_field_schema, /* SDT_INT */
    validate_zero_field_schema, /* SDT_LONG */
    validate_zero_field_schema, /* SDT_STRING */
    validate_one_field_schema,  /* SDT_ARRAY */
    validate_one_field_schema,  /* SDT_MAP */
    validate_record_schema,     /* SDT_RECORD */
    validate_zero_field_schema, /* SDT_OBJECT */
    validate_zero_field_schema  /* SDT_OBJECT_ARRAY */
};

/* Internal function to validate a Schema object. Returns the object passed in
   if it is valid, otherwise returns NULL with an exception set. */
static Schema* validate_schema(Schema* schema)
{
    /* Dispatch the correct validation function based on the data type. */

    if (validate_schema_types[schema->data_type](schema))
    {
        /* The Schema appears to be valid, but default_value, if present,
           must be checked independently. */

        if (schema->default_value != Py_None)
        {
            /* A default value was provided. Attempt to prepare the default
               value for encoding, which will validate that it is valid
               according to the schema's data type. */

            PyObject* path = NULL;
            Py_ssize_t size = 0;
            PyObject* temp = prepare_schema(schema, schema->default_value, &path, &size);

            if (!temp)
            {
                if (path)
                {
                    prefix_exception(path);
                    Py_DECREF(path);
                }

                path = format_string_safe("invalid default value");

                if (path)
                {
                    prefix_exception(path);
                    Py_DECREF(path);
                }

                return NULL;
            }

            /* Discard the prepared value (the prepare was only for
               validation). */

            Py_DECREF(temp);
        }

        return schema;
    }
    else
    {
        Py_DECREF(schema);
        return NULL;
    }
}

/*----------------------------------------------------------------------------*/

/* Schema reading functions. */

/* Function type for a function that reads an object from an Avro-encoded
   binary buffer using a Schema object with a specific data type for decoding.
   All functions of this type take these parameters:

   schema: The Schema object to use for decoding.

   buf: Pointer to the start of the buffer containing the Avro-encoded binary
        object data. For object and object_array type Schemas, any returned
        BufferRange objects will contain start indices relative to this buffer.

   pos: Pointer to a pointer to the start of the Avro-encoded binary data for
        the object being read within the buffer. This will be updated to point
        to the position immediately following the object data on return. If an
        error occurs, this may point to an arbitrary location between the
        initial value and max on return.

    max: Pointer to the end of the buffer containing the Avro-encoded binary
         object data (must be >= *pos, and may be beyond the end of the object
         data). If this is reached before the object data is completely read,
         an exception is set.

    If successful, the object read is returned; otherwise NULL is returned and
    an exception is set. */
typedef PyObject* (*ReadSchemaTypeFunc)(Schema*, uint8_t*, uint8_t**, uint8_t*);

/* Forward declaration of reading function dispatch table. Each entry in the
   table contains a pointer to the Schema reading function for one data type. */
static ReadSchemaTypeFunc read_schema_types[SDT_MAX];

/* Reading function for nullable Schemas. */
static PyObject* read_nullable_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PY_LONG_LONG is_null;

    CHECK(handle_read_error(read_long(pos, max, &is_null)), error)

    if (is_null == 1)
    {
        Py_RETURN_NONE;
    }
    else if (is_null == 0)
    {
        return read_schema((Schema*)PyTuple_GET_ITEM(schema->fields, 0), buf, pos, max);
    }
    else
    {
        CHECK(handle_read_error(ERR_OVERFLOW), error)
    }

error:
    return NULL;
}

/* Reading function for boolean Schemas. */
static PyObject* read_boolean_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    char value = 0;

    CHECK(handle_read_error(read_boolean(pos, max, &value)), error)

    if (value)
    {
        Py_RETURN_TRUE;
    }
    else
    {
        Py_RETURN_FALSE;
    }

error:
    return NULL;
}

/* Reading function for bytes Schemas. */
static PyObject* read_bytes_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PyObject* result = NULL;

    Py_ssize_t len;

    CHECK(handle_read_error(read_bytes_len(pos, max, &len)), error)

    #if PY_MAJOR_VERSION >= 3
        result = PyBytes_FromStringAndSize(NULL, len);
        read_bytes_data(pos, max, (uint8_t*)PyBytes_AS_STRING(result), len);
    #else
        result = PyString_FromStringAndSize(NULL, len);
        read_bytes_data(pos, max, (uint8_t*)PyString_AS_STRING(result), len);
    #endif

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Reading function for double Schemas. */
static PyObject* read_double_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    double value = 0.0;

    CHECK(handle_read_error(read_double(pos, max, &value)), error)
    return PyFloat_FromDouble(value);

error:
    return NULL;
}

/* reading function for float Schemas. */
static PyObject* read_float_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    float value = 0.0f;

    CHECK(handle_read_error(read_float(pos, max, &value)), error)
    return PyFloat_FromDouble((double)value);

error:
    return NULL;
}

/* Reading function for int Schemas. */
static PyObject* read_int_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    long value;

    CHECK(handle_read_error(read_int(pos, max, &value)), error)

    #if PY_MAJOR_VERSION >= 3
        return PyLong_FromLong((long)value);
    #else
        return PyInt_FromLong((long)value);
    #endif

error:
    return NULL;
}

/* Reading function for long Schemas. */
static PyObject* read_long_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PY_LONG_LONG value;

    CHECK(handle_read_error(read_long(pos, max, &value)), error)
    return PyLong_FromLongLong((PY_LONG_LONG)value);

error:
    return NULL;
}

/* Reading function for string Schemas. */
static PyObject* read_string_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    Py_ssize_t len;
    char* data;
    PyObject* result;

    CHECK(handle_read_error(read_bytes_len(pos, max, &len)), error)
    data = (char*)MALLOC(len);
    CHECK(handle_read_error(data ? ERR_NONE : ERR_OOM), error)
    read_bytes_data(pos, max, (uint8_t*)data, len);
    result = PyUnicode_FromStringAndSize(data, len);
    free(data);
    return result;

error:
    return NULL;
}

/* Reading function for array Schemas. */
static PyObject* read_array_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PyObject* result = NULL;

    Schema* field;
    Py_ssize_t count;

    Py_ssize_t block_count = 0;
    Py_ssize_t i;

    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);
    CHECK(handle_read_error(read_size(pos, max, &block_count)), error)
    count = (block_count >= 0) ? block_count : -block_count;
    result = PyList_New(count);
    CHECK(result, error)
    i = 0;

    while (1)
    {
        if (block_count == 0)
        {
            break;
        }

        if (block_count < 0)
        {
            PY_LONG_LONG size;

            CHECK(handle_read_error(read_long(pos, max, &size)), error)
            block_count = -block_count;
        }

        if (i != 0)
        {
            PyObject* temp;
            Py_ssize_t j;

            count = i + block_count;
            temp = PyList_New(count);
            CHECK(temp, error)

            for (j = 0; j < i; ++j)
            {
                PyList_SET_ITEM(temp, j, PyList_GET_ITEM(result, j));
                PyList_SET_ITEM(result, j, NULL);
            }

            Py_DECREF(result);
            result = temp;
        }

        while (block_count > 0)
        {
            PyObject* item = read_schema(field, buf, pos, max);
            CHECK(item, error)
            PyList_SET_ITEM(result, i, item);
            ++i;
            --block_count;
        }

        CHECK(handle_read_error(read_size(pos, max, &block_count)), error)
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Reading function for map Schemas. */
static PyObject* read_map_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PyObject* result = NULL;
    PyObject* key = NULL;
    PyObject* value = NULL;

    Schema* field;

    result = PyDict_New();
    CHECK(result, error)
    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);

    while (1)
    {
        Py_ssize_t count = 0;

        CHECK(handle_read_error(read_size(pos, max, &count)), error)

        if (count == 0)
        {
            break;
        }

        if (count < 0)
        {
            PY_LONG_LONG size;

            CHECK(handle_read_error(read_long(pos, max, &size)), error)
            count = -count;
        }

        while (count > 0)
        {
            key = read_string_schema(schema, buf, pos, max);
            CHECK(key, error)
            value = read_schema(field, buf, pos, max);
            CHECK(value, error)
            CHECK(PyDict_SetItem(result, key, value) == 0, error)
            Py_DECREF(key);
            Py_CLEAR(value);
            --count;
        }
    }

    return result;

error:
    Py_XDECREF(result);
    Py_XDECREF(key);
    Py_XDECREF(value);
    return NULL;
}

/* Reading function for record Schemas. */
static PyObject* read_record_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PyObject* result;

    Py_ssize_t field_count;

    Py_ssize_t i;

    result = PyDict_New();
    CHECK(result, error)
    field_count = PyTuple_GET_SIZE(schema->fields);

    for (i = 0; i < field_count; ++i)
    {
        Schema* field;
        PyObject* value;
        int r;

        field = (Schema*)PyTuple_GET_ITEM(schema->fields, i);
        value = read_schema(field, buf, pos, max);
        CHECK(value, error)
        r = PyDict_SetItem(result, field->name, value);
        Py_DECREF(value);
        CHECK(r == 0, error)
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Reading function for object Schemas. */
static PyObject* read_object_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    Py_ssize_t len = 0;
    PyObject* result;

    CHECK(handle_read_error(read_bytes_len(pos, max, &len)), error)
    result = BufferRange_create((Py_ssize_t)(*pos - buf), len);
    CHECK(result, error)
    *pos += len;
    return result;

error:
    return NULL;
}

/* Reading function for object_array Schemas. */
static PyObject* read_object_array_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    PyObject* result = NULL;

    Py_ssize_t count;

    Py_ssize_t block_count = 0;
    Py_ssize_t i;

    CHECK(handle_read_error(read_size(pos, max, &block_count)), error)
    count = (block_count >= 0) ? block_count : -block_count;
    result = PyList_New(count);
    CHECK(result, error)
    i = 0;

    while (1)
    {
        if (block_count == 0)
        {
            break;
        }

        if (block_count < 0)
        {
            PY_LONG_LONG size;

            CHECK(handle_read_error(read_long(pos, max, &size)), error)
            block_count = -block_count;
        }

        if (i != 0)
        {
            PyObject* temp;
            Py_ssize_t j;

            count = i + block_count;
            temp = PyList_New(count);
            CHECK(temp, error)

            for (j = 0; j < i; ++j)
            {
                PyList_SET_ITEM(temp, j, PyList_GET_ITEM(result, j));
                PyList_SET_ITEM(result, j, NULL);
            }

            Py_DECREF(result);
            result = temp;
        }

        while (block_count > 0)
        {
            PyObject* item = read_object_schema(schema, buf, pos, max);
            CHECK(item, error)
            PyList_SET_ITEM(result, i, item);
            ++i;
            --block_count;
        }

        CHECK(handle_read_error(read_size(pos, max, &block_count)), error)
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Schema reading function dispatch table. */
static ReadSchemaTypeFunc read_schema_types[SDT_MAX] =
{
    read_nullable_schema,    /* SDT_NULLABLE */
    read_boolean_schema,     /* SDT_BOOLEAN */
    read_bytes_schema,       /* SDT_BYTES */
    read_double_schema,      /* SDT_DOUBLE */
    read_float_schema,       /* SDT_FLOAT */
    read_int_schema,         /* SDT_INT */
    read_long_schema,        /* SDT_LONG */
    read_string_schema,      /* SDT_STRING */
    read_array_schema,       /* SDT_ARRAY */
    read_map_schema,         /* SDT_MAP */
    read_record_schema,      /* SDT_RECORD */
    read_object_schema,      /* SDT_OBJECT */
    read_object_array_schema /* SDT_OBJECT_ARRAY */
};

/* Internal function to read an object from an Avro-encoded binary buffer using
   a Schema object for decoding. The function signature is the same as for
   ReadSchemaTypeFunc above. */
static PyObject* read_schema(Schema* schema, uint8_t* buf, uint8_t** pos, uint8_t* max)
{
    /* Dispatch the correct reading function based on the data type. */
    return read_schema_types[schema->data_type](schema, buf, pos, max);
}

/*----------------------------------------------------------------------------*/

/* Schema preparation functions.

   Preparation is the first step of the two-step process for writing an object
   into a buffer in Avro-encoded binary form using a Schema. It validates that
   the object is valid according to the schema's data type, does any necessary
   data type conversions, precomputes the required buffer size, and results in
   a new "prepared" object that is passed into the second step (write). */

/* Function type for a function that prepares an object for writing using a
   Schema object with a specific data type. All functions of this type take
   these parameters:

   schema: The Schema object to use for preparation.

   value: The object to prepare.

   path: Pointer to a variable that, on error, will be populated with a unicode
         object containing a description of the path through the schema to the
         place where the error occurred. (For example, "array item 10 of value
         of record field foo".) This can then be used as the basis for an
         exception message.

   size: Pointer to a Py_ssize_t that will be populated with the required
         buffer size to hold the Avro-encoded binary form of the object.

   The return value on success is the prepared object to pass to the
   write_schema function, or NULL on error. Note that the prepared object is a
   different object than the one passed into the value parameter and must be
   freed separately; the caller is also responsible for any object returned via
   the path parameter on error. */
typedef PyObject* (*PrepareSchemaTypeFunc)(Schema*, PyObject*, PyObject**, Py_ssize_t*);

/* Forward declaration of preparation function dispatch table. Each entry in
   the table contains a pointer to the Schema preparation function for one data
   type. */
static PrepareSchemaTypeFunc prepare_schema_types[SDT_MAX];

/* Preparation function for nullable Schemas. */
static PyObject* prepare_nullable_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    *size += 1;

    if (value == Py_None)
    {
        Py_RETURN_NONE;
    }
    else
    {
        return prepare_schema((Schema*)PyTuple_GET_ITEM(schema->fields, 0), value, path, size);
    }
}

/* Preparation function for boolean Schemas. */
static PyObject* prepare_boolean_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    int result = PyObject_IsTrue(value);
    CHECK(result != -1, error)
    *size += 1;

    if (result)
    {
        Py_RETURN_TRUE;
    }
    else
    {
        Py_RETURN_FALSE;
    }

error:
    return NULL;
}

/* Preparation function for bytes schemas. */
static PyObject* prepare_bytes_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result;
    Py_ssize_t len;

    #if PY_MAJOR_VERSION >= 3
        result = PyObject_Bytes(value);
        CHECK(result, error)
        len = PyBytes_GET_SIZE(result);
    #else
        result = PyObject_Str(value);
        CHECK(result, error)
        len = PyString_GET_SIZE(result);
    #endif

    *size += size_long((PY_LONG_LONG)len) + len;
    return result;

error:
    return NULL;
}

/* Preparation function for double schemas. */
static PyObject* prepare_double_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = PyNumber_Float(value);
    CHECK(result, error)
    *size += 8;
    return result;

error:
    return NULL;
}

/* Preparation function for float schemas. */
static PyObject* prepare_float_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = PyNumber_Float(value);
    CHECK(result, error)
    *size += 4;
    return result;

error:
    return NULL;
}

/* Preparation function for int schemas. */
static PyObject* prepare_int_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result;

    #if PY_MAJOR_VERSION >= 3
        long temp;

        result = PyNumber_Long(value);
        CHECK(result, error)
        temp = PyLong_AsLong(result);
        CHECK(temp != -1 || !PyErr_Occurred(), error)
        CHECK_STRING(temp >= INT32_MIN && temp <= INT32_MAX, PyExc_OverflowError, "value out of range", error)
    #else
        long temp;

        result = PyNumber_Int(value);
        CHECK(result, error)
        CHECK_STRING(PyInt_Check(result), PyExc_ValueError, "value out of range", error)
        temp = PyInt_AS_LONG(result);
        CHECK_STRING(temp >= INT32_MIN && temp <= INT32_MAX, PyExc_OverflowError, "value out of range", error)
    #endif

    *size += size_long((PY_LONG_LONG)temp);
    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Preparation function for long schemas. */
static PyObject* prepare_long_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result;

    PY_LONG_LONG temp;

    result = PyNumber_Long(value);
    CHECK(result, error)
    temp = PyLong_AsLongLong(result);
    CHECK(temp != -1 || !PyErr_Occurred(), error)
    CHECK_STRING(temp >= INT64_MIN && temp <= INT64_MAX, PyExc_OverflowError, "value out of range", error)
    *size += size_long(temp);
    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Preparation function for string schemas. */
static PyObject* prepare_string_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* string;
    PyObject* result;
    Py_ssize_t len;

    #if PY_MAJOR_VERSION >= 3
        string = PyObject_Str(value);
        CHECK(string, error)
        result = PyUnicode_AsUTF8String(string);
        Py_DECREF(string);
        CHECK(result, error)
        len = PyBytes_GET_SIZE(result);
    #else
        string = PyObject_Unicode(value);
        CHECK(string, error)
        result = PyUnicode_AsUTF8String(string);
        Py_DECREF(string);
        CHECK(result, error)
        len = PyString_GET_SIZE(result);
    #endif

    *size += size_long((PY_LONG_LONG)len) + len;
    return result;

error:
    return NULL;
}

/* Preparation function for array schemas. */
static PyObject* prepare_array_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = NULL;
    PyObject* value_seq;

    Py_ssize_t count;
    Schema* field;

    Py_ssize_t i;

    value_seq = PySequence_Fast(value, "value must be iterable");
    CHECK(value_seq, error)
    count = PySequence_Fast_GET_SIZE(value_seq);
    result = PyList_New(count);
    CHECK(result, error)
    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);

    for (i = 0; i < count; ++i)
    {
        PyObject* item = prepare_schema(field, PySequence_Fast_GET_ITEM(value_seq, i), path, size);
        CHECK(item, item_error)
        PyList_SET_ITEM(result, i, item);
    }

    *size += size_long((PY_LONG_LONG)count) + (count > 0 ? 1 : 0);
    Py_DECREF(value_seq);
    return result;

item_error:
    if (*path)
    {
        PyObject* temp = format_string_safe("%S of array item %zd", *path, i);
        Py_DECREF(*path);
        *path = temp;
    }
    else
    {
        *path = format_string_safe("array item %zd", i);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(value_seq);
    return NULL;
}

/* Preparation function for map schemas. */
static PyObject* prepare_map_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = NULL;
    PyObject* items = NULL;
    PyObject* items_seq = NULL;

    Py_ssize_t count;
    Schema* field;
    PyObject* key;

    Py_ssize_t i;

    CHECK_STRING(PyMapping_Check(value), PyExc_TypeError, "value must be mapping", error)
    items = PyMapping_Items(value);
    CHECK(items, error);
    items_seq = PySequence_Fast(items, "value contains invalid mapping");
    CHECK(items_seq, error);
    count = PySequence_Fast_GET_SIZE(items_seq);
    result = PyList_New(count * 2);
    CHECK(result, error);
    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);
    key = NULL;

    for (i = 0; i < count; ++i)
    {
        PyObject* item;
        PyObject* prepared_key;
        PyObject* value;

        item = PySequence_Fast_GET_ITEM(items_seq, i);
        CHECK_STRING(PyTuple_Check(item) && PyTuple_GET_SIZE(item) == 2, PyExc_TypeError, "value contains invalid mapping", error)
        key = PyTuple_GET_ITEM(item, 0);
        prepared_key = prepare_string_schema(schema, key, path, size);
        CHECK(prepared_key, key_error)
        key = prepared_key;
        PyList_SET_ITEM(result, i * 2, key);
        value = prepare_schema(field, PyTuple_GET_ITEM(item, 1), path, size);
        CHECK(value, value_error)
        PyList_SET_ITEM(result, i * 2 + 1, value);
    }

    *size += size_long((PY_LONG_LONG)count) + (count > 0 ? 1 : 0);
    Py_DECREF(items);
    Py_DECREF(items_seq);
    return result;

key_error:
    if (*path)
    {
        Py_DECREF(*path);
    }

    *path = format_string_safe("map key %S", key);
    goto error;

value_error:
    if (*path)
    {
        PyObject* temp = format_string_safe("%S of value of map key %S", *path, key);
        Py_DECREF(*path);
        *path = temp;
    }
    else
    {
        *path = format_string_safe("value of map key %S", key);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(items);
    Py_XDECREF(items_seq);
    return NULL;
}

/* Preparation function for record schemas. */
static PyObject* prepare_record_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = NULL;

    Py_ssize_t field_count;
    Py_ssize_t count;
    PyObject* key;

    Py_ssize_t i;

    CHECK_STRING(PyMapping_Check(value), PyExc_TypeError, "value must be mapping", error)
    field_count = PyTuple_GET_SIZE(schema->fields);
    result = PyList_New(field_count);
    CHECK(result, error)
    count = PyMapping_Length(value);
    CHECK(count >= 0, error)

    for (i = 0; i < field_count; ++i)
    {
        Schema* field;
        PyObject* prepared_field_value;

        field = (Schema*)PyTuple_GET_ITEM(schema->fields, i);
        key = field->name;

        if (PyMapping_HasKey(value, key))
        {
            PyObject* field_value = PyObject_GetItem(value, key);
            CHECK(field_value, key_error)

            if (field_value == Py_None)
            {
                Py_DECREF(field_value);
                CHECK_STRING(field->data_type == SDT_NULLABLE || field->default_value != Py_None, PyExc_ValueError, "required", key_error)
                prepared_field_value = prepare_schema(field, field->default_value, path, size);
            }
            else
            {
                prepared_field_value = prepare_schema(field, field_value, path, size);
                Py_DECREF(field_value);
            }

            --count;
        }
        else
        {
            CHECK_STRING(field->data_type == SDT_NULLABLE || field->default_value != Py_None, PyExc_ValueError, "not found", key_error)
            prepared_field_value = prepare_schema(field, field->default_value, path, size);
        }

        CHECK(prepared_field_value, value_error)
        PyList_SET_ITEM(result, i, prepared_field_value);
    }

    CHECK_STRING(count == 0, PyExc_ValueError, "extraneous fields provided", error)
    return result;

key_error:
    if (*path)
    {
        Py_DECREF(*path);
    }

    *path = format_string_safe("record field %S", key);
    goto error;

value_error:
    if (*path)
    {
        PyObject* temp = format_string_safe("%S of value of record field %S", *path, key);
        Py_DECREF(*path);
        *path = temp;
    }
    else
    {
        *path = format_string_safe("value of record field %S", key);
    }

error:
    Py_XDECREF(result);
    return NULL;
}

/* Preparation function for object schemas. */
static PyObject* prepare_object_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = NULL;

    Py_ssize_t tuple_size;
    PyObject* type;
    PyObject* object;

    CHECK_STRING(PyTuple_Check(value), PyExc_TypeError, "value must be tuple", error)
    tuple_size = PyTuple_GET_SIZE(value);

    if (tuple_size == 0)
    {
        Py_INCREF(value);
        *size += 1;
        return value;
    }

    CHECK_STRING(tuple_size, PyExc_TypeError, "value must contain type and object", error)
    result = PyTuple_New(2);
    CHECK(result, error)
    type = PyTuple_GET_ITEM(value, 0);
    Py_INCREF(type);
    PyTuple_SET_ITEM(result, 0, type);
    object = PyTuple_GET_ITEM(value, 1);

    if (PyObject_TypeCheck(type, &Schema_type))
    {
        PyObject* object_tuple;
        Py_ssize_t object_size;

        object_tuple = PyTuple_New(2);
        CHECK(object_tuple, error)
        PyTuple_SET_ITEM(result, 1, object_tuple);
        object_size = 0;
        object = prepare_schema((Schema*)type, object, path, &object_size);
        CHECK(object, object_error)
        PyTuple_SET_ITEM(object_tuple, 1, object);
        object = PyLong_FromSsize_t(object_size);
        CHECK(object, error)
        PyTuple_SET_ITEM(object_tuple, 0, object);
        *size += object_size + size_long((PY_LONG_LONG)object_size);
    }
    else
    {
        Py_ssize_t record_size;

        CHECK_STRING(RecordType_check(type), PyExc_TypeError, "type must be Schema or RecordType", error)
        CHECK_STRING(Record_check(object), PyExc_TypeError, "object must be Record", object_error)
        CHECK_STRING(((Record*)object)->type == (RecordType*)type, PyExc_ValueError, "object has incorrect RecordType", object_error)
        record_size = size_record((Record*)object);
        *size += record_size + size_long((PY_LONG_LONG)record_size);
        Py_INCREF(object);
        PyTuple_SET_ITEM(result, 1, object);
    }

    return result;

object_error:
    if (*path)
    {
        PyObject* temp = format_string_safe("%S of object", *path);
        Py_DECREF(*path);
        *path = temp;
    }
    else
    {
        *path = format_string_safe("object");
    }

error:
    Py_XDECREF(result);
    return NULL;
}

/* Preparation function for object_array Schemas. */
static PyObject* prepare_object_array_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    PyObject* result = NULL;
    PyObject* object_seq = NULL;

    Py_ssize_t tuple_size;
    PyObject* type;
    char is_schema;
    Py_ssize_t count;
    PyObject* prepared;

    Py_ssize_t i;

    CHECK_STRING(PyTuple_Check(value), PyExc_TypeError, "value must be tuple", error)
    tuple_size = PyTuple_GET_SIZE(value);

    if (tuple_size == 0)
    {
        Py_INCREF(value);
        *size += 1;
        return value;
    }

    CHECK_STRING(tuple_size == 2, PyExc_TypeError, "value must contain type and object list", error)
    type = PyTuple_GET_ITEM(value, 0);
    is_schema = PyObject_TypeCheck(type, &Schema_type);

    if (!is_schema)
    {
        CHECK_STRING(RecordType_check(type), PyExc_TypeError, "type must be Schema or RecordType", error)
    }

    result = PyTuple_New(2);
    CHECK(result, error)
    Py_INCREF(type);
    PyTuple_SET_ITEM(result, 0, type);
    object_seq = PySequence_Fast(PyTuple_GET_ITEM(value, 1), "object list must be iterable");
    CHECK(object_seq, error)
    count = PySequence_Fast_GET_SIZE(object_seq);
    prepared = PyList_New(count);
    CHECK(prepared, error)
    PyTuple_SET_ITEM(result, 1, prepared);

    if (is_schema)
    {
        for (i = 0; i < count; ++i)
        {
            PyObject* object_tuple;
            PyObject* object;
            Py_ssize_t object_size;

            object_tuple = PyTuple_New(2);
            CHECK(object_tuple, error)
            PyList_SET_ITEM(prepared, i, object_tuple);
            object_size = 0;
            object = prepare_schema((Schema*)type, PySequence_Fast_GET_ITEM(object_seq, i), path, &object_size);
            CHECK(object, object_error)
            PyTuple_SET_ITEM(object_tuple, 1, object);
            object = PyLong_FromSsize_t(object_size);
            CHECK(object, error)
            PyTuple_SET_ITEM(object_tuple, 0, object);
            *size += object_size + size_long((PY_LONG_LONG)object_size);
        }
    }
    else
    {
        for (i = 0; i < count; ++i)
        {
            PyObject* object;
            Py_ssize_t record_size;

            object = PySequence_Fast_GET_ITEM(object_seq, i);
            CHECK_STRING(Record_check(object), PyExc_TypeError, "object must be Record", object_error)
            CHECK_STRING(((Record*)object)->type == (RecordType*)type, PyExc_ValueError, "object has incorrect RecordType", object_error)
            Py_INCREF(object);
            PyList_SET_ITEM(prepared, i, object);
            record_size = size_record((Record*)object);
            *size += record_size + size_long((PY_LONG_LONG)record_size);
        }
    }

    *size += size_long((PY_LONG_LONG)count) + (count > 0 ? 1 : 0);
    Py_DECREF(object_seq);
    return result;

object_error:
    if (*path)
    {
        PyObject* temp = format_string_safe("%S of array object %zd", *path, i);
        Py_DECREF(*path);
        *path = temp;
    }
    else
    {
        *path = format_string_safe("array object %zd", i);
    }

error:
    Py_XDECREF(result);
    Py_XDECREF(object_seq);
    return NULL;
}

/* Preparation function dispatch table. */
static PrepareSchemaTypeFunc prepare_schema_types[SDT_MAX] =
{
    prepare_nullable_schema,    /* SDT_NULLABLE */
    prepare_boolean_schema,     /* SDT_BOOLEAN */
    prepare_bytes_schema,       /* SDT_BYTES */
    prepare_double_schema,      /* SDT_DOUBLE */
    prepare_float_schema,       /* SDT_FLOAT */
    prepare_int_schema,         /* SDT_INT */
    prepare_long_schema,        /* SDT_LONG */
    prepare_string_schema,      /* SDT_STRING */
    prepare_array_schema,       /* SDT_ARRAY */
    prepare_map_schema,         /* SDT_MAP */
    prepare_record_schema,      /* SDT_RECORD */
    prepare_object_schema,      /* SDT_OBJECT */
    prepare_object_array_schema /* SDT_OBJECT_ARRAY */
};

/* Internal function to prepare an object for writing using a Schema object.
   The function signature is the same as for PrepareSchemaTypeFunc above. */
static PyObject* prepare_schema(Schema* schema, PyObject* value, PyObject** path, Py_ssize_t* size)
{
    return prepare_schema_types[schema->data_type](schema, value, path, size);
}

/*----------------------------------------------------------------------------*/

/* Schema writing functions.

   Writing is the second step of the two-step process for writing an object
   into a buffer in Avro-encoded binary form using a Schema. It requires a
   previously "prepared" form of the object being written, and that an
   appropriately-sized buffer has already been allocated. */

/* Function type for a function that writes a prepared object in Avro-encoded
   binary form into a buffer using a Schema object with a specific data type
   for encoding. All functions of this type take these parameters:

   schema: The Schema object to use for encoding.

   pos: Pointer to a pointer to the position within the buffer to write the
        object. This will be updated on a successful write to point to the
        position immediately following the object data on return. If an error
        occurs, this may point to an arbitrary location between the initial
        value and max on return.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the object data is completely written, an exception is set.

   value: The prepared object to write, previously returned by prepare_schema.

   Returns 1 on successful, or 0 (with exception set) if not. */
typedef int (*WriteSchemaTypeFunc)(Schema*, uint8_t**, uint8_t*, PyObject*);

/* Forward declaration of writing function dispatch table. Each entry in the
   table contains a pointer to the Schema writing function for one data type. */
static WriteSchemaTypeFunc write_schema_types[SDT_MAX];

/* Writing function for nullable Schemas. */
static int write_nullable_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    if (value == Py_None)
    {
        return handle_write_error(write_long(pos, max, 1));
    }
    else
    {
        CHECK(handle_write_error(write_long(pos, max, 0)), error)
        return write_schema((Schema*)PyTuple_GET_ITEM(schema->fields, 0), pos, max, value);
    }

error:
    return 0;
}

/* Writing function for boolean Schemas. */
static int write_boolean_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    if (value == Py_True)
    {
        return handle_write_error(write_boolean(pos, max, 1));
    }
    else
    {
        return handle_write_error(write_boolean(pos, max, 0));
    }
}

/* Writing function for bytes Schemas. */
static int write_bytes_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    #if PY_MAJOR_VERSION >= 3
        return handle_write_error(write_bytes(pos, max, (uint8_t*)PyBytes_AS_STRING(value), PyBytes_GET_SIZE(value)));
    #else
        return handle_write_error(write_bytes(pos, max, (uint8_t*)PyString_AS_STRING(value), PyString_GET_SIZE(value)));
    #endif
}

/* Writing function for double Schemas. */
static int write_double_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    return handle_write_error(write_double(pos, max, PyFloat_AS_DOUBLE(value)));
}

/* Writing function for float Schemas. */
static int write_float_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    return handle_write_error(write_float(pos, max, (float)PyFloat_AS_DOUBLE(value)));
}

/* Writing function for int Schemas. */
static int write_int_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    #if PY_MAJOR_VERSION >= 3
        return handle_write_error(write_int(pos, max, PyLong_AsLong(value)));
    #else
        return handle_write_error(write_int(pos, max, PyInt_AS_LONG(value)));
    #endif
}

/* Writing function for long Schemas. */
static int write_long_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    return handle_write_error(write_long(pos, max, PyLong_AsLongLong(value)));
}

/* Writing function for array Schemas. */
static int write_array_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    Py_ssize_t count;
    Schema* field;

    Py_ssize_t i;

    count = PyList_GET_SIZE(value);
    CHECK(handle_write_error(write_size(pos, max, count)), error)

    if (count == 0)
    {
        return 1;
    }

    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);

    for (i = 0; i < count; ++i)
    {
        CHECK(write_schema(field, pos, max, PyList_GET_ITEM(value, i)), error)
    }

    return handle_write_error(write_size(pos, max, 0));

error:
    return 0;
}

/* Writing function for map Schemas. */
static int write_map_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    Py_ssize_t count;
    Schema* field;

    Py_ssize_t i;

    count = PyList_GET_SIZE(value);
    CHECK(handle_write_error(write_size(pos, max, count / 2)), error)

    if (count == 0)
    {
        return 1;
    }

    field = (Schema*)PyTuple_GET_ITEM(schema->fields, 0);

    for (i = 0; i < count; i+= 2)
    {
        CHECK(write_bytes_schema(schema, pos, max, PyList_GET_ITEM(value, i)), error)
        CHECK(write_schema(field, pos, max, PyList_GET_ITEM(value, i + 1)), error)
    }

    return handle_write_error(write_size(pos, max, 0));

error:
    return 0;
}

/* Writing function for record Schemas. */
static int write_record_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    Py_ssize_t field_count;

    Py_ssize_t i;

    field_count = PyTuple_GET_SIZE(schema->fields);

    for (i = 0; i < field_count; ++i)
    {
        CHECK(write_schema((Schema*)PyTuple_GET_ITEM(schema->fields, i), pos, max, PyList_GET_ITEM(value, i)), error)
    }

    return 1;

error:
    return 0;
}

/* Writing function for object Schemas. */
static int write_object_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    PyObject* type;

    if (PyTuple_GET_SIZE(value) == 0)
    {
        CHECK(handle_write_error(write_size(pos, max, 0)), error);
        return 1;
    }

    type = PyTuple_GET_ITEM(value, 0);

    if (PyObject_TypeCheck(type, &Schema_type))
    {
        PyObject* object_tuple = PyTuple_GET_ITEM(value, 1);
        CHECK(handle_write_error(write_size(pos, max, PyLong_AsSsize_t(PyTuple_GET_ITEM(object_tuple, 0)))), error)
        return write_schema((Schema*)type, pos, max, PyTuple_GET_ITEM(object_tuple, 1));
    }
    else
    {
        Record* record = (Record*)PyTuple_GET_ITEM(value, 1);
        CHECK(handle_write_error(write_size(pos, max, size_record(record))), error)
        return handle_write_error(write_record(pos, max, record));
    }

error:
    return 0;
}

/* Writing function for object_array Schemas. */
static int write_object_array_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    Record** records = NULL;

    PyObject* type;
    PyObject* objects;
    Py_ssize_t count;

    Py_ssize_t i;

    if (PyTuple_GET_SIZE(value) == 0)
    {
        CHECK(handle_write_error(write_size(pos, max, 0)), error)
        return 1;
    }

    type = PyTuple_GET_ITEM(value, 0);
    objects = PyTuple_GET_ITEM(value, 1);
    count = PyList_GET_SIZE(objects);
    CHECK(handle_write_error(write_size(pos, max, count)), error)

    if (count == 0)
    {
        return 1;
    }

    if (PyObject_TypeCheck(type, &Schema_type))
    {
        for (i = 0; i < count; ++i)
        {
            PyObject* object_tuple = PyList_GET_ITEM(objects, i);
            CHECK(handle_write_error(write_size(pos, max, PyLong_AsSsize_t(PyTuple_GET_ITEM(object_tuple, 0)))), error)
            CHECK(write_schema((Schema*)type, pos, max, PyTuple_GET_ITEM(object_tuple, 1)), error)
        }
    }
    else
    {
        AvroErrorCode error;

        records = PyMem_New(Record*, count);
        CHECK_NONE(records, PyExc_MemoryError, error)

        for (i = 0; i < count; ++i)
        {
            records[i] = (Record*)PyList_GET_ITEM(objects, i);
        }

        Py_BEGIN_ALLOW_THREADS

        for (i = 0; i < count; ++i)
        {
            Record* record = records[i];
            Py_ssize_t size = size_record(record);
            error = write_size(pos, max, size);

            if (error != ERR_NONE)
            {
                break;
            }

            error = write_record(pos, max, record);

            if (error != ERR_NONE)
            {
                break;
            }
        }

        Py_END_ALLOW_THREADS;

        CHECK(handle_write_error(error), error)
        PyMem_Free(records);
        records = NULL;
    }

    CHECK(handle_write_error(write_size(pos, max, 0)), error)
    return 1;

error:
    PyMem_Free(records);
    return 0;
}

/* Schema writing function dispatch table. */
static WriteSchemaTypeFunc write_schema_types[SDT_MAX] =
{
    write_nullable_schema,    /* SDT_NULLABLE */
    write_boolean_schema,     /* SDT_BOOLEAN */
    write_bytes_schema,       /* SDT_BYTES */
    write_double_schema,      /* SDT_DOUBLE */
    write_float_schema,       /* SDT_FLOAT */
    write_int_schema,         /* SDT_INT */
    write_long_schema,        /* SDT_LONG */
    write_bytes_schema,       /* SDT_STRING */
    write_array_schema,       /* SDT_ARRAY */
    write_map_schema,         /* SDT_MAP */
    write_record_schema,      /* SDT_RECORD */
    write_object_schema,      /* SDT_OBJECT */
    write_object_array_schema /* SDT_OBJECT_ARRAY */
};

/* Internal function to write a prepared object in Avro-encoded binary form
   into a buffer using a Schema object for encoding. The function signature is
   the same as for WriteSchemaTypeFunc above. */
static int write_schema(Schema* schema, uint8_t** pos, uint8_t* max, PyObject* value)
{
    return write_schema_types[schema->data_type](schema, pos, max, value);
}

/*----------------------------------------------------------------------------*/

/* Schema data type names. Used to populate schema_data_type_names tuple in
   module state during initialization. */
static char* schema_data_type_names[SDT_MAX] =
{
    "nullable",
    "boolean",
    "bytes",
    "double",
    "float",
    "int",
    "long",
    "string",
    "array",
    "map",
    "record",
    "object",
    "object_array"
};

/* File initialization: called during module initialization. */
int init_schema(PyObject* module)
{
    ProtocolState* state;

    int i;

    state = GET_STATE_MODULE(module);
    CHECK(state, error)

    CHECK(PyType_Ready(&Schema_type) == 0, error)

    state->schema_data_type_names = PyTuple_New(SDT_MAX);
    CHECK(state->schema_data_type_names, error)

    for (i = 0; i < SDT_MAX; ++i)
    {
        PyObject* name = PyUnicode_FromString(schema_data_type_names[i]);
        CHECK(name, error)
        CHECK(PyTuple_SetItem(state->schema_data_type_names, i, name) == 0, error)
    }

    Py_INCREF(&Schema_type);
    CHECK(PyModule_AddObject(module, "Schema", (PyObject*)&Schema_type) == 0, error)

    return 1;

error:
    return 0;
}
