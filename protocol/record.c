/*----------------------------------------------------------------------------*/
/* record.c: RecordColumn, RecordType, and Record Python classes and related  */
/* types.                                                                     */
/*----------------------------------------------------------------------------*/

#include "record.h"

#include "datetime.h"
#include "structmember.h"

#include "bufferrange.h"
#include "common.h"
#include "dt.h"
#include "protocol.h"

/*----------------------------------------------------------------------------*/

/* RecordColumn class implementation. */

/* Internal function that returns a tuple containing the values necessary to
   reconstruct a RecordColumn. Used for implementing __repr__. */
static PyObject* _RecordColumn_repr_object(RecordColumn* self)
{
    Py_ssize_t property_count;
    PyObject* tuple;

    Py_ssize_t i;

    property_count = PyTuple_GET_SIZE(self->properties);
    tuple = PyTuple_New(2 + property_count);
    CHECK(tuple, error)

    Py_INCREF(self->name);
    PyTuple_SET_ITEM(tuple, 0, self->name);

    Py_INCREF(self->data_type_name);
    PyTuple_SET_ITEM(tuple, 1, self->data_type_name);

    for (i = 0; i < property_count; ++i)
    {
        PyObject* property = PyTuple_GET_ITEM(self->properties, i);
        Py_INCREF(property);
        PyTuple_SET_ITEM(tuple, 2 + i, property);
    }

    return tuple;

error:
    return NULL;
}

/* Python RecordColumn object deallocator. */
static void RecordColumn_dealloc(RecordColumn* self)
{
    Py_XDECREF(self->name);
    Py_XDECREF(self->data_type_name);
    Py_XDECREF(self->properties);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/* Python RecordColumn object constructor.

   Parameters:
       name (unicode)
           Name of the column.

       data_type (unicode)
           Name of the data type of the column. Must be a valid data type.

       properties (sequence of unicode, optional)
           Additional column properties of the column. It is not necessary to
           specify a property if it is the same as data_type, since data type
           properties are managed automatically. The presence or absence of a
           "nullable" property is used to determine nullability of the column.

           If keyword parameters are not used, these can also be passed inline
           as extra parameters. */
static RecordColumn* RecordColumn_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    RecordColumn* self = NULL;
    PyObject* property_seq = NULL;

    Py_ssize_t arg_count;
    PyObject* arg_name;
    PyObject* arg_data_type;

    ProtocolState* state = GET_STATE();
    CHECK(state, error)

    self = (RecordColumn*)type->tp_alloc(type, 0);
    CHECK(self, error)

    arg_count = PyTuple_GET_SIZE(args);

    if (!kwargs && arg_count > 3)
    {
        arg_name = PyTuple_GET_ITEM(args, 0);
        arg_data_type = PyTuple_GET_ITEM(args, 1);
        property_seq = PyTuple_GetSlice(args, 2, arg_count);
        CHECK(property_seq, error)
    }
    else
    {
        PyObject* arg_properties = NULL;
        static char* keywords[] = { "name", "data_type", "properties", NULL };

        CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "OO|O", keywords, &arg_name, &arg_data_type, &arg_properties), error)

        if (arg_properties)
        {
            if (IS_STRING(arg_properties))
            {
                property_seq = PyTuple_Pack(1, arg_properties);
                CHECK(property_seq, error)
            }
            else
            {
                property_seq = PySequence_Fast(arg_properties, "properties must be iterable");
                CHECK(property_seq, error)
            }
        }
    }

    CHECK_STRING(IS_STRING(arg_name), PyExc_TypeError, "name must be string", error)
    self->name = TO_STRING(arg_name);
    CHECK(self->name, error)
    CHECK_STRING(!IS_STRING_EMPTY(self->name), PyExc_ValueError, "name must not be empty", error)

    CHECK_STRING(IS_STRING(arg_data_type), PyExc_TypeError, "data type must be string", error)
    self->data_type_name = TO_STRING(arg_data_type);
    CHECK(self->data_type_name, error)
    self->data_type = lookup_string(self->data_type_name, state->column_data_type_names, CDT_MAX);
    CHECK_OBJECT(self->data_type != CDT_MAX, PyExc_ValueError, format_string("unknown data type %S", self->data_type_name), error)

    if (property_seq)
    {
        Py_ssize_t property_count;
        Py_ssize_t i;

        property_count = PySequence_Fast_GET_SIZE(property_seq);
        self->properties = PyTuple_New(property_count);
        CHECK(self->properties, error)

        for (i = 0; i < property_count; ++i)
        {
            PyObject* property;
            int r;

            property = PySequence_Fast_GET_ITEM(property_seq, i);
            CHECK_STRING(IS_STRING(property), PyExc_TypeError, "property must be string", error)
            property = TO_STRING(property);
            CHECK(property, error)
            PyTuple_SET_ITEM(self->properties, i, property);

            if (!self->is_nullable)
            {
                r = PyObject_RichCompareBool(property, state->nullable_string, Py_EQ);
                CHECK(r >= 0, error)
                self->is_nullable = r;
            }
        }

        Py_DECREF(property_seq);
    }
    else
    {
        self->properties = PyTuple_New(0);
        CHECK(self->properties, error)
    }

    return self;

error:
    Py_XDECREF(self);
    Py_XDECREF(property_seq);
    return NULL;
}

/* Python RecordColumn.__repr__ method. */
static PyObject* RecordColumn_repr(RecordColumn* self)
{
    return generic_repr((PyObject*)self, (reprfunc)_RecordColumn_repr_object);
}

/* Python RecordColumn rich compare function (supports == and != operators.) */
static PyObject* RecordColumn_richcompare(PyObject* a, PyObject* b, int op)
{
    PyObject* result;
    int eq;

    result = generic_richcompare(&RecordColumn_type, a, b, op);

    if (result != (PyObject*)&RecordColumn_type)
    {
        return result;
    }

    eq = ((RecordColumn*)a)->data_type == ((RecordColumn*)b)->data_type
         && ((RecordColumn*)a)->is_nullable == ((RecordColumn*)b)->is_nullable;

    if (eq)
    {
        eq = PyObject_RichCompareBool(((RecordColumn*)a)->name, ((RecordColumn*)b)->name, Py_EQ);
        CHECK(eq != -1, error)

        if (eq)
        {
            eq = PyObject_RichCompareBool(((RecordColumn*)a)->properties, ((RecordColumn*)b)->properties, Py_EQ);
            CHECK(eq != -1, error)
        }
    }

    result = eq ? (op == Py_EQ ? Py_True : Py_False) : (op == Py_EQ ? Py_False : Py_True);
    Py_INCREF(result);
    return result;

error:
    return NULL;
}

static PyMemberDef RecordColumn_members[] =
{
    { "name", T_OBJECT_EX, offsetof(RecordColumn, name), READONLY, NULL },
    { "data_type", T_OBJECT_EX, offsetof(RecordColumn, data_type_name), READONLY, NULL },
    { "is_nullable", T_BOOL, offsetof(RecordColumn, is_nullable), READONLY, NULL },
    { "properties", T_OBJECT_EX, offsetof(RecordColumn, properties), READONLY, NULL },
    { NULL }
};

PyTypeObject RecordColumn_type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "kinetica.protocol.RecordColumn",      /* tp_name */
    sizeof(RecordColumn),                  /* tp_basicsize */
    0,                                     /* tp_itemsize */
    (destructor)RecordColumn_dealloc,      /* tp_dealloc */
    0,                                     /* tp_print */
    0,                                     /* tp_getattr */
    0,                                     /* tp_setattr */
    0,                                     /* tp_compare */
    (reprfunc)RecordColumn_repr,           /* tp_repr */
    0,                                     /* tp_as_number */
    0,                                     /* tp_as_sequence */
    0,                                     /* tp_as_mapping */
    0,                                     /* tp_hash */
    0,                                     /* tp_call */
    0,                                     /* tp_str */
    0,                                     /* tp_getattro */
    0,                                     /* tp_setattro */
    0,                                     /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                    /* tp_flags */
    0,                                     /* tp_doc */
    0,                                     /* tp_traverse */
    0,                                     /* tp_clear */
    (richcmpfunc)RecordColumn_richcompare, /* tp_richcompare */
    0,                                     /* tp_weaklistoffset */
    0,                                     /* tp_iter */
    0,                                     /* tp_iternext */
    0,                                     /* tp_methods */
    RecordColumn_members,                  /* tp_members */
    0,                                     /* tp_getset */
    0,                                     /* tp_base */
    0,                                     /* tp_dict */
    0,                                     /* tp_descr_get */
    0,                                     /* tp_descr_set */
    0,                                     /* tp_dictoffset */
    0,                                     /* tp_init */
    0,                                     /* tp_alloc */
    (newfunc)RecordColumn_new,             /* tp_new */
};

/*----------------------------------------------------------------------------*/

/* RecordType class implementation. */

/* Internal function that returns a tuple containing the values necessary to
   reconstruct a RecordType. Used for implementing __repr__. */
static PyObject* _RecordType_repr_object(RecordType* self)
{
    PyObject* tuple;

    Py_ssize_t column_count;
    PyObject* columns;

    Py_ssize_t i;

    tuple = PyTuple_New(2);
    CHECK(tuple, error)

    Py_INCREF(self->label);
    PyTuple_SET_ITEM(tuple, 0, self->label);

    column_count = Py_SIZE(self);
    columns = PyList_New(column_count);
    CHECK(columns, error)
    PyTuple_SET_ITEM(tuple, 1, columns);

    for (i = 0; i < column_count; ++i)
    {
        PyObject* column = _RecordColumn_repr_object((RecordColumn*)PyList_GET_ITEM(self->columns, i));
        CHECK(column, error)
        PyList_SET_ITEM(columns, i, column);
    }

    return tuple;

error:
    Py_XDECREF(tuple);
    return NULL;
}

/* Python RecordType contains function (supports the in operator). Returns
   true if the RecordType contains the provided key, which can be either a
   RecordColumn object or a unicode object containing a column name. */
static int RecordType_contains(RecordType* self, PyObject* key)
{
    if (PyObject_TypeCheck(key, &RecordColumn_type))
    {
        return PySequence_Contains(self->columns, key);
    }
    else
    {
        return PyDict_Contains(self->column_indices, key);
    }
}

/* Python RecordType object dellocator. */
static void RecordType_dealloc(RecordType* self)
{
    Py_XDECREF(self->label);
    Py_XDECREF(self->columns);
    Py_XDECREF(self->column_indices);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/* Forward declarations for decoding functions. */
static PyObject* RecordType_decode_dynamic_records(RecordType* self, PyObject* args, PyObject* kwargs);
static PyObject* RecordType_decode_records(RecordType* self, PyObject* args, PyObject* kwargs);

/* Python RecordType.from_dynamic_schema method. Creates a RecordType object
   from the Avro schema and binary data returned by a dynamic schema endpoint.

   Parameters:
       schema (unicode)
           The JSON Avro schema returned by the dynamic schema endpoint.

       buffer (buffer)
           The buffer containing the Avro-encoded binary data returned by the
           dynamic schema endpoint.

       range (BufferRange, optional)
           Range of bytes within the buffer containing the Avro-encoded binary
           data. If not specified, the entire buffer is used. If the data does
           not take up the entire range, any extra data is ignored.

   Returns:
       A RecordType object describing the dynamic schema. This can be used to
       decode the Avro-encoded binary data into Record objects using the
       RecordType.decode_dynamic_records method. */
static PyObject* RecordType_from_dynamic_schema(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    Py_buffer buffer = { NULL };
    PyObject* new_args = NULL;
    PyObject* parsed = NULL;
    PyObject* field_seq = NULL;
    PyObject* field_sub_seq = NULL;
    PyObject* field_name_set = NULL;
    PyObject* column_name_set = NULL;

    PyObject* arg_schema;
    PyObject* arg_range = NULL;
    static char* keywords[] = { "schema", "buffer", "range", NULL };

    Py_ssize_t field_count;
    PyObject* columns;
    PyObject* field;
    PyObject* field_type;
    PyObject* result;

    uint8_t* pos;
    uint8_t* max;
    PyObject* temp;
    int r;
    Py_ssize_t i;

    ProtocolState* state = GET_STATE();
    CHECK(state, error)

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "Os*|O", keywords, &arg_schema, &buffer, &arg_range), error)

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

    /* Create a tuple that will contain the arguments for the RecordType
       constructor (type label and list of column descriptions). */

    new_args = PyTuple_New(2);
    CHECK(new_args, error)

    /* Use an empty string as the type label. */

    temp = PyUnicode_FromStringAndSize(NULL, 0);
    CHECK(temp, error)
    PyTuple_SET_ITEM(new_args, 0, temp);

    /* Decode the JSON Avro schema into a parsed object tree and process it. */

    parsed = PyObject_CallFunctionObjArgs(state->json_decode, arg_schema, NULL);
    CHECK_STRING(parsed, PyExc_ValueError, "could not parse schema", error)
    CHECK_STRING(PyDict_Check(parsed), PyExc_ValueError, "schema must be object", error)

    temp = PyDict_GetItemString(parsed, "type");
    CHECK_STRING(temp, PyExc_ValueError, "schema has no type", error)
    r = PyObject_RichCompareBool(temp, state->record_string, Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "schema must be of type record", error)

    temp = PyDict_GetItemString(parsed, "fields");
    CHECK_STRING(temp, PyExc_ValueError, "schema has no fields", error)
    field_seq = PySequence_Fast(temp, "");
    CHECK_STRING(field_seq, PyExc_ValueError, "schema fields must be array", error)

    /* A dynamic schema always has two extra fields at the end containing
       arrays of column names and data types. Don't turn these into columns. */

    field_count = PySequence_Fast_GET_SIZE(field_seq) - 2;
    CHECK_STRING(field_count > 0, PyExc_ValueError, "schema must have at least 3 fields", error)

    /* Create the list of tuples that describe each column, and process the
       parsed JSON Avro schema to begin filling it out.

       The first item in each tuple is the column name; the JSON Avro schema
       does not actually contain this information (it uses dummy names), so
       this item is left null for now.

       The second item is the data type; the JSON Avro schema has the base Avro
       data types, which may not be the actual column data types but are
       sufficient to parse the binary data to get the actual ones.

       The third item is a list of column properties; the JSON Avro schema
       indicates whether or not the "nullable" property should be present, so
       this is added if needed. */

    columns = PyList_New(field_count);
    CHECK(columns, error)
    PyTuple_SET_ITEM(new_args, 1, columns);

    for (i = 0; i < field_count; ++i)
    {
        PyObject* column;
        char is_nullable = 0;
        PyObject* column_properties;
        int data_type;

        column = PyTuple_New(3);
        CHECK(column, error)
        PyList_SET_ITEM(columns, i, column);

        field = PySequence_Fast_GET_ITEM(field_seq, i);
        CHECK_OBJECT(PyDict_Check(field), PyExc_ValueError, format_string("field %zd must be object", i), error)

        /* For dynamic schemas, the type of each field must be an array of
           some other type. The other type is the column type. */

        field_type = PyDict_GetItemString(field, "type");
        CHECK_OBJECT(field_type, PyExc_ValueError, format_string("field %zd has no type", i), error)
        CHECK_OBJECT(PyDict_Check(field_type), PyExc_ValueError, format_string("field %zd type must be object", i), error)
        temp = PyDict_GetItemString(field_type, "type");
        CHECK_OBJECT(temp, PyExc_ValueError, format_string("field %zd type has no type", i), error)
        r = PyObject_RichCompareBool(temp, state->array_string, Py_EQ);
        CHECK(r >= 0, error)
        CHECK_OBJECT(r, PyExc_ValueError, format_string("field %zd must be of type array", i), error)
        field_type = PyDict_GetItemString(field_type, "items");
        CHECK_OBJECT(field_type, PyExc_ValueError, format_string("field %zd type has no items", i), error)

        /* If the column is nullable, the column type is represented in the
           Avro schema as an array containing the underlying column type and
           "null". */

        if (!IS_STRING(field_type))
        {
            field_sub_seq = PySequence_Fast(field_type, "");
            CHECK_OBJECT(field_sub_seq, PyExc_ValueError, format_string("field %zd type has invalid items", i), error)
            CHECK_OBJECT(PySequence_Fast_GET_SIZE(field_sub_seq) == 2, PyExc_ValueError, format_string("field %zd items union must have 2 types", i), error)
            r = PyObject_RichCompareBool(PySequence_Fast_GET_ITEM(field_sub_seq, 1), state->null_string, Py_EQ);
            CHECK(r >= 0, error)
            CHECK_OBJECT(r, PyExc_ValueError, format_string("field %zd items union must have null as second type", i), error)
            field_type = PySequence_Fast_GET_ITEM(field_sub_seq, 0);
            Py_CLEAR(field_sub_seq);
            is_nullable = 1;
        }

        Py_INCREF(field_type);
        PyTuple_SET_ITEM(column, 1, field_type);

        column_properties = PyList_New(0);
        CHECK(column_properties, error)
        PyTuple_SET_ITEM(column, 2, column_properties);

        /* If the column was determined to be nullable, add the "nullable"
           property. */

        if (is_nullable)
        {
            CHECK(PyList_Append(column_properties, state->nullable_string) == 0, error)
        }

        /* Make sure the data type is valid. */

        data_type = lookup_string(field_type, state->column_data_type_names, CDT_MAX);
        CHECK(data_type != -1, error)
        CHECK_OBJECT(data_type != CDT_MAX, PyExc_ValueError, format_string("field %zd has unknown data type %S", i, field_type), error)

        /* The Avro-encoded binary data returned by a dynamic schema endpoint
           contains the actual array of data for each column in order, followed
           by two additional arrays containing the column names and actual data
           types. Skip over the array of data for the column just processed;
           after all columns are processed, pos will point to the array of
           column names. */

        while (1)
        {
            Py_ssize_t count = 0;

            CHECK(handle_read_error(read_size(&pos, max, &count)), error)

            if (count == 0)
            {
                break;
            }

            if (count < 0)
            {
                PY_LONG_LONG size;

                CHECK(handle_read_error(read_long(&pos, max, &size)), error)

                if (pos + size > max)
                {
                    CHECK(handle_read_error(ERR_EOF), error)
                }

                pos += size;
                continue;
            }

            while (count > 0)
            {
                if (is_nullable)
                {
                    PY_LONG_LONG is_null;

                    CHECK(handle_read_error(read_long(&pos, max, &is_null)), error)

                    if (is_null == 1)
                    {
                        --count;
                        continue;
                    }
                    else if (is_null != 0)
                    {
                        CHECK(handle_read_error(ERR_OVERFLOW), error)
                    }
                }

                switch (data_type)
                {
                    case CDT_BYTES:
                    case CDT_STRING:
                        CHECK(handle_read_error(skip_bytes(&pos, max)), error)
                        break;

                    case CDT_DOUBLE:
                        CHECK(handle_read_error(skip_double(&pos, max)), error)
                        break;

                    case CDT_FLOAT:
                        CHECK(handle_read_error(skip_float(&pos, max)), error)
                        break;

                    case CDT_INT:
                        CHECK(handle_read_error(skip_int(&pos, max)), error)
                        break;

                    case CDT_LONG:
                        CHECK(handle_read_error(skip_long(&pos, max)), error)
                        break;

                    default:
                        CHECK_OBJECT(0, PyExc_ValueError, format_string("field %zd has invalid data type %S", i, field_type), error)
                        break;
                }

                --count;
            }
        }
    }

    /* Now that all columns have been processed, and the binary data for them
       skipped, process the column_headers field, which contains the column
       names. */

    field = PySequence_Fast_GET_ITEM(field_seq, field_count);
    CHECK_STRING(PyDict_Check(field), PyExc_ValueError, "column_headers field must be object", error)

    field_type = PyDict_GetItemString(field, "type");
    CHECK_STRING(field_type, PyExc_ValueError, "column_headers field has no type", error)
    CHECK_STRING(PyDict_Check(field_type), PyExc_ValueError, "column_headers field type must be object", error)
    temp = PyDict_GetItemString(field_type, "type");
    CHECK_STRING(temp, PyExc_ValueError, "column_headers field type has no type", error)
    r = PyObject_RichCompareBool(temp, state->array_string, Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "column_headers field must be of type array", error)
    field_type = PyDict_GetItemString(field_type, "items");
    CHECK_STRING(field_type, PyExc_ValueError, "column_headers field type has no items", error)
    r = PyObject_RichCompareBool(field_type, PyTuple_GET_ITEM(state->column_data_type_names, CDT_STRING), Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "column_headers field must be of type array of string", error)

    /* It is possible for there to be duplicate names, and these will need to
       be fixed later. For now, create a set of the unique names encountered to
       assist with this process. */

    field_name_set = PySet_New(NULL);
    CHECK(field_name_set, error)

    /* Read the Avro-encoded binary array of column names, and set the first
       item in each column tuple to what is read. After doing this, pos will
       point to the array of column data types. */

    i = 0;

    while (1)
    {
        Py_ssize_t count = 0;

        CHECK(handle_read_error(read_size(&pos, max, &count)), error)

        if (count == 0)
        {
            break;
        }

        if (count < 0)
        {
            PY_LONG_LONG size;

            CHECK(handle_read_error(read_long(&pos, max, &size)), error)
            count = -count;
        }

        while (count > 0)
        {
            Py_ssize_t len;
            char* data;

            CHECK_STRING(i < field_count, PyExc_ValueError, "column_headers field has too many values", error)
            CHECK(handle_read_error(read_bytes_len(&pos, max, &len)), error)
            data = (char*)MALLOC(len);
            CHECK(handle_read_error(data ? ERR_NONE : ERR_OOM), error)
            read_bytes_data(&pos, max, (uint8_t*)data, len);
            temp = PyUnicode_FromStringAndSize(data, len);
            free(data);
            CHECK(temp, error)
            PyTuple_SET_ITEM(PyList_GET_ITEM(columns, i), 0, temp);
            CHECK(PySet_Add(field_name_set, temp) == 0, error)
            ++i;
            --count;
        }
    }

    CHECK_STRING(i == field_count, PyExc_ValueError, "column_headers field has too few values", error)

    /* Loop through all the names that were read, and if any are not unique,
       mangle duplicates so that they are. As column names are confirmed, add
       them to a set of used names to check subsequent names against. */

    column_name_set = PySet_New(NULL);
    CHECK(column_name_set, error)

    for (i = 0; i < field_count; ++i)
    {
        PyObject* column;
        PyObject* name;
        Py_ssize_t n;

        column = PyList_GET_ITEM(columns, i);
        name = PyTuple_GET_ITEM(column, 0);
        r = PySet_Contains(column_name_set, name);
        CHECK(r >= 0, error)

        if (!r)
        {
            /* No column of the same name has already been used, so add it to
               the used column name set and continue. */

            CHECK(PySet_Add(column_name_set, name) == 0, error)
            continue;
        }

        /* There is already a column in the used column name set with the same
           name as the current column. The name needs to be mangled to add a
           _# to the end, where # is a number. Start with 2, and count up
           until a name is found that conflicts with neither another column
           name already used nor another name read from the original Avro
           binary-encoded array. (Checking both sets prevents using a name that
           conflicts with one the loop hasn't gotten to yet.) Replace the
           name in the column tuple with the new name and add it to the
           used column name set. */

        for (n = 2; ; ++n)
        {
            temp = format_string("%U_%zd", name, n);
            CHECK(temp, error)
            r = PySet_Contains(field_name_set, temp);

            if (!r)
            {
                r = PySet_Contains(column_name_set, temp);
            }

            if (!r)
            {
                Py_DECREF(name);
                PyTuple_SET_ITEM(column, 0, temp);
                CHECK(PySet_Add(column_name_set, temp) == 0, error)
                break;
            }

            Py_DECREF(temp);
            CHECK(r >= 0, error)
        }
    }

    Py_CLEAR(field_name_set);
    Py_CLEAR(column_name_set);

    /* Now that all column names have been processed, process the
       column_datatypes field, which contains the actual column data types. */

    field = PySequence_Fast_GET_ITEM(field_seq, field_count + 1);
    CHECK_STRING(PyDict_Check(field), PyExc_ValueError, "column_datatypes field must be object", error)

    field_type = PyDict_GetItemString(field, "type");
    CHECK_STRING(field_type, PyExc_ValueError, "column_datatypes field has no type", error)
    CHECK_STRING(PyDict_Check(field_type), PyExc_ValueError, "column_datatypes field type must be object", error)
    temp = PyDict_GetItemString(field_type, "type");
    CHECK_STRING(temp, PyExc_ValueError, "column_datatypes field type has no type", error)
    r = PyObject_RichCompareBool(temp, state->array_string, Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "column_datatypes field must be of type array", error)
    field_type = PyDict_GetItemString(field_type, "items");
    CHECK_STRING(field_type, PyExc_ValueError, "column_datatypes field type has no items", error)
    r = PyObject_RichCompareBool(field_type, PyTuple_GET_ITEM(state->column_data_type_names, CDT_STRING), Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "column_datatypes field must be of type array of string", error)

    /* Read the Avro-encoded binary array of column data types. */

    i = 0;

    while (1)
    {
        Py_ssize_t count = 0;

        CHECK(handle_read_error(read_size(&pos, max, &count)), error)

        if (count == 0)
        {
            break;
        }

        if (count < 0)
        {
            PY_LONG_LONG size;

            CHECK(handle_read_error(read_long(&pos, max, &size)), error)
            count = -count;
        }

        while (count > 0)
        {
            Py_ssize_t len;
            char* data;

            CHECK_STRING(i < field_count, PyExc_ValueError, "column_datatypes field has too many values", error)
            CHECK(handle_read_error(read_bytes_len(&pos, max, &len)), error)
            data = (char*)MALLOC(len);
            CHECK(handle_read_error(data ? ERR_NONE : ERR_OOM), error)
            read_bytes_data(&pos, max, (uint8_t*)data, len);
            temp = PyUnicode_FromStringAndSize(data, len);
            free(data);
            CHECK(temp, error)

            r = lookup_string(temp, state->column_data_type_names, CDT_MAX);

            if (r == -1)
            {
                Py_DECREF(temp);
                goto error;
            }

            if (r != CDT_MAX)
            {
                /* The data type is recognized as a column data type, so
                   replace the existing Avro data type in the column tuple with
                   the actual data type. */

                PyObject* column = PyList_GET_ITEM(columns, i);
                Py_DECREF(PyTuple_GET_ITEM(column, 1));
                PyTuple_SET_ITEM(column, 1, temp);
            }
            else
            {
                /* The data type is not recognized, so leave the existing Avro
                   data type in place. */

                Py_DECREF(temp);
            }

            ++i;
            --count;
        }
    }

    CHECK_STRING(i == field_count, PyExc_ValueError, "column_datatypes field has too few values", error)

    Py_DECREF(parsed);
    Py_DECREF(field_seq);

    /* Call the RecordType constructor, passing in the now-completed tuple of
       arguments. */

    result = PyObject_CallObject((PyObject*)type, new_args);
    Py_DECREF(new_args);
    return result;

error:
    if (buffer.buf)
    {
        PyBuffer_Release(&buffer);
    }

    Py_XDECREF(new_args);
    Py_XDECREF(parsed);
    Py_XDECREF(field_seq);
    Py_XDECREF(field_sub_seq);
    Py_XDECREF(field_name_set);
    Py_XDECREF(column_name_set);
    return NULL;
}

/* Python RecordType.from_type_schema method. Creates a RecordType object
   from the Avro schema and column properties that a Kinetica type comprises.

   Parameters:
       label (unicode)
           The label string of the type. For informational purposes only; may
           be an empty string.

       type_schema (unicode)
           The JSON Avro schema of the type.

       properties (mapping of unicode to sequence of unicode)
           Properties applicable to the columns in type_schema. Each mapping
           key is the name of a column, and each value is a sequence of
           properties applicable to that column. Columns with no special
           properties may be omitted.

   Returns:
       A RecordType object describing the type schema. This can be used to
       encode and decode Avro-encoded binary data for records of the type from
       and into Record objects using the Record.encode, Record.decode,
       Schema.encode and RecordType_decode_records methods. */
static PyObject* RecordType_from_type_schema(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* new_args = NULL;
    PyObject* parsed = NULL;
    PyObject* field_seq = NULL;
    PyObject* field_sub_seq = NULL;

    PyObject* arg_label;
    PyObject* arg_type_schema;
    PyObject* arg_properties;
    static char* keywords[] = { "label", "type_schema", "properties", NULL };

    Py_ssize_t field_count;
    PyObject* columns;
    PyObject* result;

    Py_ssize_t i;
    int r;
    PyObject* temp;

    ProtocolState* state = GET_STATE();
    CHECK(state, error)

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "OOO", keywords, &arg_label, &arg_type_schema, &arg_properties), error)
    CHECK_STRING(PyMapping_Check(arg_properties), PyExc_TypeError, "properties must be mapping", error)

    /* Create a tuple that will contain the arguments for the RecordType
       constructor (type label and list of column descriptions). */

    new_args = PyTuple_New(2);
    CHECK(new_args, error)

    /* Set the type label. */

    Py_INCREF(arg_label);
    PyTuple_SET_ITEM(new_args, 0, arg_label);

    /* Decode the JSON Avro schema into a parsed object tree and process it. */

    parsed = PyObject_CallFunctionObjArgs(state->json_decode, arg_type_schema, NULL);
    CHECK_STRING(parsed, PyExc_ValueError, "could not parse schema", error)
    CHECK_STRING(PyDict_Check(parsed), PyExc_ValueError, "schema must be object", error)

    temp = PyDict_GetItemString(parsed, "type");
    CHECK_STRING(temp, PyExc_ValueError, "schema has no type", error)
    r = PyObject_RichCompareBool(temp, state->record_string, Py_EQ);
    CHECK(r >= 0, error)
    CHECK_STRING(r, PyExc_ValueError, "schema must be of type record", error)

    temp = PyDict_GetItemString(parsed, "fields");
    CHECK_STRING(temp, PyExc_ValueError, "schema has no fields", error)
    field_seq = PySequence_Fast(temp, "");
    CHECK_STRING(field_seq, PyExc_ValueError, "schema fields must be array", error)
    field_count = PySequence_Fast_GET_SIZE(field_seq);
    CHECK_STRING(field_count > 0, PyExc_ValueError, "schema must have at least 1 field", error)

    /* Create the list of tuples that describe each column, and process the
       parsed JSON Avro schema to begin filling it out.

       The first item in each tuple is the column name, which is taken from the
       JSON Avro schema as is.

       The second item is the data type; the JSON Avro schema has the base Avro
       data types, which may not be the actual column data types since column
       properties may override them.

       The third item is a list of column properties; the JSON Avro schema
       indicates whether or not the "nullable" property should be present, so
       this is added if needed. */

    columns = PyList_New(field_count);
    CHECK(columns, error)
    PyTuple_SET_ITEM(new_args, 1, columns);

    for (i = 0; i < field_count; ++i)
    {
        PyObject* column;
        PyObject* field;
        PyObject* field_name;
        PyObject* field_type;
        char is_nullable = 0;
        PyObject* column_properties;
        char has_nullable_property = 0;

        column = PyTuple_New(3);
        CHECK(column, error)
        PyList_SET_ITEM(columns, i, column);

        field = PySequence_Fast_GET_ITEM(field_seq, i);
        CHECK_OBJECT(PyDict_Check(field), PyExc_ValueError, format_string("field %zd must be object", i), error)

        field_name = PyDict_GetItemString(field, "name");
        CHECK_OBJECT(field_name, PyExc_ValueError, format_string("field %zd has no name", i), error)
        CHECK_OBJECT(IS_STRING(field_name), PyExc_ValueError, format_string("field %zd has invalid name", i), error)
        Py_INCREF(field_name);
        PyTuple_SET_ITEM(column, 0, field_name);

        field_type = PyDict_GetItemString(field, "type");
        CHECK_OBJECT(field_type, PyExc_ValueError, format_string("field %S has no type", field_name), error)

        /* If the column is nullable, the column type is represented in the
           Avro schema as an array containing the underlying column type and
           "null". */

        if (!IS_STRING(field_type))
        {
            field_sub_seq = PySequence_Fast(field_type, "");
            CHECK_OBJECT(field_sub_seq, PyExc_ValueError, format_string("field %S has invalid type", field_name), error)
            CHECK_OBJECT(PySequence_Fast_GET_SIZE(field_sub_seq) == 2, PyExc_ValueError, format_string("field %S union must have 2 types", field_name), error)
            r = PyObject_RichCompareBool(PySequence_Fast_GET_ITEM(field_sub_seq, 1), state->null_string, Py_EQ);
            CHECK(r >= 0, error)
            CHECK_OBJECT(r, PyExc_ValueError, format_string("field %S union must have null as second type", field_name), error)
            field_type = PySequence_Fast_GET_ITEM(field_sub_seq, 0);
            Py_CLEAR(field_sub_seq);
            is_nullable = 1;
        }

        column_properties = PyList_New(0);
        CHECK(column_properties, error)
        PyTuple_SET_ITEM(column, 2, column_properties);

        /* If the column is contained in the properties mapping, add the
           specified properties to the column tuple. */

        if (PyMapping_HasKey(arg_properties, field_name))
        {
            Py_ssize_t field_property_count;
            Py_ssize_t j;

            temp = PyObject_GetItem(arg_properties, field_name);
            CHECK(temp, error)
            field_sub_seq = PySequence_Fast(temp, "");
            Py_DECREF(temp);
            CHECK_OBJECT(field_sub_seq, PyExc_TypeError, format_string("field %S properties must be iterable", field_name), error)
            field_property_count = PySequence_Fast_GET_SIZE(field_sub_seq);

            for (j = 0; j < field_property_count; ++j)
            {
                PyObject* field_property = PySequence_Fast_GET_ITEM(field_sub_seq, j);
                CHECK_OBJECT(IS_STRING(field_property), PyExc_TypeError, format_string("field %S property %zd is invalid", field_name, j), error)
                r = PyObject_RichCompareBool(field_property, state->nullable_string, Py_EQ);
                CHECK(r >= 0, error)

                if (r)
                {
                    has_nullable_property = 1;
                }
                else
                {
                    r = lookup_string(field_property, state->column_data_type_names, CDT_MAX);
                    CHECK(r >= 0, error)

                    if (r != CDT_MAX)
                    {
                        field_type = field_property;
                        continue;
                    }
                }

                CHECK(PyList_Append(column_properties, field_property) == 0, error)
            }

            Py_CLEAR(field_sub_seq);
        }

        /* If the column was determined to be nullable from the Avro schema,
           and a "nullable" property was not present, add it. */

        if (is_nullable && !has_nullable_property)
        {
            CHECK(PyList_Append(column_properties, state->nullable_string) == 0, error)
        }

        Py_INCREF(field_type);
        PyTuple_SET_ITEM(column, 1, field_type);
    }

    Py_DECREF(parsed);
    Py_DECREF(field_seq);

    /* Call the RecordType constructor, passing in the now-completed tuple of
       arguments. */

    result = PyObject_CallObject((PyObject*)type, new_args);
    Py_DECREF(new_args);
    return result;

error:
    Py_XDECREF(new_args);
    Py_XDECREF(parsed);
    Py_XDECREF(field_seq);
    Py_XDECREF(field_sub_seq);
    return NULL;
}

/* Python RecordType.index method. Determines the index of the specified
   RecordColumn or column name within the type.

   Parameters:
       key (RecordColumn or unicode)
           The RecordColumn or column name to determine the index of.

   Returns:
       The index of the column, if found. If not found, an exception is
       raised. */
static PyObject* RecordType_index(RecordType* self, PyObject* key)
{
    if (PyObject_TypeCheck(key, &RecordColumn_type))
    {
        Py_ssize_t index = PySequence_Index(self->columns, key);
        CHECK_OBJECT(index >= 0, PyExc_ValueError, format_string("column %S not found", ((RecordColumn*)key)->name), error)

        #if PY_MAJOR_VERSION >= 3
            return PyLong_FromSsize_t(index);
        #else
            return PyInt_FromSsize_t(index);
        #endif
    }
    else
    {
        PyObject* index = PyDict_GetItem(self->column_indices, key);
        CHECK_OBJECT(index, PyExc_ValueError, format_string("column %S not found", key), error)

        #if PY_MAJOR_VERSION >= 3
            return PyLong_FromSsize_t(((BufferRange*)index)->start);
        #else
            return PyInt_FromSsize_t(((BufferRange*)index)->start);
        #endif
    }

error:
    return NULL;
}

/* Python RecordType item function (supports the [] operator with an integer
   index). Returns the RecordColumn from the specified index. */
static PyObject* RecordType_item(RecordType* self, Py_ssize_t index)
{
    PyObject* result;

    CHECK_STRING(index >= 0 && index < Py_SIZE(self), PyExc_IndexError, "column index out of range", error)
    result = PyList_GET_ITEM(self->columns, index);
    Py_INCREF(result);
    return result;

error:
    return NULL;
}

/* Python RecordType.items method.

   Returns:
       A list of tuples, one for each column, each containing the column name
       and the corresponding RecordColumn object. */
static PyObject* RecordType_items(RecordType* self, void* unused)
{
    Py_ssize_t column_count;
    PyObject* result;

    Py_ssize_t i;

    column_count = Py_SIZE(self);
    result = PyList_New(column_count);
    CHECK(result, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* column = PyList_GET_ITEM(self->columns, i);
        PyObject* item = PyTuple_Pack(2, ((RecordColumn*)column)->name, column);
        CHECK(item, error)
        PyList_SET_ITEM(result, i, item);
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Python RecordType.__iter__ method. */
static PyObject* RecordType_iter(RecordType* self)
{
    return PyObject_GetIter(self->columns);
}

/* Python RecordType.keys method.

   Returns:
       A list of the names of the columns that the RecordType comprises. */
static PyObject* RecordType_keys(RecordType* self, void* unused)
{
    Py_ssize_t column_count;
    PyObject* result;

    Py_ssize_t i;

    column_count = Py_SIZE(self);
    result = PyList_New(column_count);
    CHECK(result, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* name = ((RecordColumn*)PyList_GET_ITEM(self->columns, i))->name;
        Py_INCREF(name);
        PyList_SET_ITEM(result, i, name);
    }

    return result;

error:
    return NULL;
}

/* Python RecordType.__len__ method. */
static Py_ssize_t RecordType_length(RecordType* self)
{
    return Py_SIZE(self);
}

/* Python RecordType object constructor.

   Parameters:
       label (unicode)
           The label string of the type. For information purposes only; may
           be an empty string.

       columns (sequence of RecordColumn or tuple)
           RecordColumn objects for columns that are part of the type. Tuples
           corresponding to the parameters of the RecordColumn constructor may
           also be used, in which case RecordColumn objects will be created
           automatically. */
static RecordType* RecordType_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    PyObject* label = NULL;
    PyObject* column_seq = NULL;
    PyObject* columns = NULL;
    PyObject* column_indices = NULL;

    PyObject* arg_label;
    PyObject* arg_columns;
    static char* keywords[] = { "label", "columns", NULL };

    Py_ssize_t column_count;
    RecordType* self;

    Py_ssize_t i;
    int r;
    PyObject* temp;

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "OO", keywords, &arg_label, &arg_columns), error)

    CHECK_STRING(IS_STRING(arg_label), PyExc_TypeError, "label must be string", error)
    label = TO_STRING(arg_label);
    CHECK(label, error)

    column_seq = PySequence_Fast(arg_columns, "columns must be iterable");
    CHECK(column_seq, error)
    column_count = PySequence_Fast_GET_SIZE(column_seq);
    CHECK_STRING(column_count > 0, PyExc_ValueError, "at least one column required", error)
    columns = PyList_New(column_count);
    CHECK(columns, error)
    column_indices = PyDict_New();
    CHECK(column_indices, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* column = PySequence_Fast_GET_ITEM(column_seq, i);

        if (PyTuple_Check(column))
        {
            column = PyObject_Call((PyObject*)&RecordColumn_type, column, NULL);
            CHECK(column, error)
        }
        else
        {
            CHECK_STRING(PyObject_TypeCheck(column, &RecordColumn_type), PyExc_TypeError, "column must be RecordColumn", error)
            Py_INCREF(column);
        }

        PyList_SET_ITEM(columns, i, column);

        r = PyDict_Contains(column_indices, ((RecordColumn*)column)->name);
        CHECK(r >= 0, error)
        CHECK_OBJECT(!r, PyExc_ValueError, format_string("duplicate column name %S", ((RecordColumn*)column)->name), error)

        temp = BufferRange_create(i, -1);
        CHECK(temp, error)
        r = PyDict_SetItem(column_indices, ((RecordColumn*)column)->name, temp);
        Py_DECREF(temp);
        CHECK(r == 0, error)
    }

    Py_CLEAR(column_seq);

    self = (RecordType*)type->tp_alloc(type, column_count);
    CHECK(self, error)

    self->label = label;
    self->columns = columns;
    self->column_indices = column_indices;

    for (i = 0; i < column_count; ++i)
    {
        RecordColumn* column = (RecordColumn*)PyList_GET_ITEM(columns, i);
        ColumnDef* column_def = &(&self->column_defs)[i];
        column_def->data_type = column->data_type;
        column_def->is_nullable = column->is_nullable;
    }

    return self;

error:
    Py_XDECREF(label);
    Py_XDECREF(column_seq);
    Py_XDECREF(columns);
    Py_XDECREF(column_indices);
    return NULL;
}

/* Python RecordType.__repr__ method. */
static PyObject* RecordType_repr(RecordType* self)
{
    return generic_repr((PyObject*)self, (reprfunc)_RecordType_repr_object);
}

/* Python RecordType rich compare function (supports == and != operators). */
static PyObject* RecordType_richcompare(PyObject* a, PyObject* b, int op)
{
    PyObject* result;
    int eq;

    result = generic_richcompare(&RecordType_type, a, b, op);

    if (result != (PyObject*)&RecordType_type)
    {
        return result;
    }

    eq = PyObject_RichCompareBool(((RecordType*)a)->label, ((RecordType*)b)->label, Py_EQ);
    CHECK(eq != -1, error)

    if (eq)
    {
        eq = PyObject_RichCompareBool(((RecordType*)a)->columns, ((RecordType*)b)->columns, Py_EQ);
        CHECK(eq != -1, error)
    }

    result = eq ? (op == Py_EQ ? Py_True : Py_False) : (op == Py_EQ ? Py_False : Py_True);
    Py_INCREF(result);
    return result;

error:
    return NULL;
}

/* Python RecordType subscript function (supports the [] operator with either
   an integer index, a slice object, or column name. Returns the appropriate
   RecordColumn or list of RecordColumns. */
static PyObject* RecordType_subscript(RecordType* self, PyObject* key)
{
    if (PyIndex_Check(key))
    {
        Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
        CHECK(index != -1 || !PyErr_Occurred(), error)

        if (index < 0)
        {
            index += Py_SIZE(self);
        }

        return RecordType_item(self, index);
    }
    else if (PySlice_Check(key))
    {
        return PyObject_GetItem(self->columns, key);
    }
    else
    {
        PyObject* index = PyDict_GetItem(self->column_indices, key);
        CHECK_OBJECT(index, PyExc_KeyError, key, error)
        return RecordType_item(self, ((BufferRange*)index)->start);
    }

error:
    return NULL;
}

/* Python RecordType.to_type_schema method. Creates a JSON Avro type schema
   and column property map for the type.

   Returns:
       A dict containing the type label string (key "label"), the JSON Avro
       schema string for the type (key "type_definition"), and a dict of
       column property lists (key "properties"). The dict directly corresponds
       to the Kinetica /create/type request and can be used in a call to
       that endpoint. */
static PyObject* RecordType_to_type_schema(RecordType* self, void* unused)
{
    PyObject* result = NULL;
    PyObject* type_schema = NULL;

    PyObject* properties;
    Py_ssize_t field_count;
    PyObject* fields;

    Py_ssize_t i;
    int r;
    PyObject* temp;

    ProtocolState* state = GET_STATE();
    CHECK(state, error)

    result = PyDict_New();
    CHECK(result, error)
    CHECK(PyDict_SetItem(result, state->label_string, self->label) == 0, error)

    properties = PyDict_New();
    CHECK(properties, error)
    r = PyDict_SetItem(result, state->properties_string, properties);
    Py_DECREF(properties);
    CHECK(r == 0, error)

    /* Create a dict representing the JSON Avro schema. */

    type_schema = PyDict_New();
    CHECK(type_schema, error)
    CHECK(PyDict_SetItemString(type_schema, "type", state->record_string) == 0, error)
    CHECK(PyDict_SetItemString(type_schema, "name", state->type_name_string) == 0, error)

    /* Build up the fields in the Avro schema, and simultaneously build up the
       column property lists. */

    field_count = Py_SIZE(self);
    fields = PyList_New(field_count);
    CHECK(fields, error)
    r = PyDict_SetItemString(type_schema, "fields", fields);
    Py_DECREF(fields);
    CHECK(r == 0, error)

    for (i = 0; i < field_count; ++i)
    {
        RecordColumn* column;
        ColumnDef* column_def;
        Py_ssize_t column_property_count;
        PyObject* column_properties;
        Py_ssize_t j;
        PyObject* field;
        PyObject* field_type;

        column = (RecordColumn*)PyList_GET_ITEM(self->columns, i);
        column_def = &(&self->column_defs)[i];
        column_property_count = PyTuple_GET_SIZE(column->properties);
        column_properties = PyList_New(column_property_count);
        CHECK(column_properties, error)
        r = PyDict_SetItem(properties, column->name, column_properties);
        Py_DECREF(column_properties);
        CHECK(r == 0, error)

        for (j = 0; j < column_property_count; ++j)
        {
            temp = PyTuple_GET_ITEM(column->properties, j);
            Py_INCREF(temp);
            PyList_SET_ITEM(column_properties, j, temp);
        }

        field = PyDict_New();
        CHECK(field, error)
        PyList_SET_ITEM(fields, i, field);
        CHECK(PyDict_SetItemString(field, "name", column->name) == 0, error)

        /* If the column has a non-Avro data type, replace the field type in
           the Avro schema with the appropriate Avro type, and append the
           Kinetica data type to the column properties list. */

        switch (column_def->data_type)
        {
            case CDT_CHAR1:
            case CDT_CHAR2:
            case CDT_CHAR4:
            case CDT_CHAR8:
            case CDT_CHAR16:
            case CDT_CHAR32:
            case CDT_CHAR64:
            case CDT_CHAR128:
            case CDT_CHAR256:
            case CDT_DATE:
            case CDT_DATETIME:
            case CDT_TIME:
                CHECK(PyList_Append(column_properties, column->data_type_name) == 0, error)
                field_type = PyTuple_GET_ITEM(state->column_data_type_names, CDT_STRING);
                break;

            case CDT_INT8:
            case CDT_INT16:
                CHECK(PyList_Append(column_properties, column->data_type_name) == 0, error)
                field_type = PyTuple_GET_ITEM(state->column_data_type_names, CDT_INT);
                break;

            case CDT_TIMESTAMP:
                CHECK(PyList_Append(column_properties, column->data_type_name) == 0, error)
                field_type = PyTuple_GET_ITEM(state->column_data_type_names, CDT_LONG);
                break;

            default:
                field_type = PyTuple_GET_ITEM(state->column_data_type_names, column_def->data_type);
                break;
        }

        /* If the column is nullable, the column type is represented in the
           Avro schema as an array containing the underlying column type and
           "null". */

        if (column_def->is_nullable)
        {
            temp = PyList_New(2);
            CHECK(temp, error)
            Py_INCREF(field_type);
            PyList_SET_ITEM(temp, 0, field_type);
            Py_INCREF(state->null_string);
            PyList_SET_ITEM(temp, 1, state->null_string);
            r = PyDict_SetItemString(field, "type", temp);
            Py_DECREF(temp);
            CHECK(r == 0, error)
        }
        else
        {
            CHECK(PyDict_SetItemString(field, "type", field_type) == 0, error)
        }
    }

    /* Encode the Avro schema dict into a JSON string. */

    temp = PyObject_CallFunctionObjArgs(state->json_encode, type_schema, NULL);
    CHECK(temp, error)
    r = PyDict_SetItem(result, state->type_definition_string, temp);
    Py_DECREF(temp);
    CHECK(r == 0, error)
    Py_DECREF(type_schema);
    return result;

error:
    Py_XDECREF(result);
    Py_XDECREF(type_schema);
    return NULL;
}

/* Python RecordType.values method.

   Returns:
       A list of the RecordColumns that the RecordType comprises. */
static PyObject* RecordType_values(RecordType* self, void* unused)
{
    return PyList_GetSlice(self->columns, 0, Py_SIZE(self));
}

static PyMappingMethods RecordType_as_mapping =
{
    (lenfunc)RecordType_length,       /* mp_length */
    (binaryfunc)RecordType_subscript, /* mp_subscript */
    0                                 /* mp_ass_subscript */
};

static PySequenceMethods RecordType_as_sequence =
{
    (lenfunc)RecordType_length,      /* sq_length */
    0,                               /* sq_concat */
    0,                               /* sq_repeat */
    (ssizeargfunc)RecordType_item,   /* sq_item */
    0,                               /* sq_slice */
    0,                               /* sq_ass_item */
    0,                               /* sq_ass_slice */
    (objobjproc)RecordType_contains, /* sq_contains */
    0,                               /* sq_inplace_concat */
    0                                /* sq_inplace_repeat */
};

static PyMemberDef RecordType_members[] =
{
    { "label", T_OBJECT_EX, offsetof(RecordType, label), READONLY, NULL },
    { NULL }
};

static PyMethodDef RecordType_methods[] =
{
    { "decode_dynamic_records", (PyCFunction)RecordType_decode_dynamic_records, METH_VARARGS | METH_KEYWORDS, NULL },
    { "decode_records", (PyCFunction)RecordType_decode_records, METH_VARARGS | METH_KEYWORDS, NULL },
    { "from_dynamic_schema", (PyCFunction)RecordType_from_dynamic_schema, METH_CLASS | METH_VARARGS | METH_KEYWORDS, NULL },
    { "from_type_schema", (PyCFunction)RecordType_from_type_schema, METH_CLASS | METH_VARARGS | METH_KEYWORDS, NULL },
    { "items", (PyCFunction)RecordType_items, METH_NOARGS, NULL },
    { "index", (PyCFunction)RecordType_index, METH_O, NULL },
    { "keys", (PyCFunction)RecordType_keys, METH_NOARGS, NULL },
    { "to_type_schema", (PyCFunction)RecordType_to_type_schema, METH_NOARGS, NULL },
    { "values", (PyCFunction)RecordType_values, METH_NOARGS, NULL },
    { NULL }
};

PyTypeObject RecordType_type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "kinetica.protocol.RecordType",         /* tp_name */
    sizeof(RecordType) - sizeof(ColumnDef), /* tp_basicsize */
    sizeof(ColumnDef),                      /* tp_itemsize */
    (destructor)RecordType_dealloc,         /* tp_dealloc */
    0,                                      /* tp_print */
    0,                                      /* tp_getattr */
    0,                                      /* tp_setattr */
    0,                                      /* tp_compare */
    (reprfunc)RecordType_repr,              /* tp_repr */
    0,                                      /* tp_as_number */
    &RecordType_as_sequence,                /* tp_as_sequence */
    &RecordType_as_mapping,                 /* tp_as_mapping */
    0,                                      /* tp_hash */
    0,                                      /* tp_call */
    0,                                      /* tp_str */
    0,                                      /* tp_getattro */
    0,                                      /* tp_setattro */
    0,                                      /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                     /* tp_flags */
    0,                                      /* tp_doc */
    0,                                      /* tp_traverse */
    0,                                      /* tp_clear */
    (richcmpfunc)RecordType_richcompare,    /* tp_richcompare */
    0,                                      /* tp_weaklistoffset */
    (getiterfunc)RecordType_iter,           /* tp_iter */
    0,                                      /* tp_iternext */
    RecordType_methods,                     /* tp_methods */
    RecordType_members,                     /* tp_members */
    0,                                      /* tp_getset */
    0,                                      /* tp_base */
    0,                                      /* tp_dict */
    0,                                      /* tp_descr_get */
    0,                                      /* tp_descr_set */
    0,                                      /* tp_dictoffset */
    0,                                      /* tp_init */
    0,                                      /* tp_alloc */
    (newfunc)RecordType_new,                /* tp_new */
};

/*----------------------------------------------------------------------------*/

/* Record column accessor and mutator function types. */

/* Function type for an accessor function that creates a Python value object
   for the raw value in a ColumnValue struct.

   For variable-length data types that may be larger than 8 bytes (bytes,
   string, and CharN with N > 8), this may free the internal buffer held in the
   ColumnValue struct and replace the buffer pointer with a pointer to the
   internal buffer of the newly created object; for this reason it is not safe
   to call a GetColumnFunc twice on the same ColumnValue struct unless the
   column has been cleared or the record re-decoded in between. */
typedef PyObject* (*GetColumnFunc)(ColumnValue*);

/* Function type for a mutator function that sets the value of a column in a
   record to a specified Python object value. This sets both the raw and Python
   object forms of the value, freeing any values already present. If an error
   (such as data type mismatch) occurs, the existing value is unchanged.

   record: The record containing the column to set.

   index: The index of the column to set. Must be valid.

   value: The Python object value to set the column to.

   If successful, 1 is returned; otherwise 0 is returned and an exception is
   set. */
typedef int (*SetColumnFunc)(Record*, Py_ssize_t, PyObject*);

/* Function type for a function that clears the value of a column in a record,
   freeing any associated buffers and objects.

   record: The record containing the column to clear.

   index: The index of the column to clear. Must be valid.

   from_python: A Boolean indicating whether the function is being called
                after the record has been touched by Python (versus only from C
                extension code). If false, it is assumed only raw values are
                present and no attempts are made to free Python object values,
                so it is safe to call without holding the GIL. */
typedef void (*ClearColumnFunc)(Record*, Py_ssize_t, int);

/*----------------------------------------------------------------------------*/

/* Forward declarations for Record column value accessor dispatch tables. */

static GetColumnFunc get_column[CDT_MAX];
static SetColumnFunc set_column[CDT_MAX];
static ClearColumnFunc clear_column[CDT_MAX];

/*----------------------------------------------------------------------------*/

/* Record class implementation. */

/* Internal function to return the Python object value from the column at the
   specified index. Assumes a valid index is provided. */
static PyObject* _Record_get_value(Record* self, Py_ssize_t index)
{
    /* Attempt to get the Python object value from the values list. */

    PyObject* result = PyList_GET_ITEM(self->values, index);

    /* If the value at the specified index is null, the raw value has not been
       converted into a Python value yet. Do the conversion and set the value
       in the values list for subsequent accesses. */

    if (!result)
    {
        ColumnValue* column_value = &(&self->column_values)[index];

        if (column_value->len < 0)
        {
            Py_INCREF(Py_None);
            result = Py_None;
        }
        else
        {
            result = get_column[(&self->type->column_defs)[index].data_type](column_value);
            CHECK(result, error)
        }

        PyList_SET_ITEM(self->values, index, result);
    }

    return result;

error:
    return NULL;
}

/* Internal function that returns a tuple containing the values necessary to
   reconstruct a Record. Used for implementing __repr__. */
static PyObject* _Record_repr_object(Record* self)
{
    PyObject* tuple;

    Py_ssize_t column_count;
    PyObject* values;

    Py_ssize_t i;

    tuple = PyTuple_New(2);
    CHECK(tuple, error)

    Py_INCREF(self->type);
    PyTuple_SET_ITEM(tuple, 0, (PyObject*)self->type);

    column_count = Py_SIZE(self);
    values = PyList_New(column_count);
    CHECK(values, error);
    PyTuple_SET_ITEM(tuple, 1, values);

    for (i = 0; i < column_count; ++i)
    {
        PyObject* value = _Record_get_value(self, i);
        CHECK(value, error)
        Py_INCREF(value);
        PyList_SET_ITEM(values, i, value);
    }

    return tuple;

error:
    Py_XDECREF(tuple);
    return NULL;
}

/* Internal function to set the column at the specified index to the specified
   Python object value. Assumes a valid index is provided. */
static int _Record_set_value(Record* self, Py_ssize_t index, PyObject* value)
{
    ColumnDef* column_def = &(&self->type->column_defs)[index];

    if (!value || value == Py_None)
    {
        CHECK_STRING(!value || column_def->is_nullable, PyExc_ValueError, "column is not nullable", error)
        clear_column[column_def->data_type](self, index, 1);
    }
    else
    {
        CHECK(set_column[column_def->data_type](self, index, value), error)
    }

    return 0;

error:
    prefix_exception(((RecordColumn*)PyList_GET_ITEM(self->type->columns, index))->name);
    return -1;
}

/* Internal function to set the Python object values of various columns based
   on a mapping of column names to values. */
static int _Record_set_mapping(Record* self, PyObject* values)
{
    PyObject* column_indices = self->type->column_indices;

    /* Due to values changing, clear cached record size. */

    self->size = 0;

    /* If the mapping is a dict, use the more efficient dict API. */

    if (PyDict_Check(values))
    {
        Py_ssize_t pos = 0;
        PyObject* key;
        PyObject* value;

        while (PyDict_Next(values, &pos, &key, &value))
        {
            PyObject* index = PyDict_GetItem(column_indices, key);
            CHECK_OBJECT(index, PyExc_ValueError, format_string("column %S not found", key), error_dict)
            CHECK(_Record_set_value(self, ((BufferRange*)index)->start, value) == 0, error_dict)
        }

        return 0;

    error_dict:
        return -1;
    }
    else
    {
        PyObject* keys = NULL;
        PyObject* key = NULL;
        PyObject* value = NULL;

        PyObject* temp;
        Py_ssize_t i;

        keys = PyMapping_Keys(values);
        CHECK(keys, error_iter)
        temp = PyObject_GetIter(keys);
        CHECK(temp, error_iter)
        Py_DECREF(keys);
        keys = temp;
        i = 0;

        while ((key = PyIter_Next(keys)))
        {
            PyObject* index = PyDict_GetItem(column_indices, key);
            CHECK_OBJECT(index, PyExc_ValueError, format_string("column %S not found", key), error_iter)
            value = PyObject_GetItem(values, key);
            CHECK(value, error_iter)
            CHECK(_Record_set_value(self, ((BufferRange*)index)->start, value) == 0, error_iter)
            Py_DECREF(key);
            Py_CLEAR(value);
            ++i;
        }

        CHECK(!PyErr_Occurred(), error_iter)
        Py_DECREF(keys);
        return 0;

    error_iter:
        Py_XDECREF(keys);
        Py_XDECREF(key);
        Py_XDECREF(value);
        return -1;
    }
}

/* Internal function the set the Python object values of columns to the values
   in an iterable object. Optionally skip the first value in the iterable (used
   when called during record initialization where the first value is the
   RecordType). */
static int _Record_set_sequence(Record* self, PyObject* values, char skip)
{
    Py_ssize_t column_count = Py_SIZE(self);

    /* Due to values changing, clear cached record size. */

    self->size = 0;

    /* If the iterable is a list or tuple, use the more efficient sequence
       API. */

    if (PyList_CheckExact(values) || PyTuple_CheckExact(values))
    {
        PyObject* value_seq = NULL;

        Py_ssize_t value_count;

        Py_ssize_t i;

        value_seq = PySequence_Fast(values, "values must be iterable");
        CHECK(value_seq, error_sequence)
        value_count = PySequence_Fast_GET_SIZE(value_seq);

        for (i = skip; i < value_count && i - skip < column_count; ++i)
        {
            CHECK(_Record_set_value(self, i - skip, PySequence_Fast_GET_ITEM(value_seq, i)) == 0, error_sequence)
        }

        CHECK_STRING(value_count - skip == column_count, PyExc_ValueError, "incorrect number of values", error_sequence)
        Py_DECREF(value_seq);
        return 0;

    error_sequence:
        Py_XDECREF(value_seq);
        return -1;
    }
    else
    {
        PyObject* value_iter = NULL;
        PyObject* value = NULL;

        Py_ssize_t i;

        value_iter = PyObject_GetIter(values);
        CHECK_STRING(value_iter, PyExc_TypeError, "values must be iterable", error_iter)
        i = 0;

        while ((value = PyIter_Next(value_iter)))
        {
            if (skip)
            {
                skip = 0;
                Py_DECREF(value);
                continue;
            }

            if (i == column_count)
            {
                Py_DECREF(value);
                break;
            }

            CHECK(_Record_set_value(self, i, value) == 0, error_iter)
            Py_DECREF(value);
            ++i;
        }

        CHECK(!PyErr_Occurred(), error_iter)
        CHECK_STRING(i == column_count, PyExc_ValueError, "incorrect number of values", error_iter)
        Py_DECREF(value_iter);
        return 0;

    error_iter:
        Py_XDECREF(value_iter);
        Py_XDECREF(value);
        return -1;
    }
}

/* Python Record.as_dict method.

   Returns:
       A dict mapping column names to the values of the record. */
static PyObject* Record_as_dict(Record* self, void* unused)
{
    PyObject* result;

    Py_ssize_t column_count;

    Py_ssize_t i;

    column_count = Py_SIZE(self);
    result = PyDict_New();
    CHECK(result, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* column = PyList_GET_ITEM(self->type->columns, i);
        PyObject* value = _Record_get_value(self, i);
        CHECK(value, error)
        CHECK(PyDict_SetItem(result, ((RecordColumn*)column)->name, value) == 0, error)
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Python Record item assignment function (supports the assignment to []
   operator with an integer index). */
static int Record_ass_item(Record* self, Py_ssize_t index, PyObject* value)
{
    CHECK_STRING(index >= 0 && index < Py_SIZE(self), PyExc_IndexError, "column index out of range", error)
    self->size = 0;
    return _Record_set_value(self, index, value);

error:
    return -1;
}

/* Python Record subscript assignment function (supports the assignment to []
   operator with either an integer index, a slice object, or column name). */
static int Record_ass_subscript(Record* self, PyObject* key, PyObject* value)
{
    if (PyIndex_Check(key))
    {
        Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
        CHECK(index != -1 || !PyErr_Occurred(), error_index)

        if (index < 0)
        {
            index += Py_SIZE(self);
        }

        return Record_ass_item(self, index, value);

    error_index:
        return -1;
    }
    else if (PySlice_Check(key))
    {
        PyObject* value_seq = NULL;

        Py_ssize_t start, stop, step, slicelength;

        Py_ssize_t i;
        Py_ssize_t index;

        #if PY_MAJOR_VERSION >= 3
            CHECK(PySlice_GetIndicesEx(key, Py_SIZE(self), &start, &stop, &step, &slicelength) == 0, error_slice)
        #else
            CHECK(PySlice_GetIndicesEx((PySliceObject*)key, Py_SIZE(self), &start, &stop, &step, &slicelength) == 0, error_slice)
        #endif

        self->size = 0;

        if (value == NULL)
        {
            for (i = 0, index = start; i < slicelength; ++i, index += step)
            {
                CHECK(_Record_set_value(self, index, NULL) == 0, error_slice)
            }
        }
        else
        {
            if (value == (PyObject*)self)
            {
                Py_ssize_t column_count = Py_SIZE(self);
                value_seq = PyList_New(column_count);
                CHECK(value_seq, error_slice)

                for (i = 0; i < column_count; ++i)
                {
                    PyObject* value = _Record_get_value(self, i);
                    CHECK(value, error_slice)
                    Py_INCREF(value);
                    PyList_SET_ITEM(value_seq, i, value);
                }
            }
            else
            {
                value_seq = PySequence_Fast(value, "assigned value must be iterable");
                CHECK(value_seq, error_slice)
            }

            CHECK_STRING(PySequence_Fast_GET_SIZE(value_seq) == slicelength, PyExc_ValueError, "assigned value must be same length as slice", error_slice)

            for (i = 0, index = start; i < slicelength; ++i, index += step)
            {
                CHECK(_Record_set_value(self, index, PySequence_Fast_GET_ITEM(value_seq, i)) == 0, error_slice)
            }

            Py_DECREF(value_seq);
        }

        return 0;

    error_slice:
        Py_XDECREF(value_seq);
        return -1;
    }
    else
    {
        PyObject* index = PyDict_GetItem(self->type->column_indices, key);
        CHECK_OBJECT(index, PyExc_KeyError, key, error_key)
        return Record_ass_item(self, ((BufferRange*)index)->start, value);

    error_key:
        return -1;
    }
}

/* Python Record object deallocator. */
static void Record_dealloc(Record* self)
{
    Py_ssize_t column_count;

    Py_ssize_t i;

    /* Clear all columns to make sure any allocated memory is freed. */

    column_count = Py_SIZE(self);

    for (i = 0; i < column_count; ++i)
    {
        clear_column[(&self->type->column_defs)[i].data_type](self, i, 1);
    }

    Py_XDECREF(self->type);
    Py_XDECREF(self->values);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

/* Python Record.decode method. Decodes the record from an Avro-encoded binary
   buffer, overwriting any existing values. The record must be of the same
   record type that the Record was initialized with.

   Parameters:
       buffer (buffer):
           The buffer containing the Avro-encoded binary data.

       range (BufferRange, optional):
           Range of bytes within the buffer containing the record. If not
           specified, the entire buffer is used. If the record's data does not
           take up the entire range, any extra data is ignored.

   Returns:
       The same Record instance on which the method was called. */
static PyObject* Record_decode(Record* self, PyObject* args, PyObject* kwargs)
{
    Py_buffer buffer = { NULL };

    PyObject* arg_range = NULL;
    static char* keywords[] = { "buffer", "range", NULL };

    Py_ssize_t column_count;

    uint8_t* pos;
    uint8_t* max;
    Py_ssize_t i;

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

    /* Clear existing values to make sure any allocated memory is freed and any
       Python object values are destroyed. */

    column_count = Py_SIZE(self);

    for (i = 0; i < column_count; ++i)
    {
        clear_column[(&self->type->column_defs)[i].data_type](self, i, 1);
    }

    CHECK(handle_read_error(read_record(&pos, max, self)), error)
    PyBuffer_Release(&buffer);
    Py_INCREF(self);
    return (PyObject*)self;

error:
    if (buffer.buf)
    {
        PyBuffer_Release(&buffer);
    }

    return NULL;
}

/* Python Record.encode method. Encodes the record into Avro-encoded binary
   format.

   Returns:
       Bytes (Python 3) or str (Python 2) containing the Avro-encoded binary
       data. */
static PyObject* Record_encode(Record* self, void* unused)
{
    PyObject* result = NULL;

    Py_ssize_t size;

    uint8_t* pos;
    uint8_t* max;

    size = size_record(self);

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
    CHECK(handle_write_error(write_record(&pos, max, self)), error)
    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Python Record object initializer. This assumes that the object has already
   been constructed and the record type set.

   Parameters:
       First parameter is ignored.

       Named parameters following the first are matched with column names and
       the values passed in are assigned to their corresponding columns.

       If named parameters are not used, a mapping or sequence can be passed
       as the second parameter, and this will be used to assign column values;
       alternatively, if there is more than one column, the column values can
       simply be passed in order as parameters directly. */
static int Record_init(Record* self, PyObject* args, PyObject* kwargs)
{
    if (kwargs)
    {
        CHECK_STRING(PyTuple_GET_SIZE(args) == 1, PyExc_TypeError, "positional arguments not supported with keywords", error)
        return _Record_set_mapping(self, kwargs);
    }
    else
    {
        Py_ssize_t arg_count = PyTuple_GET_SIZE(args);

        if (arg_count == 2)
        {
            PyObject* arg_1 = PyTuple_GET_ITEM(args, 1);

            if (PyTuple_Check(arg_1) || PyList_Check(arg_1))
            {
                return _Record_set_sequence(self, arg_1, 0);
            }
            else if (PyDict_Check(arg_1) || (PyMapping_Check(arg_1) && PyObject_HasAttrString(arg_1, "keys")))
            {
                return _Record_set_mapping(self, arg_1);
            }
            else
            {
                return _Record_set_sequence(self, args, 1);
            }
        }
        else if (arg_count > 2)
        {
            return _Record_set_sequence(self, args, 1);
        }
    }

    return 0;

error:
    return -1;
}

/* Python Record item function (supports the [] operator with an integer
   index). Returns the value of the column at the specified index. */
static PyObject* Record_item(Record* self, Py_ssize_t index)
{
    PyObject* result;

    CHECK_STRING(index >= 0 && index < Py_SIZE(self), PyExc_IndexError, "column index out of range", error)
    result = _Record_get_value(self, index);
    Py_XINCREF(result);
    return result;

error:
    return NULL;
}

/* Python Record.items method.

   Returns:
       A list of tuples, one for each column, each containing the column name
       and the column's value. */
static PyObject* Record_items(Record* self, void* unused)
{
    PyObject* result;

    Py_ssize_t column_count;

    Py_ssize_t i;

    column_count = Py_SIZE(self);
    result = PyList_New(column_count);
    CHECK(result, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* column;
        PyObject* value;
        PyObject* item;

        column = PyList_GET_ITEM(self->type->columns, i);
        value = _Record_get_value(self, i);
        CHECK(value, error)
        item = PyTuple_Pack(2, ((RecordColumn*)column)->name, value);
        CHECK(item, error)
        PyList_SET_ITEM(result, i, item);
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

/* Python Record.__iter__ method. */
static PyObject* Record_iter(Record* self)
{
    Py_ssize_t column_count;

    Py_ssize_t i;

    column_count = Py_SIZE(self);

    for (i = 0; i < column_count; ++i)
    {
        CHECK(_Record_get_value(self, i), error)
    }

    return PyObject_GetIter(self->values);

error:
    return NULL;
}

/* Python Record.keys method.

   Returns:
      A list of the names of the columns of the record. */
static PyObject* Record_keys(Record* self, void* unused)
{
    return RecordType_keys(self->type, unused);
}

/* Python Record.__len__ method. */
static Py_ssize_t Record_length(Record* self)
{
    return Py_SIZE(self);
}

/* Python Record object constructor. Note that this constructor only
   initializes the record type; initialization of column values is performed
   in the Record object initializer.

   Parameters:
       First parameter (RecordType)
           The record type.

       Subsequent parameters are ignored. */
static Record* Record_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    Record* self = NULL;

    PyObject* arg_type;

    Py_ssize_t column_count;

    Py_ssize_t i;

    CHECK_STRING(PyTuple_GET_SIZE(args) > 0, PyExc_TypeError, "RecordType required", error)
    arg_type = PyTuple_GET_ITEM(args, 0);
    CHECK_STRING(PyObject_TypeCheck(arg_type, &RecordType_type), PyExc_TypeError, "RecordType required", error)
    column_count = Py_SIZE(arg_type);
    self = (Record*)type->tp_alloc(type, column_count);
    CHECK(self, error)
    Py_INCREF(arg_type);
    self->type = (RecordType*)arg_type;

    for (i = 0; i < column_count; ++i)
    {
        (&self->column_values)[i].len = - (&self->type->column_defs)[i].is_nullable;
    }

    self->values = PyList_New(column_count);
    CHECK(self->values, error)
    return self;

error:
    Py_XDECREF(self);
    return NULL;
}

/* Python Record.__repr__ method. */
static PyObject* Record_repr(Record* self)
{
    return generic_repr((PyObject*)self, (reprfunc)_Record_repr_object);
}

/* Python Record rich compare function (supports == and != operators.) */
static PyObject* Record_richcompare(PyObject* a, PyObject* b, int op)
{
    PyObject* result;
    int eq;

    result = generic_richcompare(&Record_type, a, b, op);

    if (result != (PyObject*)&Record_type)
    {
        return result;
    }

    eq = PyObject_RichCompareBool((PyObject*)((Record*)a)->type, (PyObject*)((Record*)b)->type, Py_EQ);
    CHECK(eq != -1, error)

    if (eq)
    {
        Py_ssize_t column_count;
        Py_ssize_t i;

        column_count = Py_SIZE(a);

        for (i = 0; i < column_count; ++i)
        {
            PyObject* value_a;
            PyObject* value_b;

            value_a = _Record_get_value((Record*)a, i);
            CHECK(value_a, error)
            value_b = _Record_get_value((Record*)b, i);
            CHECK(value_b, error)
            eq = PyObject_RichCompareBool(value_a, value_b, Py_EQ);
            CHECK(eq != -1, error)

            if (!eq)
            {
                break;
            }
        }
    }

    result = eq ? (op == Py_EQ ? Py_True : Py_False) : (op == Py_EQ ? Py_False : Py_True);
    Py_INCREF(result);
    return result;

error:
    return NULL;
}

/* Python Record.size method. Calculates the size of the binary-encoded Avro
   form of the record in bytes, or returns the cached value if it was already
   calculated and no values have been changed.

   Returns:
       The size in bytes. */
static PyObject* Record_size(Record* self, void* unused)
{
    return PyLong_FromLongLong((PY_LONG_LONG)size_record(self));
}

/* Python RecordType subscript function (supports the [] operator with either
   an integer index, a slice object, or column name. Returns the appropriate
   column value or list of values. */
static PyObject* Record_subscript(Record* self, PyObject* key)
{
    if (PyIndex_Check(key))
    {
        Py_ssize_t index = PyNumber_AsSsize_t(key, PyExc_IndexError);
        CHECK(index != -1 || !PyErr_Occurred(), error_index)

        if (index < 0)
        {
            index += Py_SIZE(self);
        }

        return Record_item(self, index);

    error_index:
        return NULL;
    }
    else if (PySlice_Check(key))
    {
        PyObject* result = NULL;

        Py_ssize_t start, stop, step, slicelength;

        Py_ssize_t i;
        Py_ssize_t index;

        #if PY_MAJOR_VERSION >= 3
            CHECK(PySlice_GetIndicesEx(key, Py_SIZE(self), &start, &stop, &step, &slicelength) == 0, error_slice)
        #else
            CHECK(PySlice_GetIndicesEx((PySliceObject*)key, Py_SIZE(self), &start, &stop, &step, &slicelength) == 0, error_slice)
        #endif

        result = PyList_New(slicelength);
        CHECK(result, error_slice)

        for (i = 0, index = start; i < slicelength; ++i, index += step)
        {
            PyObject* value = _Record_get_value(self, index);
            CHECK(value, error_slice)
            Py_INCREF(value);
            PyList_SET_ITEM(result, i, value);
        }

        return result;

    error_slice:
        Py_XDECREF(result);
        return NULL;
    }
    else
    {
        PyObject* index;
        PyObject* value;

        index = PyDict_GetItem(self->type->column_indices, key);
        CHECK_OBJECT(index, PyExc_KeyError, key, error_key)
        value = _Record_get_value(self, ((BufferRange*)index)->start);
        CHECK(value, error_key)
        Py_INCREF(value);
        return value;

    error_key:
        return NULL;
    }
}

/* Python Record.update method. Assigns new values to the columns of the
   record in a batch.

   Parameters:
       Named parameters are matched with column names and the values assigned
       to their corresponding columns.

       If named parameters are not used, a mapping or sequence can be passed,
       and this will be used to assign column values; alternatively, if there
       is more than one column, the column values can simply be passed in order
       as parameters directly.

   Returns:
       The same record instance on which the method was called. */
static PyObject* Record_update(Record* self, PyObject* args, PyObject* kwargs)
{
    if (kwargs)
    {
        CHECK_STRING(PyTuple_GET_SIZE(args) == 0, PyExc_TypeError, "positional arguments not supported with keywords", error)
        CHECK(_Record_set_mapping(self, kwargs) == 0, error)
    }
    else if (PyTuple_GET_SIZE(args) == 1)
    {
        PyObject* arg_0 = PyTuple_GET_ITEM(args, 0);

        if (PyTuple_Check(arg_0) || PyList_Check(arg_0))
        {
            CHECK(_Record_set_sequence(self, arg_0, 0) == 0, error)
        }
        else if (PyDict_Check(arg_0) || (PyMapping_Check(arg_0) && PyObject_HasAttrString(arg_0, "keys")))
        {
            CHECK(_Record_set_mapping(self, arg_0) == 0, error)
        }
        else
        {
            CHECK(_Record_set_sequence(self, args, 0) == 0, error)
        }
    }
    else
    {
        CHECK(_Record_set_sequence(self, args, 0) == 0, error)
    }

    Py_INCREF(self);
    return (PyObject*)self;

error:
    return NULL;
}

/* Python Record.values method.

   Returns:
       A list of the column values of the record. */
static PyObject* Record_values(Record* self, void* unused)
{
    PyObject* result;

    Py_ssize_t column_count;

    Py_ssize_t i;

    column_count = Py_SIZE(self);
    result = PyList_New(column_count);
    CHECK(result, error)

    for (i = 0; i < column_count; ++i)
    {
        PyObject* value = _Record_get_value(self, i);
        CHECK(value, error)
        Py_INCREF(value);
        PyList_SET_ITEM(result, i, value);
    }

    return result;

error:
    Py_XDECREF(result);
    return NULL;
}

static PyMappingMethods Record_as_mapping =
{
    (lenfunc)Record_length,             /* mp_length */
    (binaryfunc)Record_subscript,       /* mp_subscript */
    (objobjargproc)Record_ass_subscript /* mp_ass_subscript */
};

static PySequenceMethods Record_as_sequence =
{
    (lenfunc)Record_length,           /* sq_length */
    0,                                /* sq_concat */
    0,                                /* sq_repeat */
    (ssizeargfunc)Record_item,        /* sq_item */
    0,                                /* sq_slice */
    (ssizeobjargproc)Record_ass_item, /* sq_ass_item */
    0,                                /* sq_ass_slice */
    0,                                /* sq_contains */
    0,                                /* sq_inplace_concat */
    0                                 /* sq_inplace_repeat */
};

static PyMemberDef Record_members[] =
{
    { "type", T_OBJECT_EX, offsetof(Record, type), READONLY, NULL },
    { NULL }
};

static PyMethodDef Record_methods[] =
{
    { "as_dict", (PyCFunction)Record_as_dict, METH_NOARGS, NULL },
    { "decode", (PyCFunction)Record_decode, METH_VARARGS | METH_KEYWORDS, NULL },
    { "encode", (PyCFunction)Record_encode, METH_NOARGS, NULL },
    { "items", (PyCFunction)Record_items, METH_NOARGS, NULL },
    { "keys", (PyCFunction)Record_keys, METH_NOARGS, NULL },
    { "size", (PyCFunction)Record_size, METH_NOARGS, NULL },
    { "update", (PyCFunction)Record_update, METH_VARARGS | METH_KEYWORDS, NULL },
    { "values", (PyCFunction)Record_values, METH_NOARGS, NULL },
    { NULL }
};

PyTypeObject Record_type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "kinetica.protocol.Record",           /* tp_name */
    sizeof(Record) - sizeof(ColumnValue), /* tp_basicsize */
    sizeof(ColumnValue),                  /* tp_itemsize */
    (destructor)Record_dealloc,           /* tp_dealloc */
    0,                                    /* tp_print */
    0,                                    /* tp_getattr */
    0,                                    /* tp_setattr */
    0,                                    /* tp_compare */
    (reprfunc)Record_repr,                /* tp_repr */
    0,                                    /* tp_as_number */
    &Record_as_sequence,                  /* tp_as_sequence */
    &Record_as_mapping,                   /* tp_as_mapping */
    0,                                    /* tp_hash */
    0,                                    /* tp_call */
    0,                                    /* tp_str */
    0,                                    /* tp_getattro */
    0,                                    /* tp_setattro */
    0,                                    /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                   /* tp_flags */
    0,                                    /* tp_doc */
    0,                                    /* tp_traverse */
    0,                                    /* tp_clear */
    (richcmpfunc)Record_richcompare,      /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    (getiterfunc)Record_iter,             /* tp_iter */
    0,                                    /* tp_iternext */
    Record_methods,                       /* tp_methods */
    Record_members,                       /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    (initproc)Record_init,                /* tp_init */
    0,                                    /* tp_alloc */
    (newfunc)Record_new,                  /* tp_new */
};

/* Internal function to create a Record object directly. Does not perform any
   initialization of columns to default values; it is assumed the caller will
   do this. */
static PyObject* Record_create(RecordType* type)
{
    Py_ssize_t column_count = Py_SIZE(type);
    Record* result = (Record*)Record_type.tp_alloc(&Record_type, column_count);
    CHECK(result, error)
    Py_INCREF(type);
    result->type = type;
    result->values = PyList_New(column_count);
    CHECK(result->values, error)
    return (PyObject*)result;

error:
    Py_XDECREF(result);
    return NULL;
}

/*----------------------------------------------------------------------------*/

/* Record column accessor functions. See description and notes at typedef for
   GetColumnFunc above. */

/* Accessor function for bytes columns. */
static PyObject* get_bytes_column(ColumnValue* column_value)
{
    PyObject* value;

    #if PY_MAJOR_VERSION >= 3
        value = PyBytes_FromStringAndSize(column_value->value.data, column_value->len);

        if (value)
        {
            free(column_value->value.data);
            column_value->value.data = PyBytes_AS_STRING(value);
        }
    #else
        value = PyString_FromStringAndSize(column_value->value.data, column_value->len);

        if (value)
        {
            free(column_value->value.data);
            column_value->value.data = PyString_AS_STRING(value);
        }
    #endif

    return value;
}

/* Accessor function for small charN columns (N <= 8). */
static PyObject* get_char_column_small(ColumnValue* column_value)
{
    return PyUnicode_FromStringAndSize(&column_value->value.c[0], column_value->len);
}

/* Accessor function for date columns. */
static PyObject* get_date_column(ColumnValue* column_value)
{
    long date = column_value->value.i;

    if (date == 0)
    {
        date = DATE_DEFAULT;
    }

    return PyDate_FromDate(DATE_YEAR(date),
                           DATE_MONTH(date),
                           DATE_DAY(date));
}

/* Accessor function for datetime columns. */
static PyObject* get_datetime_column(ColumnValue* column_value)
{
    PY_LONG_LONG datetime = column_value->value.l;

    if (datetime == 0)
    {
        datetime = DT_DEFAULT;
    }

    return PyDateTime_FromDateAndTime(DT_YEAR(datetime),
                                      DT_MONTH(datetime),
                                      DT_DAY(datetime),
                                      DT_HOUR(datetime),
                                      DT_MINUTE(datetime),
                                      DT_SEC(datetime),
                                      DT_MSEC(datetime) * 1000);
}

/* Accessor function for double columns. */
static PyObject* get_double_column(ColumnValue* column_value)
{
    return PyFloat_FromDouble(column_value->value.d);
}

/* Accessor function for float columns. */
static PyObject* get_float_column(ColumnValue* column_value)
{
    return PyFloat_FromDouble((double)column_value->value.f);
}

/* Accessor function for int columns. */
static PyObject* get_int_column(ColumnValue* column_value)
{
    #if PY_MAJOR_VERSION >= 3
        return PyLong_FromLong(column_value->value.i);
    #else
        return PyInt_FromLong(column_value->value.i);
    #endif
}

/* Accessor function for long columns. */
static PyObject* get_long_column(ColumnValue* column_value)
{
    return PyLong_FromLongLong(column_value->value.l);
}

/* Accessor function for string columns and large charN columns (N > 8). */
static PyObject* get_string_column(ColumnValue* column_value)
{
    PyObject* value;

    #if PY_MAJOR_VERSION >= 3
        value = PyUnicode_FromStringAndSize(column_value->value.data, column_value->len);

        if (value)
        {
            Py_ssize_t len;
            char* data;

            data = PyUnicode_AsUTF8AndSize(value, &len);

            if (data)
            {
                free(column_value->value.data);
                column_value->value.data = data;
                column_value->len = len;
            }
            else
            {
                Py_CLEAR(value);
            }
        }
    #else
        value = PyUnicode_FromStringAndSize(column_value->value.data, column_value->len);
    #endif

    return value;
}

/* Accessor function for time columns. */
static PyObject* get_time_column(ColumnValue* column_value)
{
    long time = column_value->value.i;

    return PyTime_FromTime(TIME_HOUR(time),
                           TIME_MINUTE(time),
                           TIME_SEC(time),
                           TIME_MSEC(time) * 1000);
}

/* Accessor function for timestamp columns. */
static PyObject* get_timestamp_column(ColumnValue* column_value)
{
    PY_LONG_LONG datetime = column_value->value.l;

    if (datetime == 0)
    {
        datetime = DT_DEFAULT;
    }

    return PyLong_FromLongLong(datetime_to_epoch_ms(datetime));
}

/* Column accessor function dispatch table. */
static GetColumnFunc get_column[CDT_MAX] =
{
    get_bytes_column,      /* CDT_BYTES */
    get_char_column_small, /* CDT_CHAR1 */
    get_char_column_small, /* CDT_CHAR2 */
    get_char_column_small, /* CDT_CHAR4 */
    get_char_column_small, /* CDT_CHAR8 */
    get_string_column,     /* CDT_CHAR16 */
    get_string_column,     /* CDT_CHAR32 */
    get_string_column,     /* CDT_CHAR64 */
    get_string_column,     /* CDT_CHAR128 */
    get_string_column,     /* CDT_CHAR256 */
    get_date_column,       /* CDT_DATE */
    get_datetime_column,   /* CDT_DATETIME */
    get_double_column,     /* CDT_DOUBLE */
    get_float_column,      /* CDT_FLOAT */
    get_int_column,        /* CDT_INT */
    get_int_column,        /* CDT_INT8 */
    get_int_column,        /* CDT_INT16 */
    get_long_column,       /* CDT_LONG */
    get_string_column,     /* CDT_STRING */
    get_time_column,       /* CDT_TIME */
    get_timestamp_column   /* CDT_TIMESTAMP */
};

/*----------------------------------------------------------------------------*/

/* Record column mutator functions. See description and notes at typedef for
   SetColumnFunc above.*/

/* Mutator function for bytes columns. */
static int set_bytes_column(Record* self, Py_ssize_t index, PyObject* value)
{
    Py_ssize_t len;
    char* data;
    ColumnValue* column_value;
    PyObject* old_value;

    #if PY_MAJOR_VERSION >= 3
        value = PyObject_Bytes(value);
        CHECK(value, error)
        len = PyBytes_GET_SIZE(value);
        data = PyBytes_AS_STRING(value);
    #else
        value = PyObject_Str(value);
        CHECK(value, error)
        len = PyString_GET_SIZE(value);
        data = PyString_AS_STRING(value);
    #endif

    column_value = &(&self->column_values)[index];
    old_value = PyList_GET_ITEM(self->values, index);

    if (!old_value)
    {
        free(column_value->value.data);
    }
    else
    {
        Py_DECREF(old_value);
    }

    PyList_SET_ITEM(self->values, index, value);
    column_value->value.data = data;
    column_value->len = len;
    return 1;

error:
    return 0;
}

/* Mutator helper function for small charN columns (N <= 8). */
static int set_char_column_small(Record* self, Py_ssize_t index, PyObject* value, int size)
{
    Py_ssize_t len;
    char* data;
    ColumnValue* column_value;

    #if PY_MAJOR_VERSION >= 3
        value = PyObject_Str(value);
        CHECK(value, error)
        data = PyUnicode_AsUTF8AndSize(value, &len);
        CHECK(data, error)
        CHECK_OBJECT(len <= size, PyExc_ValueError, format_string("maximum length %d exceeded", size), error)
        column_value = &(&self->column_values)[index];
        memcpy(&column_value->value.c, data, len);
    #else
        PyObject* string = NULL;

        value = PyObject_Unicode(value);
        CHECK(value, error)
        string = PyUnicode_AsUTF8String(value);
        CHECK(string, error)
        len = PyString_GET_SIZE(string);
        CHECK_OBJECT(len <= size, PyExc_ValueError, format_string("maximum length %d exceeded", size), error)
        data = PyString_AS_STRING(string);
        column_value = &(&self->column_values)[index];
        memcpy(&column_value->value.c, data, len);
        Py_DECREF(string);
    #endif

    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value->len = len;
    return 1;

error:
    #if PY_MAJOR_VERSION < 3
        Py_XDECREF(string);
    #endif

    Py_XDECREF(value);
    return 0;
}

/* Mutator function for char1 columns. */
static int set_char1_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_small(self, index, value, 1);
}

/* Mutator function for char2 columns. */
static int set_char2_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_small(self, index, value, 2);
}

/* Mutator function for char4 columns. */
static int set_char4_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_small(self, index, value, 4);
}

/* Mutator function for char8 columns. */
static int set_char8_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_small(self, index, value, 8);
}

/* Mutator helper function for large charN columns (N > 8). */
static int set_char_column_large(Record* self, Py_ssize_t index, PyObject* value, int size)
{
    Py_ssize_t len;
    char* data;
    ColumnValue* column_value;

    #if PY_MAJOR_VERSION >= 3
        PyObject* old_value;

        value = PyObject_Str(value);
        CHECK(value, error)
        data = PyUnicode_AsUTF8AndSize(value, &len);
        CHECK(data, error)
        CHECK_OBJECT(len <= size, PyExc_ValueError, format_string("maximum length %d exceeded", size), error)
        column_value = &(&self->column_values)[index];
        old_value = PyList_GET_ITEM(self->values, index);

        if (!old_value)
        {
            free(column_value->value.data);
        }
        else
        {
            Py_DECREF(old_value);
        }

        PyList_SET_ITEM(self->values, index, value);
        column_value->value.data = data;
    #else
        PyObject* string = NULL;

        value = PyObject_Unicode(value);
        CHECK(value, error)
        string = PyUnicode_AsUTF8String(value);
        CHECK(string, error)
        len = PyString_GET_SIZE(string);
        CHECK_OBJECT(len <= size, PyExc_ValueError, format_string("maximum length %d exceeded", size), error)
        data = PyString_AS_STRING(string);
        column_value = &(&self->column_values)[index];

        if (len != column_value->len)
        {
            char* temp = MALLOC(len);
            CHECK_NONE(temp, PyExc_MemoryError, error)
            free(column_value->value.data);
            column_value->value.data = temp;
        }

        memcpy(column_value->value.data, data, len);
        Py_DECREF(string);
        Py_XDECREF(PyList_GET_ITEM(self->values, index));
        PyList_SET_ITEM(self->values, index, value);
    #endif

    column_value->len = len;
    return 1;

error:
    #if PY_MAJOR_VERSION < 3
        Py_XDECREF(string);
    #endif

    Py_XDECREF(value);
    return 0;
}

/* Mutator function for char16 columns. */
static int set_char16_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_large(self, index, value, 16);
}

/* Mutator function for char32 columns. */
static int set_char32_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_large(self, index, value, 32);
}

/* Mutator function for char64 columns. */
static int set_char64_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_large(self, index, value, 64);
}

/* Mutator function for char128 columns. */
static int set_char128_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_large(self, index, value, 128);
}

/* Mutator function for char256 columns. */
static int set_char256_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_char_column_large(self, index, value, 256);
}

/* Mutator function for date columns. */
static int set_date_column(Record* self, Py_ssize_t index, PyObject* value)
{
    long date;
    ColumnValue* column_value;

    CHECK_STRING(PyDate_Check(value), PyExc_TypeError, "value must be date", error)
    CHECK_STRING(encode_date(PyDateTime_GET_YEAR(value),
                             PyDateTime_GET_MONTH(value),
                             PyDateTime_GET_DAY(value),
                             &date),
                 PyExc_ValueError, "value out of range, must be between 1/1/1000 and 12/31/2900", error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    Py_INCREF(value);
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.i = date;
    column_value->len = 0;
    return 1;

error:
    return 0;
}

/* Mutator function for datetime columns. */
static int set_datetime_column(Record* self, Py_ssize_t index, PyObject* value)
{
    PY_LONG_LONG datetime;
    ColumnValue* column_value;

    CHECK_STRING(PyDateTime_Check(value), PyExc_TypeError, "value must be datetime", error)
    CHECK_STRING(encode_datetime(PyDateTime_GET_YEAR(value),
                                 PyDateTime_GET_MONTH(value),
                                 PyDateTime_GET_DAY(value),
                                 PyDateTime_DATE_GET_HOUR(value),
                                 PyDateTime_DATE_GET_MINUTE(value),
                                 PyDateTime_DATE_GET_SECOND(value),
                                 PyDateTime_DATE_GET_MICROSECOND(value) / 1000,
                                 &datetime),
                 PyExc_ValueError, "value out of range, must be between 1/1/1000 and 12/31/2900", error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    Py_INCREF(value);
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.l = datetime;
    column_value->len = 0;
    return 1;

error:
    return 0;
}

/* Mutator function for double columns. */
static int set_double_column(Record* self, Py_ssize_t index, PyObject* value)
{
    ColumnValue* column_value;

    value = PyNumber_Float(value);
    CHECK(value, error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.d = PyFloat_AS_DOUBLE(value);
    column_value->len = 0;
    return 1;

error:
    return 0;
}

/* Mutator function for float columns. */
static int set_float_column(Record* self, Py_ssize_t index, PyObject* value)
{
    ColumnValue* column_value;

    value = PyNumber_Float(value);
    CHECK(value, error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.f = (float)PyFloat_AS_DOUBLE(value);
    column_value->len = 0;
    return 1;

error:
    return 0;
}

/* Mutator helper function for sized int columns. */
static int set_int_column_sized(Record* self, Py_ssize_t index, PyObject* value, long min, long max)
{
    ColumnValue* column_value;

    #if PY_MAJOR_VERSION >= 3
        long temp;

        value = PyNumber_Long(value);
        CHECK(value, error)
        temp = PyLong_AsLong(value);
        CHECK(temp != -1 || !PyErr_Occurred(), error)
        CHECK_STRING(temp >= min && temp <= max, PyExc_OverflowError, "value out of range", error)
    #else
        long temp;

        value = PyNumber_Int(value);
        CHECK(value, error)
        CHECK_STRING(PyInt_Check(value), PyExc_ValueError, "value out of range", error)
        temp = PyInt_AS_LONG(value);
        CHECK_STRING(temp >= min && temp <= max, PyExc_OverflowError, "value out of range", error)
    #endif

    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.i = temp;
    column_value->len = 0;
    return 1;

error:
    Py_XDECREF(value);
    return 0;
}

/* Mutator function for int columns. */
static int set_int_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_int_column_sized(self, index, value, INT32_MIN, INT32_MAX);
}

/* Mutator function for int8 columns. */
static int set_int8_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_int_column_sized(self, index, value, INT8_MIN, INT8_MAX);
}

/* Mutator function for int16 columns. */
static int set_int16_column(Record* self, Py_ssize_t index, PyObject* value)
{
    return set_int_column_sized(self, index, value, INT16_MIN, INT16_MAX);
}

/* Mutator function for long columns. */
static int set_long_column(Record* self, Py_ssize_t index, PyObject* value)
{
    ColumnValue* column_value;

    PY_LONG_LONG temp;

    value = PyNumber_Long(value);
    CHECK(value, error)
    temp = PyLong_AsLongLong(value);
    CHECK(temp != -1 || !PyErr_Occurred(), error)
    CHECK_STRING(temp >= INT64_MIN && temp <= INT64_MAX, PyExc_OverflowError, "value out of range", error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.l = temp;
    column_value->len = 0;
    return 1;

error:
    Py_XDECREF(value);
    return 0;
}

/* Mutator function for string columns. */
static int set_string_column(Record* self, Py_ssize_t index, PyObject* value)
{
    Py_ssize_t len;
    char* data;
    ColumnValue* column_value;

    #if PY_MAJOR_VERSION >= 3
        PyObject* old_value;

        value = PyObject_Str(value);
        CHECK(value, error)
        data = PyUnicode_AsUTF8AndSize(value, &len);
        CHECK(data, error)
        column_value = &(&self->column_values)[index];
        old_value = PyList_GET_ITEM(self->values, index);

        if (!old_value)
        {
            free(column_value->value.data);
        }
        else
        {
            Py_DECREF(old_value);
        }

        PyList_SET_ITEM(self->values, index, value);
        column_value->value.data = data;
    #else
        PyObject* string = NULL;

        value = PyObject_Unicode(value);
        CHECK(value, error)
        string = PyUnicode_AsUTF8String(value);
        CHECK(string, error)
        len = PyString_GET_SIZE(string);
        data = PyString_AS_STRING(string);
        column_value = &(&self->column_values)[index];

        if (len != column_value->len)
        {
            char* temp = MALLOC(len);
            CHECK_NONE(temp, PyExc_MemoryError, error)
            free(column_value->value.data);
            column_value->value.data = temp;
        }

        memcpy(column_value->value.data, data, len);
        Py_DECREF(string);
        Py_XDECREF(PyList_GET_ITEM(self->values, index));
        PyList_SET_ITEM(self->values, index, value);
    #endif

    column_value->len = len;
    return 1;

error:
    #if PY_MAJOR_VERSION < 3
        Py_XDECREF(string);
    #endif

    Py_XDECREF(value);
    return 0;
}

/* Mutator function for time columns. */
static int set_time_column(Record* self, Py_ssize_t index, PyObject* value)
{
    long time;
    ColumnValue* column_value;

    CHECK_STRING(PyTime_Check(value), PyExc_TypeError, "value must be time", error)
    encode_time(PyDateTime_TIME_GET_HOUR(value),
                PyDateTime_TIME_GET_MINUTE(value),
                PyDateTime_TIME_GET_SECOND(value),
                PyDateTime_TIME_GET_MICROSECOND(value) / 1000,
                &time);
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    Py_INCREF(value);
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.i = time;
    column_value->len = 0;
    return 1;

error:
    return 0;
}

/* Mutator function for timestamp columns. */
static int set_timestamp_column(Record* self, Py_ssize_t index, PyObject* value)
{
    ColumnValue* column_value;

    PY_LONG_LONG temp;

    value = PyNumber_Long(value);
    CHECK(value, error)
    temp = PyLong_AsLongLong(value);
    CHECK(temp != -1 || !PyErr_Occurred(), error)
    CHECK_STRING(temp >= MIN_EPOCH_MS && temp <= MAX_EPOCH_MS, PyExc_ValueError, "value out of range, must be between 1/1/1000 and 12/31/2900", error)
    Py_XDECREF(PyList_GET_ITEM(self->values, index));
    PyList_SET_ITEM(self->values, index, value);
    column_value = &(&self->column_values)[index];
    column_value->value.l = epoch_ms_to_datetime(temp);
    column_value->len = 0;
    return 1;

error:
    Py_XDECREF(value);
    return 0;
}

/* Column mutator function dispatch table. */
static SetColumnFunc set_column[CDT_MAX] =
{
    set_bytes_column,    /* CDT_BYTES */
    set_char1_column,    /* CDT_CHAR1 */
    set_char2_column,    /* CDT_CHAR2 */
    set_char4_column,    /* CDT_CHAR4 */
    set_char8_column,    /* CDT_CHAR8 */
    set_char16_column,   /* CDT_CHAR16 */
    set_char32_column,   /* CDT_CHAR32 */
    set_char64_column,   /* CDT_CHAR64 */
    set_char128_column,  /* CDT_CHAR128 */
    set_char256_column,  /* CDT_CHAR256 */
    set_date_column,     /* CDT_DATE */
    set_datetime_column, /* CDT_DATETIME */
    set_double_column,   /* CDT_DOUBLE */
    set_float_column,    /* CDT_FLOAT */
    set_int_column,      /* CDT_INT */
    set_int8_column,     /* CDT_INT8 */
    set_int16_column,    /* CDT_INT16 */
    set_long_column,     /* CDT_LONG */
    set_string_column,   /* CDT_STRING */
    set_time_column,     /* CDT_TIME */
    set_timestamp_column /* CDT_TIMESTAMP */
};

/*----------------------------------------------------------------------------*/

/* Record column clear functions. See description and notes at typedef for
   ClearColumnFunc above. */

/* Clear function for bytes columns. */
static void clear_bytes_column(Record* self, Py_ssize_t index, int from_python)
{
    ColumnValue* column_value = &(&self->column_values)[index];

    if (from_python)
    {
        PyObject* old_value = PyList_GET_ITEM(self->values, index);

        if (!old_value)
        {
            free(column_value->value.data);
        }
        else
        {
            Py_DECREF(old_value);
        }

        PyList_SET_ITEM(self->values, index, NULL);
    }
    else
    {
        free(column_value->value.data);
    }

    column_value->value.data = NULL;
    column_value->len = - (&self->type->column_defs)[index].is_nullable;
}

/* Clear function for non-variable length and small variable-length data types
   that do not require an external buffer outside of the ColumnValue struct. */
static void clear_simple_column(Record* self, Py_ssize_t index, int from_python)
{
    ColumnValue* column_value = &(&self->column_values)[index];

    if (from_python)
    {
        Py_XDECREF(PyList_GET_ITEM(self->values, index));
        PyList_SET_ITEM(self->values, index, NULL);
    }

    memset(&column_value->value, 0, sizeof(ColumnValueBase));
    column_value->len = - (&self->type->column_defs)[index].is_nullable;
}

/* Clear function for string columns. */
static void clear_string_column(Record* self, Py_ssize_t index, int from_python)
{
    #if PY_MAJOR_VERSION >= 3
        clear_bytes_column(self, index, from_python);
    #else
        ColumnValue* column_value = &(&self->column_values)[index];

        if (from_python)
        {
            Py_XDECREF(PyList_GET_ITEM(self->values, index));
            PyList_SET_ITEM(self->values, index, NULL);
        }

        free((&self->column_values)[index].value.data);
        column_value->value.data = NULL;
        column_value->len = - (&self->type->column_defs)[index].is_nullable;
    #endif
}

/* Column clear function dispatch table. */
static ClearColumnFunc clear_column[] =
{
    clear_bytes_column,  /* CDT_BYTES */
    clear_simple_column, /* CDT_CHAR1 */
    clear_simple_column, /* CDT_CHAR2 */
    clear_simple_column, /* CDT_CHAR4 */
    clear_simple_column, /* CDT_CHAR8 */
    clear_string_column, /* CDT_CHAR16 */
    clear_string_column, /* CDT_CHAR32 */
    clear_string_column, /* CDT_CHAR64 */
    clear_string_column, /* CDT_CHAR128 */
    clear_string_column, /* CDT_CHAR256 */
    clear_simple_column, /* CDT_DATE */
    clear_simple_column, /* CDT_DATETIME */
    clear_simple_column, /* CDT_DOUBLE */
    clear_simple_column, /* CDT_FLOAT */
    clear_simple_column, /* CDT_INT */
    clear_simple_column, /* CDT_INT8 */
    clear_simple_column, /* CDT_INT16 */
    clear_simple_column, /* CDT_LONG */
    clear_string_column, /* CDT_STRING */
    clear_simple_column, /* CDT_TIME */
    clear_simple_column  /* CDT_TIMESTAMP */
};

/*----------------------------------------------------------------------------*/

/* Record reading functions. */

/* Function type for a function that reads a column value from an Avro-encoded
   binary buffer into a ColumnValue struct as a raw value. All functions of
   this type take these parameters:

   pos: Pointer to a pointer to the start of the Avro-encoded binary data for
        the value being read within the buffer. This will be updated to point
        to the position immediately following the value data on return. If an
        error occurs, this may point to an arbitrary location between the
        initial value and max on return.

   max: Pointer to the end of the buffer containing the Avro-encoded binary
        value data (must be >= *pos, and may be beyond the end of the value
        data). If this is reached before the value is completely read,
        ERR_EOF is returned.

   column_value: Pointer to the ColumnValue struct into which to read the
                 value. The struct must not contain an existing value that
                 should be freed.

   An AvroErrorCode is returned indicating success (ERR_NONE) or failure. */
typedef AvroErrorCode (*ReadColumnFunc)(uint8_t**, uint8_t*, ColumnValue*);

/* Reading function for bytes columns. */
static AvroErrorCode read_bytes_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    Py_ssize_t len;
    char* data;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))
    data = (char*)MALLOC(len);

    if (!data)
    {
        return ERR_OOM;
    }

    read_bytes_data(pos, max, (uint8_t*)data, len);
    column_value->value.data = data;
    column_value->len = len;
    return ERR_NONE;
}

/* Reading helper function for small CharN columns (N <= 8). */
static AvroErrorCode read_char_column_small(uint8_t** pos, uint8_t* max, ColumnValue* column_value, int size)
{
    Py_ssize_t len;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))

    if (len > size)
    {
        return ERR_OVERFLOW;
    }

    read_bytes_data(pos, max, (uint8_t*)&column_value->value.c, len);
    column_value->len = len;
    return ERR_NONE;
}

/* Reading function for char1 columns. */
static AvroErrorCode read_char1_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_small(pos, max, column_value, 1);
}

/* Reading function for char2 columns. */
static AvroErrorCode read_char2_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_small(pos, max, column_value, 2);
}

/* Reading function for char4 columns. */
static AvroErrorCode read_char4_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_small(pos, max, column_value, 4);
}

/* Reading function for char8 columns. */
static AvroErrorCode read_char8_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_small(pos, max, column_value, 8);
}

/* Reading helper function for large CharN columns (N > 8). */
static AvroErrorCode read_char_column_large(uint8_t** pos, uint8_t* max, ColumnValue* column_value, int size)
{
    Py_ssize_t len;
    char* data;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))

    if (len > size)
    {
        return ERR_OVERFLOW;
    }

    data = (char*)MALLOC(len);

    if (!data)
    {
        return ERR_OOM;
    }

    read_bytes_data(pos, max, (uint8_t*)data, len);
    column_value->value.data = data;
    column_value->len = len;
    return ERR_NONE;
}

/* Reading function for char16 columns. */
static AvroErrorCode read_char16_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_large(pos, max, column_value, 16);
}

/* Reading function for char32 columns. */
static AvroErrorCode read_char32_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_large(pos, max, column_value, 32);
}

/* Reading function for char64 columns. */
static AvroErrorCode read_char64_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_large(pos, max, column_value, 64);
}

/* Reading function for char128 columns. */
static AvroErrorCode read_char128_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_large(pos, max, column_value, 128);
}

/* Reading function for char256 columns. */
static AvroErrorCode read_char256_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_char_column_large(pos, max, column_value, 256);
}

/* Reading function for date columns. */
static AvroErrorCode read_date_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    Py_ssize_t len;
    long year;
    long month;
    long day;
    long date;

    unsigned digits;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))
    max = *pos + len;
    skip_whitespace(pos, max, 0);
    AVRO_RETURN_ERROR(read_digits(pos, max, 4, 4, 1000, 2900, &year, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, '-'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 1, 12, &month, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, '-'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 1, 31, &day, &digits))
    skip_whitespace(pos, max, 0);

    if (*pos != max)
    {
        return ERR_OVERFLOW;
    }

    if (!encode_date(year, month, day, &date))
    {
        return ERR_OVERFLOW;
    }

    column_value->value.i = date;
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for datetime columns. */
static AvroErrorCode read_datetime_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    Py_ssize_t len;
    long year;
    long month;
    long day;
    long hour;
    long minute;
    long second;
    long millisecond;
    PY_LONG_LONG datetime;

    unsigned digits;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))
    max = *pos + len;
    skip_whitespace(pos, max, 0);
    AVRO_RETURN_ERROR(read_digits(pos, max, 4, 4, 1000, 2900, &year, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, '-'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 1, 12, &month, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, '-'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 1, 31, &day, &digits))

    if (*pos < max)
    {
        AVRO_RETURN_ERROR(skip_whitespace(pos, max, 1))
    }

    if (*pos == max)
    {
        if (!encode_datetime(year, month, day, 0, 0, 0, 0, &datetime))
        {
            return ERR_OVERFLOW;
        }

        column_value->value.l = datetime;
        column_value->len = 0;
        return ERR_NONE;
    }

    AVRO_RETURN_ERROR(read_digits(pos, max, 1, 2, 0, 23, &hour, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, ':'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 0, 59, &minute, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, ':'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 0, 59, &second, &digits))

    if (*pos < max && **pos == '.')
    {
        ++*pos;
        AVRO_RETURN_ERROR(read_digits(pos, max, 1, 6, 0, 999999, &millisecond, &digits))

        if (digits < 3)
        {
            if (digits == 2)
            {
                millisecond *= 10;
            }
            else
            {
                millisecond *= 100;
            }
        }
        else if (digits > 3)
        {
            if (digits == 4)
            {
                millisecond /= 10;
            }
            else if (digits == 5)
            {
                millisecond /= 100;
            }
            else
            {
                millisecond /= 1000;
            }
        }
    }
    else
    {
        millisecond = 0;
    }

    skip_whitespace(pos, max, 0);

    if (*pos != max)
    {
        return ERR_OVERFLOW;
    }

    if (!encode_datetime(year, month, day, hour, minute, second, millisecond, &datetime))
    {
        return ERR_OVERFLOW;
    }

    column_value->value.l = datetime;
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for double columns. */
static AvroErrorCode read_double_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    AVRO_RETURN_ERROR(read_double(pos, max, &column_value->value.d))
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for float columns. */
static AvroErrorCode read_float_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    AVRO_RETURN_ERROR(read_float(pos, max, &column_value->value.f))
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for int columns. */
static AvroErrorCode read_int_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    AVRO_RETURN_ERROR(read_int(pos, max, &column_value->value.i))
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading helper function for small int columns (<= int16). */
static AvroErrorCode read_int_column_small(uint8_t** pos, uint8_t* max, ColumnValue* column_value, long min_value, long max_value)
{
    long value;

    AVRO_RETURN_ERROR(read_int(pos, max, &value))

    if (value < min_value || value > max_value)
    {
        return ERR_OVERFLOW;
    }

    column_value->value.i = value;
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for int8 columns. */
static AvroErrorCode read_int8_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_int_column_small(pos, max, column_value, INT8_MIN, INT8_MAX);
}

/* Reading function for int16 columns. */
static AvroErrorCode read_int16_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return read_int_column_small(pos, max, column_value, INT16_MIN, INT16_MAX);
}

/* Reading function for long columns. */
static AvroErrorCode read_long_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    AVRO_RETURN_ERROR(read_long(pos, max, &column_value->value.l))
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for time columns. */
static AvroErrorCode read_time_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    Py_ssize_t len;
    long hour;
    long minute;
    long second;
    long millisecond;
    long time;

    unsigned digits;

    AVRO_RETURN_ERROR(read_bytes_len(pos, max, &len))
    max = *pos + len;
    skip_whitespace(pos, max, 0);
    AVRO_RETURN_ERROR(read_digits(pos, max, 1, 2, 0, 23, &hour, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, ':'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 0, 59, &minute, &digits))
    AVRO_RETURN_ERROR(skip_char(pos, max, ':'))
    AVRO_RETURN_ERROR(read_digits(pos, max, 2, 2, 0, 59, &second, &digits))

    if (*pos < max && **pos == '.')
    {
        ++*pos;
        AVRO_RETURN_ERROR(read_digits(pos, max, 1, 3, 0, 999999, &millisecond, &digits));

        if (digits < 3)
        {
            if (digits == 2)
            {
                millisecond *= 10;
            }
            else
            {
                millisecond *= 100;
            }
        }
    }
    else
    {
        millisecond = 0;
    }

    skip_whitespace(pos, max, 0);

    if (*pos != max)
    {
        return ERR_OVERFLOW;
    }

    encode_time(hour, minute, second, millisecond, &time);
    column_value->value.i = time;
    column_value->len = 0;
    return ERR_NONE;
}

/* Reading function for timestamp columns. */
static AvroErrorCode read_timestamp_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    PY_LONG_LONG value;

    AVRO_RETURN_ERROR(read_long(pos, max, &value))

    if (value < MIN_EPOCH_MS || value > MAX_EPOCH_MS)
    {
        return ERR_OVERFLOW;
    }

    column_value->value.l = epoch_ms_to_datetime(value);
    column_value->len = 0;
    return ERR_NONE;
}

/* Column reading function dispatch table. */
static ReadColumnFunc read_column[CDT_MAX] =
{
    read_bytes_column,    /* CDT_BYTES */
    read_char1_column,    /* CDT_CHAR1 */
    read_char2_column,    /* CDT_CHAR2 */
    read_char4_column,    /* CDT_CHAR4 */
    read_char8_column,    /* CDT_CHAR8 */
    read_char16_column,   /* CDT_CHAR16 */
    read_char32_column,   /* CDT_CHAR32 */
    read_char64_column,   /* CDT_CHAR64 */
    read_char128_column,  /* CDT_CHAR128 */
    read_char256_column,  /* CDT_CHAR256 */
    read_date_column,     /* CDT_DATE */
    read_datetime_column, /* CDT_DATETIME */
    read_double_column,   /* CDT_DOUBLE */
    read_float_column,    /* CDT_FLOAT */
    read_int_column,      /* CDT_INT */
    read_int8_column,     /* CDT_INT8 */
    read_int16_column,    /* CDT_INT16 */
    read_long_column,     /* CDT_LONG */
    read_bytes_column,    /* CDT_STRING */
    read_time_column,     /* CDT_TIME */
    read_timestamp_column /* CDT_TIMESTAMP */
};

/* Internal function to read an Avro-encoded record into an empty Record
   object. See description in header file. */
AvroErrorCode read_record(uint8_t** pos, uint8_t* max, Record* record)
{
    Py_ssize_t column_count;
    ColumnDef* column_defs;
    ColumnValue* column_values;

    Py_ssize_t i = 0;
    AvroErrorCode error;

    record->size = 0;
    column_count = Py_SIZE(record);
    column_defs = &record->type->column_defs;
    column_values = &record->column_values;

    for (; i < column_count; ++i)
    {
        ColumnDef* column_def = &column_defs[i];
        ColumnValue* column_value = &column_values[i];

        if (column_def->is_nullable)
        {
            PY_LONG_LONG is_null;

            error = read_long(pos, max, &is_null);

            if (error != ERR_NONE)
            {
                goto error;
            }

            if (is_null == 1)
            {
                column_value->len = -1;
                continue;
            }
            else if (is_null != 0)
            {
                error = ERR_OVERFLOW;
                goto error;
            }
        }

        error = read_column[column_def->data_type](pos, max, column_value);

        if (error != ERR_NONE)
        {
            goto error;
        }
    }

    return ERR_NONE;

error:
    for (; i > 0; --i)
    {
        clear_column[(&record->type->column_defs)[i].data_type](record, i, 0);
    }

    return error;
}

/*----------------------------------------------------------------------------*/

/* Record size computing functions. */

/* Function type for a function that calculates the size of the Avro-encoded
   binary form of the value in a ColumnValue struct in bytes. */
typedef Py_ssize_t (*SizeColumnFunc)(ColumnValue*);

/* Sizing function for bytes and other variable-length columns. */
static Py_ssize_t size_bytes_column(ColumnValue* column_value)
{
    return (Py_ssize_t)size_long(column_value->len) + column_value->len;
}

/* Sizing function for date columns. */
static Py_ssize_t size_date_column(ColumnValue* column_value)
{
    return 11;
}

/* Sizing function for datetime columns. */
static Py_ssize_t size_datetime_column(ColumnValue* column_value)
{
    return 24;
}

/* Sizing function for double columns. */
static Py_ssize_t size_double_column(ColumnValue* column_value)
{
    return 8;
}

/* Sizing function for float columns. */
static Py_ssize_t size_float_column(ColumnValue* column_value)
{
    return 4;
}

/* Sizing function for int columns. */
static Py_ssize_t size_int_column(ColumnValue* column_value)
{
    return size_long(column_value->value.i);
}

/* Sizing function for long columns. */
static Py_ssize_t size_long_column(ColumnValue* column_value)
{
    return size_long(column_value->value.l);
}

/* Sizing function for time columns. */
static Py_ssize_t size_time_column(ColumnValue* column_value)
{
    return 13;
}

/* Sizing function for timestamp columns. */
static Py_ssize_t size_timestamp_column(ColumnValue* column_value)
{
    return size_long(datetime_to_epoch_ms(column_value->value.l));
}

/* Column value sizing function dispatch table. */
static SizeColumnFunc size_column[CDT_MAX] =
{
    size_bytes_column,    /* CDT_BYTES */
    size_bytes_column,    /* CDT_CHAR1 */
    size_bytes_column,    /* CDT_CHAR2 */
    size_bytes_column,    /* CDT_CHAR4 */
    size_bytes_column,    /* CDT_CHAR8 */
    size_bytes_column,    /* CDT_CHAR16 */
    size_bytes_column,    /* CDT_CHAR32 */
    size_bytes_column,    /* CDT_CHAR64 */
    size_bytes_column,    /* CDT_CHAR128 */
    size_bytes_column,    /* CDT_CHAR256 */
    size_date_column,     /* CDT_DATE */
    size_datetime_column, /* CDT_DATETIME */
    size_double_column,   /* CDT_DOUBLE */
    size_float_column,    /* CDT_FLOAT */
    size_int_column,      /* CDT_INT */
    size_int_column,      /* CDT_INT8 */
    size_int_column,      /* CDT_INT16 */
    size_long_column,     /* CDT_LONG */
    size_bytes_column,    /* CDT_STRING */
    size_time_column,     /* CDT_TIME */
    size_timestamp_column /* CDT_TIMESTAMP */
};

/* Internal function to compute the size in bytes of the Avro-encoded binary
   form of a Record object. See description in header file. */
Py_ssize_t size_record(Record* record)
{
    Py_ssize_t column_count;
    ColumnDef* column_defs;
    ColumnValue* column_values;
    Py_ssize_t size = 0;

    Py_ssize_t i;

    if (record->size)
    {
        return record->size;
    }

    column_count = Py_SIZE(record);
    column_defs = &record->type->column_defs;
    column_values = &record->column_values;

    for (i = 0; i < column_count; ++i)
    {
        ColumnDef* column_def = &column_defs[i];
        ColumnValue* column_value = &column_values[i];

        if (column_def->is_nullable)
        {
            size += 1;

            if (column_value->len < 0)
            {
                continue;
            }
        }

        size += size_column[column_def->data_type](column_value);
    }

    record->size = size;
    return size;
}

/*----------------------------------------------------------------------------*/

/* Record writing functions. */

/* Function type for a function that writes a column value from a ColumnValue
   struct in Avro-encoded binary form into a buffer. All functions of this type
   take these parameters:

   pos: Pointer to a pointer to the position within the buffer to write the
        value. This will be updated on a successful write to point to the
        position immediately following the value data on return. If an
        error occurs, this may point to an arbitrary location between the
        initial value and max on return.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the value data is completely written, ERR_EOF is returned.

   column_value: Pointer to the ColumnValue struct from which to write the
                 value.

   An AvroErrorCode is returned indicating success (ERR_NONE) or failure. */
typedef int (*WriteColumnFunc)(uint8_t**, uint8_t*, ColumnValue*);

/* Writing function for bytes columns. */
static int write_bytes_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_bytes(pos, max, (uint8_t*)column_value->value.data, column_value->len);
}

/* Writing function for small CharN columns (N <= 8). */
static int write_char_column_small(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_bytes(pos, max, (uint8_t*)&column_value->value.c, column_value->len);
}

/* Writing function for date columns. */
static int write_date_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    long date;

    date = column_value->value.i;

    if (date == 0)
    {
        date = DATE_DEFAULT;
    }

    AVRO_RETURN_ERROR(write_size(pos, max, 10))
    AVRO_RETURN_ERROR(write_digits(pos, max, 4, DATE_YEAR(date)))
    AVRO_RETURN_ERROR(write_char(pos, max, '-'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DATE_MONTH(date)))
    AVRO_RETURN_ERROR(write_char(pos, max, '-'))
    return write_digits(pos, max, 2, DATE_DAY(date));
}

/* Writing function for datetime columns. */
static int write_datetime_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    PY_LONG_LONG datetime;

    datetime = column_value->value.l;

    if (datetime == 0)
    {
        datetime = DT_DEFAULT;
    }

    AVRO_RETURN_ERROR(write_size(pos, max, 23))
    AVRO_RETURN_ERROR(write_digits(pos, max, 4, DT_YEAR(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, '-'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DT_MONTH(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, '-'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DT_DAY(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, ' '))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DT_HOUR(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, ':'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DT_MINUTE(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, ':'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, DT_SEC(datetime)))
    AVRO_RETURN_ERROR(write_char(pos, max, '.'))
    return write_digits(pos, max, 3, DT_MSEC(datetime));
}

/* Writing function for double columns. */
static int write_double_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_double(pos, max, column_value->value.d);
}

/* Writing function for float columns. */
static int write_float_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_float(pos, max, column_value->value.f);
}

/* Writing function for int columns. */
static int write_int_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_int(pos, max, column_value->value.i);
}

/* Writing function for long columns. */
static int write_long_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    return write_long(pos, max, column_value->value.l);
}

/* Writing function for time columns. */
static int write_time_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    long time;

    time = column_value->value.i;
    AVRO_RETURN_ERROR(write_size(pos, max, 12))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, TIME_HOUR(time)))
    AVRO_RETURN_ERROR(write_char(pos, max, ':'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, TIME_MINUTE(time)))
    AVRO_RETURN_ERROR(write_char(pos, max, ':'))
    AVRO_RETURN_ERROR(write_digits(pos, max, 2, TIME_SEC(time)))
    AVRO_RETURN_ERROR(write_char(pos, max, '.'))
    return write_digits(pos, max, 3, TIME_MSEC(time));
}

/* Writing function for timestamp columns. */
static int write_timestamp_column(uint8_t** pos, uint8_t* max, ColumnValue* column_value)
{
    PY_LONG_LONG datetime = column_value->value.l;

    if (datetime == 0)
    {
        datetime = DT_DEFAULT;
    }

    return write_long(pos, max, datetime_to_epoch_ms(datetime));
}

/* Column writing function dispatch table. */
static WriteColumnFunc write_column[CDT_MAX] =
{
    write_bytes_column,      /* CDT_BYTES */
    write_char_column_small, /* CDT_CHAR1 */
    write_char_column_small, /* CDT_CHAR2 */
    write_char_column_small, /* CDT_CHAR4 */
    write_char_column_small, /* CDT_CHAR8 */
    write_bytes_column,      /* CDT_CHAR16 */
    write_bytes_column,      /* CDT_CHAR32 */
    write_bytes_column,      /* CDT_CHAR64 */
    write_bytes_column,      /* CDT_CHAR128 */
    write_bytes_column,      /* CDT_CHAR256 */
    write_date_column,       /* CDT_DATE */
    write_datetime_column,   /* CDT_DATETIME */
    write_double_column,     /* CDT_DOUBLE */
    write_float_column,      /* CDT_FLOAT */
    write_int_column,        /* CDT_INT */
    write_int_column,        /* CDT_INT8 */
    write_int_column,        /* CDT_INT16 */
    write_long_column,       /* CDT_LONG */
    write_bytes_column,      /* CDT_STRING */
    write_time_column,       /* CDT_TIME */
    write_timestamp_column   /* CDT_TIMESTAMP */
};

/* Internal function to write a Record object into a buffer in Avro-encoded
   binary form. See description in header file. */
AvroErrorCode write_record(uint8_t** pos, uint8_t* max, Record* record)
{
    Py_ssize_t column_count;
    ColumnDef* column_defs;
    ColumnValue* column_values;

    Py_ssize_t i;

    column_count = Py_SIZE(record);
    column_defs = &record->type->column_defs;
    column_values = &record->column_values;

    for (i = 0; i < column_count; ++i)
    {
        ColumnDef* column_def = &column_defs[i];
        ColumnValue* column_value = &column_values[i];

        if (column_def->is_nullable)
        {
            if (column_value->len < 0)
            {
                AVRO_RETURN_ERROR(write_long(pos, max, 1))
                continue;
            }
            else
            {
                AVRO_RETURN_ERROR(write_long(pos, max, 0))
            }
        }

        AVRO_RETURN_ERROR(write_column[column_def->data_type](pos, max, column_value))
    }

    return ERR_NONE;
}

/*----------------------------------------------------------------------------*/

/* RecordType forwarded methods. */

/* Python RecordType.decode_dynamic_records method. Decodes the records in the
   Avro-encoded binary data returned by a dynamic schema endpoint. The records
   must be of the correct record type.

   Parameters:
       buffer (buffer)
           The buffer containing the Avro-encoded binary data returned by the
           dynamic schema endpoint.

       range (BufferRange, optional)
           Range of bytes within the buffer containing the Avro-encoded binary
           data. If not specified, the entire buffer is used. If the data does
           not take up the entire range, any extra data is ignored.

   Returns:
       A list of decoded Record objects. */
static PyObject* RecordType_decode_dynamic_records(RecordType* self, PyObject* args, PyObject* kwargs)
{
    Py_buffer buffer = { NULL };
    PyObject* result = NULL;

    PyObject* arg_range = NULL;
    static char* keywords[] = { "buffer", "range", NULL };

    Py_ssize_t column_count;
    ColumnDef* column_defs;
    Py_ssize_t count;

    uint8_t* pos;
    uint8_t* max;
    Py_ssize_t block_count = 0;
    Py_ssize_t i;
    AvroErrorCode error;

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

    column_count = Py_SIZE(self);
    column_defs = &self->column_defs;

    CHECK(handle_read_error(read_size(&pos, max, &block_count)), error)
    count = (block_count >= 0) ? block_count : -block_count;

    /* Create all the Record objects that will be read into first, since this
       equires holding the GIL. Once created, the GIL can be released for the
       actual decoding process. */

    result = PyList_New(count);
    CHECK(result, error)

    for (i = 0; i < count; ++i)
    {
        PyObject* record = Record_create(self);
        CHECK(record, error)
        PyList_SET_ITEM(result, i, record);
    }

    Py_BEGIN_ALLOW_THREADS

    /* Dynamic schema responses encode data column-wise as arrays of values,
       so loop through the columns first. */

    for (i = 0; i < column_count; ++i)
    {
        ColumnDef* column_def = &column_defs[i];
        Py_ssize_t j = 0;

        /* Read the size of the first block of the column array, unless this
           is the first column, in which case the size was already read above
           to determine the number of Records to create. */

        if (i != 0)
        {
            error = read_size(&pos, max, &block_count);

            if (error != ERR_NONE)
            {
                Py_BLOCK_THREADS
                CHECK(handle_read_error(error), error)
            }
        }

        while (1)
        {
            if (block_count == 0)
            {
                break;
            }

            if (block_count < 0)
            {
                PY_LONG_LONG size;

                error = read_long(&pos, max, &size);

                if (error != ERR_NONE)
                {
                    Py_BLOCK_THREADS
                    CHECK(handle_read_error(error), error)
                }

                block_count = -block_count;
            }

            if (j + block_count > count)
            {
                /* The Avro data is in multiple array blocks, and the number of
                   array items in the current block exceeds the number of
                   Records created earlier. If this is the first column,
                   more Records must be created; if this is a subsequent
                   column, this is an error condition. In either case, the GIL
                   must be reacquired temporarily. */

                PyObject* temp;
                Py_ssize_t k;

                Py_BLOCK_THREADS

                CHECK_OBJECT(i == 0, PyExc_ValueError, format_string("column %zd has too many values", i), error)

                count = j + block_count;
                temp = PyList_New(count);
                CHECK(temp, error)

                for (k = 0; k < j; ++k)
                {
                    PyList_SET_ITEM(temp, k, PyList_GET_ITEM(result, k));
                    PyList_SET_ITEM(result, k, NULL);
                }

                Py_DECREF(result);
                result = temp;

                for (k = j; k < count; ++k)
                {
                    PyObject* record = Record_create(self);
                    CHECK(record, error)
                    PyList_SET_ITEM(result, k, record);
                }

                Py_UNBLOCK_THREADS
            }

            /* Now loop through each row in the column and read the value for
               the row into the appropriate column in the corresponding
               Record. */

            while (block_count > 0)
            {
                ColumnValue* column_value = &(&((Record*)PyList_GET_ITEM(result, j))->column_values)[i];

                if (column_def->is_nullable)
                {
                    PY_LONG_LONG is_null;

                    error = read_long(&pos, max, &is_null);

                    if (error != ERR_NONE)
                    {
                        Py_BLOCK_THREADS
                        CHECK(handle_read_error(error), error)
                    }

                    if (is_null == 1)
                    {
                        column_value->len = -1;
                        ++j;
                        --block_count;
                        continue;
                    }
                    else if (is_null != 0)
                    {
                        Py_BLOCK_THREADS
                        CHECK(handle_read_error(ERR_OVERFLOW), error)
                    }
                }

                error = read_column[column_def->data_type](&pos, max, column_value);

                if (error != ERR_NONE)
                {
                    Py_BLOCK_THREADS
                    CHECK(handle_read_error(error), error)
                }

                ++j;
                --block_count;
            }

            /* Read the size of the next block of the column array. The size
               is checked at the top of the loop. */

            error = read_size(&pos, max, &block_count);

            if (error != ERR_NONE)
            {
                Py_BLOCK_THREADS
                CHECK(handle_read_error(error), error)
            }
        }

        /* Make sure a value was read for every Record. */

        if (j != count)
        {
            Py_BLOCK_THREADS
            CHECK_OBJECT(0, PyExc_ValueError, format_string("column %zd has too few values", i), error)
        }
    }

    Py_END_ALLOW_THREADS

    PyBuffer_Release(&buffer);
    return result;

error:
    if (buffer.buf)
    {
        PyBuffer_Release(&buffer);
    }

    Py_XDECREF(result);
    return NULL;
}

/* Python RecordType.decode_records method. Decodes one or more Avro-encoded
   binary records in a buffer. The records must be of the correct record type.

   Parameters:
       buffer (buffer)
           The buffer containing the Avro-encoded binary records.

       ranges (BufferRange or iterable of BufferRange, optional)
           The ranges of bytes within the buffer corresponding to each
           Avro-encoded binary record to be decoded. If only one record is
           present in the buffer, a single BufferRange can be passed. If not
           specified, it is assumed that the buffer contains a single record to
           decode.

   Returns:
       A list of decoded Record objects. */
static PyObject* RecordType_decode_records(RecordType* self, PyObject* args, PyObject* kwargs)
{
    Py_buffer buffer = { NULL };
    PyObject* ranges_seq = NULL;
    Py_ssize_t* ranges = NULL;
    PyObject* result = NULL;

    PyObject* arg_ranges = NULL;
    static char* keywords[] = { "buffer", "ranges", NULL };

    Py_ssize_t count;
    Py_ssize_t* starts;
    Py_ssize_t* lengths;

    Py_ssize_t i;

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "s*|O", keywords, &buffer, &arg_ranges), error)

    if (arg_ranges)
    {
        if (BufferRange_check(arg_ranges))
        {
            Py_ssize_t start;
            Py_ssize_t length;

            start = ((BufferRange*)arg_ranges)->start;
            CHECK_STRING(start >= 0 && start <= buffer.len, PyExc_ValueError, "start index out of range", error)
            length = ((BufferRange*)arg_ranges)->length;
            CHECK_STRING(length >= 0 && start + length <= buffer.len, PyExc_ValueError, "length out of range", error)
            count = 1;
            ranges = PyMem_New(Py_ssize_t, 2);
            CHECK_NONE(ranges, PyExc_MemoryError, error)
            starts = ranges;
            *starts = start;
            lengths = &ranges[1];
            *lengths = length;
        }
        else
        {
            ranges_seq = PySequence_Fast(arg_ranges, "ranges must be BufferRange or iterable");
            CHECK(ranges_seq, error)
            count = PySequence_Fast_GET_SIZE(ranges_seq);
            ranges = PyMem_New(Py_ssize_t, count * 2);
            CHECK_NONE(ranges, PyExc_MemoryError, error)
            starts = ranges;
            lengths = &ranges[count];

            for (i = 0; i < count; ++i)
            {
                PyObject* range;
                Py_ssize_t start;
                Py_ssize_t length;

                range = PySequence_Fast_GET_ITEM(ranges_seq, i);
                CHECK_STRING(BufferRange_check(range), PyExc_TypeError, "range must be BufferRange", error)
                start = ((BufferRange*)range)->start;
                CHECK_STRING(start >= 0 && start <= buffer.len, PyExc_ValueError, "start index out of range", error)
                length = ((BufferRange*)range)->length;
                CHECK_STRING(length >= 0 && start + length <= buffer.len, PyExc_ValueError, "length out of range", error)
                starts[i] = start;
                lengths[i] = length;
            }

            Py_CLEAR(ranges_seq);
        }
    }
    else
    {
        count = 1;
        ranges = PyMem_New(Py_ssize_t, 2);
        CHECK_NONE(ranges, PyExc_MemoryError, error)
        starts = ranges;
        *starts = 0;
        lengths = &ranges[1];
        *lengths = buffer.len;
    }

    /* Create all the Record objects that will be read into first, since this
       requires holding the GIL. Once created, the GIL can be released for the
       actual decoding process. */

    result = PyList_New(count);
    CHECK(result, error)

    for (i = 0; i < count; ++i)
    {
        PyObject* record = Record_create(self);
        CHECK(record, error)
        PyList_SET_ITEM(result, i, record);
    }

    Py_BEGIN_ALLOW_THREADS

    /* Read each record. */

    for (i = 0; i < count; ++i)
    {
        uint8_t* pos = (uint8_t*)buffer.buf + starts[i];
        uint8_t* max = pos + lengths[i];
        AvroErrorCode error = read_record(&pos, max, (Record*)PyList_GET_ITEM(result, i));

        if (error != ERR_NONE)
        {
            Py_BLOCK_THREADS
            CHECK(handle_read_error(error), error)
        }
    }

    Py_END_ALLOW_THREADS

    PyBuffer_Release(&buffer);
    PyMem_Free(ranges);
    return result;

error:
    if (buffer.buf)
    {
        PyBuffer_Release(&buffer);
    }

    Py_XDECREF(ranges_seq);
    PyMem_Free(ranges);
    Py_XDECREF(result);
    return NULL;
}

/*----------------------------------------------------------------------------*/

/* Record column data type names. Used to populate column_data_type_names tuple
   in module state during initialization. */
static char* column_data_type_names[CDT_MAX] =
{
    "bytes",
    "char1",
    "char2",
    "char4",
    "char8",
    "char16",
    "char32",
    "char64",
    "char128",
    "char256",
    "date",
    "datetime",
    "double",
    "float",
    "int",
    "int8",
    "int16",
    "long",
    "string",
    "time",
    "timestamp"
};

/* File initialization: called during module initialization. */
int init_record(PyObject* module)
{
    ProtocolState* state;

    int i;

    state = GET_STATE_MODULE(module);
    CHECK(state, error)

    CHECK(PyType_Ready(&RecordColumn_type) == 0, error)
    CHECK(PyType_Ready(&RecordType_type) == 0, error)
    CHECK(PyType_Ready(&Record_type) == 0, error)

    PyDateTime_IMPORT;

    state->array_string = PyUnicode_FromString("array");
    CHECK(state->array_string, error)

    state->label_string = PyUnicode_FromString("label");
    CHECK(state->label_string, error)

    state->null_string = PyUnicode_FromString("null");
    CHECK(state->null_string, error)

    state->nullable_string = PyUnicode_FromString("nullable");
    CHECK(state->nullable_string, error)

    state->properties_string = PyUnicode_FromString("properties");
    CHECK(state->properties_string, error)

    state->record_string = PyUnicode_FromString("record");
    CHECK(state->record_string, error)

    state->type_definition_string = PyUnicode_FromString("type_definition");
    CHECK(state->type_definition_string, error)

    state->type_name_string = PyUnicode_FromString("type_name");
    CHECK(state->type_name_string, error)

    state->column_data_type_names = PyTuple_New(CDT_MAX);
    CHECK(state->column_data_type_names, error)

    for (i = 0; i < CDT_MAX; ++i)
    {
        PyObject* name = PyUnicode_FromString(column_data_type_names[i]);
        CHECK(name, error)
        CHECK(PyTuple_SetItem(state->column_data_type_names, i, name) == 0, error)
    }

    Py_INCREF(&RecordColumn_type);
    CHECK(PyModule_AddObject(module, "RecordColumn", (PyObject*)&RecordColumn_type) == 0, error)

    Py_INCREF(&RecordType_type);
    CHECK(PyModule_AddObject(module, "RecordType", (PyObject*)&RecordType_type) == 0, error)

    Py_INCREF(&Record_type);
    CHECK(PyModule_AddObject(module, "Record", (PyObject*)&Record_type) == 0, error)

    return 1;

error:
    return 0;
}
