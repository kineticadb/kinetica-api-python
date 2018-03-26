/*----------------------------------------------------------------------------*/
/* common.c: miscellaneous functions for interacting with the Python          */
/* runtime.                                                                   */
/*----------------------------------------------------------------------------*/

#include "common.h"

/* Create a Python str/unicode object based on format and args. */
PyObject* format_string_v(const char* format, va_list args)
{
    #if PY_MAJOR_VERSION >= 3
        return PyUnicode_FromFormatV(format, args);
    #else
        PyObject* unicode;
        PyObject* string;

        unicode = PyUnicode_FromFormatV(format, args);

        if (!unicode)
        {
            return NULL;
        }

        string = PyUnicode_AsUTF8String(unicode);
        Py_DECREF(unicode);
        return string;
    #endif
}

/* Create a Python str/unicode object based on format and varargs. */
PyObject* format_string(const char* format, ...)
{
    va_list args;
    PyObject* result;

    va_start(args, format);
    result = format_string_v(format, args);
    va_end(args);
    return result;
}

/* Create a Python str/unicode object based on format and varargs. If an
   exception occurs, ignore it and restore any previous exception that was
   present. */
PyObject* format_string_safe(const char* format, ...)
{
    PyObject* type;
    PyObject* value;
    PyObject* traceback;
    va_list args;
    PyObject* result;

    PyErr_Fetch(&type, &value, &traceback);
    va_start(args, format);
    result = format_string_v(format, args);
    va_end(args);
    PyErr_Restore(type, value, traceback);
    return result;
}

/* Generic repr method handler. Calls repr_object_func to get a representation
   of self that can be rendered as a string, then prepends the class name. */
PyObject* generic_repr(PyObject* self, const reprfunc repr_object_func)
{
    PyObject* repr_object;
    PyObject* result;

    repr_object = repr_object_func(self);

    if (!repr_object)
    {
        return NULL;
    }

    /* If repr_object_func returned a tuple, convert to a string with the
       class name prepended. Otherwise, convert to a string, surround with
       parentheses, and prepend the class name. */

    if (PyTuple_Check(repr_object))
    {
        result = format_string("%s%R", Py_TYPE(self)->tp_name, repr_object);
    }
    else
    {
        result = format_string("%s(%R)", Py_TYPE(self)->tp_name, repr_object);
    }

    Py_DECREF(repr_object);
    return result;
}

/* Generic richcompare method handler for classes that only support equal and
   not equal checks. If a and b are not both of the type type, returns
   Py_NotImplemented. If a and b are the same object, returns Py_True or
   Py_False depending on op. Otherwise, returns type, and the caller can do
   any additional checks to determine equality. */
PyObject* generic_richcompare(PyTypeObject* type, PyObject* a, PyObject* b, int op)
{
    if (Py_TYPE(a) != type || Py_TYPE(b) != type)
    {
        Py_INCREF(Py_NotImplemented);
        return Py_NotImplemented;
    }

    switch (op)
    {
        case Py_EQ:
            if (a == b)
            {
                Py_RETURN_TRUE;
            }

            break;

        case Py_NE:
            if (a == b)
            {
                Py_RETURN_FALSE;
            }

            break;

        default:
            PyErr_SetString(PyExc_TypeError, "comparison not supported");
            return NULL;
    }

    return (PyObject*)type;
}

/* If error indicates an Avro read error, sets an appropriate Python exception
   and returns 0. Otherwise, returns 1. */
int handle_read_error(const AvroErrorCode error)
{
    switch (error)
    {
        case ERR_NONE:
            return 1;

        case ERR_OOM:
            PyErr_NoMemory();
            return 0;

        case ERR_EOF:
            PyErr_SetString(PyExc_EOFError, "incomplete binary data");
            return 0;

        case ERR_OVERFLOW:
            PyErr_SetString(PyExc_ValueError, "invalid binary data");
            return 0;
    }

    PyErr_SetString(PyExc_ValueError, "read returned invalid error code");
    return 0;
}

/* If error indicates an Avro write error, sets an appropriate Python
   exception and returns 0. Otherwise, returns 1. */
int handle_write_error(const AvroErrorCode error)
{
    switch (error)
    {
        case ERR_NONE:
            return 1;

        case ERR_OOM:
            PyErr_NoMemory();
            return 0;

        case ERR_EOF:
            PyErr_SetString(PyExc_EOFError, "insufficient buffer size");
            return 0;

        case ERR_OVERFLOW:
            PyErr_SetString(PyExc_ValueError, "invalid value");
            return 0;
    }

    PyErr_SetString(PyExc_ValueError, "write returned invalid error code");
    return 0;
}

/* Searches through the first valid_value_count values of the tuple
   valid_values for an object that equals value. If found, returns the index of
   the matching value in the tuple. Otherwise, returns valid_value_count. */
int lookup_string(PyObject* value, PyObject* valid_values, const int valid_value_count)
{
    int i;

    for (i = 0; i < valid_value_count; ++i)
    {
        int r;

        r = PyObject_RichCompareBool(value, PyTuple_GET_ITEM(valid_values, i), Py_EQ);

        if (r == -1)
        {
            return -1;
        }

        if (r)
        {
            return i;
        }
    }

    return valid_value_count;
}

/* Prepends the string representation of the specified object prefix to the
   current Python exception's value, if an exception is set. */
void prefix_exception(PyObject* prefix)
{
    PyObject* type;
    PyObject* value;
    PyObject* traceback;

    PyErr_Fetch(&type, &value, &traceback);

    if (value)
    {
        PyObject* new_value = format_string("%S: %S", prefix, value);

        if (new_value)
        {
            Py_DECREF(value);
            value = new_value;
        }
    }

    PyErr_Restore(type, value, traceback);
}
