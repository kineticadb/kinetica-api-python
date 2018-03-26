/*----------------------------------------------------------------------------*/
/* common.h: miscellaneous macros and functions for interacting with the      */
/* Python runtime.                                                            */
/*----------------------------------------------------------------------------*/

#ifndef _COMMON_H_
#define _COMMON_H_

#include <Python.h>

#include "avro.h"

/*----------------------------------------------------------------------------*/

/* Error handling macros. */

/* Go to label g if condition c is not true. */
#define CHECK(c, g) if (!(c)) { goto g; }

/* Set Python exception type t, with no value, and go to label g, if condition
   c is not true */
#define CHECK_NONE(c, t, g) if (!(c)) { PyErr_SetNone(t); goto g; }

/* Set Python exception type t, with a value of Python object o, and go to
   label g, if condition c is not true. */
#define CHECK_OBJECT(c, t, o, g) if (!(c)) { PyErr_SetObject(t, o); goto g; }

/* Set Python exception type t, with a value of char* string s, and go to
   label g, if condition c is not true. */
#define CHECK_STRING(c, t, s, g) if (!(c)) { PyErr_SetString(t, s); goto g; }

/*----------------------------------------------------------------------------*/

/* Python 2/3 compatibility macros. */

#if PY_MAJOR_VERSION >= 3
    /* Check if v is a Python str object. */
    #define IS_STRING(v) PyUnicode_Check(v)

    /* Check if Python str object v is an empty string. */
    #define IS_STRING_EMPTY(v) (PyUnicode_GET_LENGTH(v) == 0)

    /* Allocate s bytes using the Python raw allocator. */
    #define MALLOC(s) PyMem_RawMalloc(s)

    /* Create a Python str object from arbitrary Python object v. */
    #define TO_STRING(v) PyObject_Str(v)
#else
    /* Check if v is a Python str or unicode object. */
    #define IS_STRING(v) (PyUnicode_Check(v) || PyString_Check(v))

    /* Check if Python unicode object v is an empty string. */
    #define IS_STRING_EMPTY(v) (PyUnicode_GET_SIZE(v) == 0)

    /* Allocate s bytes using the system allocator. */
    #define MALLOC(s) malloc(s)

    /* Create a Python unicode object from arbitrary Python object v. */
    #define TO_STRING(v) PyObject_Unicode(v)
#endif

/*----------------------------------------------------------------------------*/

/* String formatting functions. */

/* Create a Python str/unicode object based on format and args. */
PyObject* format_string_v(const char* format, va_list args);

/* Create a Python str/unicode object based on format and varargs. */
PyObject* format_string(const char* format, ...);

/* Create a Python str/unicode object based on format and varargs. If an
   exception occurs, ignore it and restore any previous exception that was
   present. */
PyObject* format_string_safe(const char* format, ...);

/*----------------------------------------------------------------------------*/

/* Generic handlers for standard Python methods. */

/* Generic repr method handler. Calls repr_object_func to get a representation
   of self that can be rendered as a string, then prepends the class name. */
PyObject* generic_repr(PyObject* self, const reprfunc repr_object_func);

/* Generic richcompare method handler for classes that only support equal and
   not equal checks. If a and b are not both of the type type, returns
   Py_NotImplemented. If a and b are the same object, returns Py_True or
   Py_False depending on op. Otherwise, returns type, and the caller can do
   any additional checks to determine equality. */
PyObject* generic_richcompare(PyTypeObject* type, PyObject* a, PyObject* b, int op);

/*----------------------------------------------------------------------------*/

/* Avro error handlers. */

/* If error indicates an Avro read error, sets an appropriate Python exception
   and returns 0. Otherwise, returns 1. */
int handle_read_error(const AvroErrorCode error);

/* If error indicates an Avro write error, sets an appropriate Python
   exception and returns 0. Otherwise, returns 1. */
int handle_write_error(const AvroErrorCode error);

/*----------------------------------------------------------------------------*/

/* Searches through the first valid_value_count values of the tuple
   valid_values for an object that equals value. If found, returns the index of
   the matching value in the tuple. Otherwise, returns valid_value_count. */
int lookup_string(PyObject* value, PyObject* valid_values, const int valid_value_count);

/*----------------------------------------------------------------------------*/

/* Prepends the string representation of the specified object prefix to the
   current Python exception's value, if an exception is set. */
void prefix_exception(PyObject* prefix);

#endif /* _COMMON_H_ */
