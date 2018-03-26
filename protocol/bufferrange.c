/*----------------------------------------------------------------------------*/
/* bufferrange.c: BufferRange Python class.                                   */
/*----------------------------------------------------------------------------*/

#include "bufferrange.h"

#include "structmember.h"

#include "common.h"

/*----------------------------------------------------------------------------*/

/* BufferRange class implementation. */

/* Internal function that returns a tuple containing the start and length
   values from a BufferRange. Used for implementing __repr__. */
static PyObject* _BufferRange_repr_object(BufferRange* self)
{
    PyObject* tuple;

    PyObject* temp;

    tuple = PyTuple_New(2);
    CHECK(tuple, error)

    #if PY_MAJOR_VERSION >= 3
        temp = PyLong_FromSsize_t(self->start);
        CHECK(temp, error)
        PyTuple_SET_ITEM(tuple, 0, temp);
        temp = PyLong_FromSsize_t(self->length);
        CHECK(temp, error)
        PyTuple_SET_ITEM(tuple, 1, temp);
    #else
        temp = PyInt_FromSsize_t(self->start);
        CHECK(temp, error)
        PyTuple_SET_ITEM(tuple, 0, temp);
        temp = PyInt_FromSsize_t(self->length);
        CHECK(temp, error)
        PyTuple_SET_ITEM(tuple, 1, temp);
    #endif

    return tuple;

error:
    Py_DECREF(tuple);
    return NULL;
}

/* Python BufferRange object constructor.

   Parameters:
       start (int, optional)
           Start position of the range. Must be >= 0 and <= max Py_ssize_t.
           Defaults to 0.

       length (int, optional)
           Length of the range, or -1 if not applicable. Must be >= -1 and
           <= max Py_ssize_t. Defaults to -1. */
static BufferRange* BufferRange_new(PyTypeObject* type, PyObject* args, PyObject* kwargs)
{
    Py_ssize_t arg_start = 0;
    Py_ssize_t arg_length = -1;
    static char* keywords[] = { "start", "length", NULL };

    BufferRange* self;

    CHECK(PyArg_ParseTupleAndKeywords(args, kwargs, "|nn", keywords, &arg_start, &arg_length), error)
    CHECK_STRING(arg_start >= 0, PyExc_ValueError, "start must be be >= 0", error)
    CHECK_STRING(arg_length >= -1, PyExc_ValueError, "length must be >= -1", error)

    self = (BufferRange*)type->tp_alloc(type, 0);
    CHECK(self, error)
    self->start = arg_start;
    self->length = arg_length;
    return self;

error:
    return NULL;
}

/* Python BufferRange.__repr__ method. */
static PyObject* BufferRange_repr(BufferRange* self)
{
    return generic_repr((PyObject*)self, (reprfunc)_BufferRange_repr_object);
}

/* Python BufferRange rich compare function (supports == and != operators). */
static PyObject* BufferRange_richcompare(PyObject* a, PyObject* b, int op)
{
    PyObject* result;
    int eq;

    result = generic_richcompare(&BufferRange_type, a, b, op);

    if (result != (PyObject*)&BufferRange_type)
    {
        return result;
    }

    eq = ((BufferRange*)a)->start == ((BufferRange*)b)->start
         && ((BufferRange*)a)->length == ((BufferRange*)b)->length;
    result = eq ? (op == Py_EQ ? Py_True : Py_False) : (op == Py_EQ ? Py_False : Py_True);
    Py_INCREF(result);
    return result;
}

static PyMemberDef BufferRange_members[] =
{
    { "start", T_PYSSIZET, offsetof(BufferRange, start), READONLY, NULL },
    { "length", T_PYSSIZET, offsetof(BufferRange, length), READONLY, NULL },
    { NULL }
};

PyTypeObject BufferRange_type =
{
    PyVarObject_HEAD_INIT(NULL, 0)
    "kinetica.protocol.BufferRange",      /* tp_name */
    sizeof(BufferRange),                  /* tp_basicsize */
    0,                                    /* tp_itemsize */
    0,                                    /* tp_dealloc */
    0,                                    /* tp_print */
    0,                                    /* tp_getattr */
    0,                                    /* tp_setattr */
    0,                                    /* tp_compare */
    (reprfunc)BufferRange_repr,           /* tp_repr */
    0,                                    /* tp_as_number */
    0,                                    /* tp_as_sequence */
    0,                                    /* tp_as_mapping */
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
    (richcmpfunc)BufferRange_richcompare, /* tp_richcompare */
    0,                                    /* tp_weaklistoffset */
    0,                                    /* tp_iter */
    0,                                    /* tp_iternext */
    0,                                    /* tp_methods */
    BufferRange_members,                  /* tp_members */
    0,                                    /* tp_getset */
    0,                                    /* tp_base */
    0,                                    /* tp_dict */
    0,                                    /* tp_descr_get */
    0,                                    /* tp_descr_set */
    0,                                    /* tp_dictoffset */
    0,                                    /* tp_init */
    0,                                    /* tp_alloc */
    (newfunc)BufferRange_new,             /* tp_new */
};

/* Internal function to create a BufferRange object directly. */
PyObject* BufferRange_create(Py_ssize_t start, Py_ssize_t length)
{
    BufferRange* result = (BufferRange*)BufferRange_type.tp_alloc(&BufferRange_type, 0);

    if (result)
    {
        result->start = start;
        result->length = length;
    }

    return (PyObject*)result;
}

/*----------------------------------------------------------------------------*/

/* File initialization: called during module initialization. */
int init_bufferrange(PyObject* module)
{
    CHECK(PyType_Ready(&BufferRange_type) == 0, error)

    Py_INCREF(&BufferRange_type);
    CHECK(PyModule_AddObject(module, "BufferRange", (PyObject*)&BufferRange_type) == 0, error)

    return 1;

error:
    return 0;
}
