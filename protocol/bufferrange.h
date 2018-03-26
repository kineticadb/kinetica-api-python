/*----------------------------------------------------------------------------*/
/* bufferrange.h: BufferRange Python class.                                   */
/*----------------------------------------------------------------------------*/

#ifndef _BUFFERRANGE_H_
#define _BUFFERRANGE_H_

#include <Python.h>

/*----------------------------------------------------------------------------*/

/* BufferRange: an immutable Python class containing a start position and
   length. Used instead of a tuple of two ints because the values can be stored
   directly as native Py_ssize_t values for better performance and no
   requirement to hold the GIL to access them. */

typedef struct
{
    PyObject_HEAD
    Py_ssize_t start;  /* Start position of range (must be >= 0) */
    Py_ssize_t length; /* Length of range or -1 for N/A (must be >= -1) */
}
BufferRange;

/* Internal function to create a BufferRange object directly. */
PyObject* BufferRange_create(Py_ssize_t start, Py_ssize_t length);

/* Python type object structure and type check macro. */
extern PyTypeObject BufferRange_type;
#define BufferRange_check(o) PyObject_TypeCheck(o, &BufferRange_type)

/*----------------------------------------------------------------------------*/

/* File initialization: called during module initialization. */
int init_bufferrange(PyObject* module);

#endif /* _BUFFERRANGE_H_ */
