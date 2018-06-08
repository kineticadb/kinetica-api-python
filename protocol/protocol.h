/*----------------------------------------------------------------------------*/
/* protocol.h: state for the protocol Python module.                          */
/*----------------------------------------------------------------------------*/

#ifndef _PROTOCOL_H_
#define _PROTOCOL_H_

#include <Python.h>
#include "platform.h"

/*----------------------------------------------------------------------------*/

/* ProtocolState: a struct containing module-wide global state used by the
   protocol Python module. */

typedef struct
{
    /* Pointer to the decode method of a JSONDecoder object. */
    PyObject* json_decode;

    /* Pointer to he encode method of a JSONEncoder object. */
    PyObject* json_encode;

    /* Unicode objects containing string constants initialized and used by
       record.c. */
    PyObject* array_string;           /* "array" */
    PyObject* label_string;           /* "label" */
    PyObject* null_string;            /* "null" */
    PyObject* nullable_string;        /* "nullable" */
    PyObject* properties_string;      /* "properties" */
    PyObject* record_string;          /* "record" */
    PyObject* type_definition_string; /* "type_definition" */
    PyObject* type_name_string;       /* "type_name" */

    /* Tuple containing unicode objects corresponding to the values of the
       ColumnDataType enumeration in record.h. Initialized by record.c. */
    PyObject* column_data_type_names;

    /* Tuple containing unicode objects corresponding to the values of the
       SchemaDataType enumeration in schema.h. Initialized by schema.c. */
    PyObject* schema_data_type_names;
}
ProtocolState;

/* Macros to get a pointer to the state struct using appropriate Python 2 or 3
   semantics.

   GET_STATE() does not require a pointer to the Python module object and can
   be used from anywhere. Returns NULL and sets a Python exception on error.
   This may be relatively slow and the result should be cached when possible.

   GET_STATE_MODULE(m) requires a pointer to the the Python module object to
   be specified. Returns NULL on error. This is faster than GET_STATE() and is
   usable from module initialization code where the module object is
   accessible. */

#if PY_MAJOR_VERSION >= 3
    ProtocolState* Protocol_get_state(void);

    #define GET_STATE() Protocol_get_state()
    #define GET_STATE_MODULE(m) ((ProtocolState*)PyModule_GetState(m))
#else
    extern ProtocolState Protocol_state;

    #define GET_STATE() &Protocol_state
    #define GET_STATE_MODULE(m) &Protocol_state
#endif

#endif /* _PROTOCOL_H_ */
