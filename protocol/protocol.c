#include "protocol.h"

#include "bufferrange.h"
#include "common.h"
#include "record.h"
#include "schema.h"

/* -- Protocol Module State ------------------------------------------------- */

#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef Protocol_module;

    static int Protocol_clear(PyObject* module)
    {
        ProtocolState* state = (ProtocolState*)PyModule_GetState(module);
        Py_CLEAR(state->json_decode);
        Py_CLEAR(state->json_encode);
        Py_CLEAR(state->array_string);
        Py_CLEAR(state->label_string);
        Py_CLEAR(state->null_string);
        Py_CLEAR(state->nullable_string);
        Py_CLEAR(state->properties_string);
        Py_CLEAR(state->record_string);
        Py_CLEAR(state->type_definition_string);
        Py_CLEAR(state->type_name_string);
        Py_CLEAR(state->column_data_type_names);
        Py_CLEAR(state->schema_data_type_names);
        return 0;
    }

    static void Protocol_free(PyObject* module)
    {
        Protocol_clear(module);
    }

    ProtocolState* Protocol_get_state(void)
    {
        PyObject* module = PyState_FindModule(&Protocol_module);

        if (!module)
        {
            PyErr_SetString(PyExc_RuntimeError, "kinetica.protocol module not found");
            return NULL;
        }

        return (ProtocolState*)PyModule_GetState(module);
    }

    static int Protocol_traverse(PyObject* module, visitproc visit, void* arg)
    {
        ProtocolState* state = (ProtocolState*)PyModule_GetState(module);
        Py_VISIT(state->json_decode);
        Py_VISIT(state->json_encode);
        Py_VISIT(state->array_string);
        Py_VISIT(state->label_string);
        Py_VISIT(state->null_string);
        Py_VISIT(state->nullable_string);
        Py_VISIT(state->properties_string);
        Py_VISIT(state->record_string);
        Py_VISIT(state->type_definition_string);
        Py_VISIT(state->type_name_string);
        Py_VISIT(state->column_data_type_names);
        Py_VISIT(state->schema_data_type_names);
        return 0;
    }
#endif

/* -- Protocol Module Initialization ---------------------------------------- */

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef Protocol_module =
{
    PyModuleDef_HEAD_INIT,  /* m_base */
    "kinetica.protocol",    /* m_name */
    0,                      /* m_doc */
    sizeof(ProtocolState),  /* m_size */
    0,                      /* m_methods */
    0,                      /* m_slots */
    Protocol_traverse,      /* m_traverse */
    Protocol_clear,         /* m_clear */
    (freefunc)Protocol_free /* m_free */
};

PyMODINIT_FUNC PyInit_protocol(void)
#else
void initprotocol(void)
#endif
{
    PyObject* module;
    PyObject* imported = NULL;

    ProtocolState* state;

    PyObject* temp_class;
    PyObject* temp_object;

    #if PY_MAJOR_VERSION >= 3
        module = PyState_FindModule(&Protocol_module);

        if (module)
        {
            Py_INCREF(module);
            return module;
        }
    #endif

    #if PY_MAJOR_VERSION >= 3
        module = PyModule_Create(&Protocol_module);
    #else
        module = Py_InitModule("protocol", NULL);
    #endif

    CHECK(module, error);

    state = GET_STATE_MODULE(module);
    CHECK(state, error)

    imported = PyImport_ImportModule("json");
    CHECK(imported, error)

    temp_class = PyObject_GetAttrString(imported, "JSONDecoder");
    CHECK(temp_class, error)
    temp_object = PyObject_CallObject(temp_class, NULL);
    Py_DECREF(temp_class);
    CHECK(temp_object, error)
    state->json_decode = PyObject_GetAttrString(temp_object, "decode");
    Py_DECREF(temp_object);
    CHECK(state->json_decode, error)

    temp_class = PyObject_GetAttrString(imported, "JSONEncoder");
    CHECK(temp_class, error)
    temp_object = PyObject_CallObject(temp_class, NULL);
    Py_DECREF(temp_class);
    CHECK(temp_object, error)
    state->json_encode = PyObject_GetAttrString(temp_object, "encode");
    Py_DECREF(temp_object);
    CHECK(state->json_encode, error)

    Py_CLEAR(imported);

    CHECK(init_bufferrange(module), error)
    CHECK(init_record(module), error)
    CHECK(init_schema(module), error)

    #if PY_MAJOR_VERSION >= 3
        return module;
    #else
        return;
    #endif

error:
    Py_XDECREF(imported);

    #if PY_MAJOR_VERSION >= 3
        Py_XDECREF(module);
        return NULL;
    #else
        return;
    #endif
}
