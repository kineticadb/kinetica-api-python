/*----------------------------------------------------------------------------*/
/* avro.h: functions for reading and writing raw binary-encoded Avro data.    */
/*----------------------------------------------------------------------------*/

#ifndef _AVRO_H_
#define _AVRO_H_

#include <Python.h>

/*----------------------------------------------------------------------------*/

/* Defines for some Visual C++ versions which do not include the stdint
   types. */

#if defined(_MSC_VER)
    typedef signed __int8 int8_t;
    typedef signed __int32 int32_t;
    typedef signed __int64 int64_t;
    typedef unsigned __int8 uint8_t;
    typedef unsigned __int32 uint32_t;
    typedef unsigned __int64 uint64_t;
#else
    #include <stdint.h>
#endif

/*----------------------------------------------------------------------------*/

/* AvroErrorCode: an enumeration of errors returned by Avro functions. */
typedef enum
{
    ERR_NONE,     /* No error, the function completed successfully */
    ERR_OOM,      /* Insufficient memory to read a given value */
    ERR_EOF,      /* Premature EOF was reached */
    ERR_OVERFLOW  /* A given value overflows data type range or is invalid */
}
AvroErrorCode;

/*----------------------------------------------------------------------------*/

/* Macro to return an AvroErrorCode from a function if an error occurred. */
#define AVRO_RETURN_ERROR(s) { AvroErrorCode _e = s; if (_e != ERR_NONE) return _e; }

/*----------------------------------------------------------------------------*/

/* Functions to read binary-encoded Avro values from a buffer. All read
   functions take these parameters:

   pos: Pointer to a pointer to the start of the value being read within the
        buffer. This will be updated on a successful read to point to the
        position immediately following the value that was read.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the value is completely read, ERR_EOF is returned.

   Most functions return an error code, or ERR_NONE if successful. In case of
   error, no output parameters will be altered. */

/* Read a Boolean value into b (1 = true, 0 = false). Returns ERR_OVERFLOW
   if the value is invalid. */
AvroErrorCode read_boolean(uint8_t** pos, const uint8_t* max, char* b);

/* Read the length of a bytes or string value into len. The value itself can
   then be read with the read_bytes_data function. Returns ERR_OVERFLOW if the
   value is invalid, or ERR_EOF if the value specifies a length that would
   extend beyond the end of the buffer. */
AvroErrorCode read_bytes_len(uint8_t** pos, const uint8_t* max, Py_ssize_t* len);

/* Read a bytes or string value into b, given a length in len previously read
   by read_bytes_len. b must point to a buffer of at least len bytes. This
   function assumes read_bytes_len has already detected invalid data and thus
   always succeeds and does not return any error code. */
void read_bytes_data(uint8_t** pos, const uint8_t* max, uint8_t* b, const Py_ssize_t len);

/* Read a sequence of ASCII digits from a string value and parse them into i,
   also returning the number of digits actually read in digits. min_digits
   specifies the minimum number of digits expected, and min_value and max_value
   specify the minimum and maximum allowed value; returns ERR_OVERFLOW if these
   conditions are not met. max_digits specifies the maximum number of digits to
   be read (if more are present no error is returned, but they are not read).
   When calling this function, set max to point to the end of the value which
   the digits are being read from, instead of the end of the overall buffer. */
AvroErrorCode read_digits(uint8_t** pos, const uint8_t* max, const unsigned min_digits, const unsigned max_digits, const long min_value, const long max_value, long* i, unsigned* digits);

/* Read an IEEE double-precision floating point value into d. */
AvroErrorCode read_double(uint8_t** pos, const uint8_t* max, double* d);

/* Read an IEEE single-precision floating point value into f. */
AvroErrorCode read_float(uint8_t** pos, const uint8_t* max, float* f);

/* Read a 32-bit integer value into i. Returns ERR_OVERFLOW if the value is
   invalid. */
AvroErrorCode read_int(uint8_t** pos, const uint8_t* max, long* i);

/* Read a 64-bit integer value into l. Returns ERR_OVERFLOW if the value is
   invalid. */
AvroErrorCode read_long(uint8_t** pos, const uint8_t* max, PY_LONG_LONG* l);

/* Read a Py_ssize_t value (e.g. a bytes, string, array or map length) into l.
   Returns ERR_OVERFLOW if the value is invalid. */
AvroErrorCode read_size(uint8_t** pos, const uint8_t* max, Py_ssize_t* l);

/*----------------------------------------------------------------------------*/

/* Calculate the number of bytes required for Avro encoding integer value l. */
Py_ssize_t size_long(const PY_LONG_LONG l);

/*----------------------------------------------------------------------------*/

/* Functions to skip over binary-encoded Avro values in a buffer. All skip
   functions take these parameters:

   pos: Pointer to a pointer to the start of the value being skipped within the
        buffer. This will be updated on a successful skip to point to the
        position immediately following the value that was skipped.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the value is completely skipped, ERR_EOF is returned.

   All functions return an error code, or ERR_NONE if successful. In case of
   error, no output parameters will be altered. */

/* Skip a bytes or string value. Returns ERR_OVERFLOW if the value is
   invalid. */
AvroErrorCode skip_bytes(uint8_t** pos, const uint8_t* max);

/* Skip an expected character in a string value, returning ERR_OVERFLOW if the
   character is not present at *pos. When calling this function, set max to
   point to the end of the value containing the character, instead of the end
   of the overall buffer. */
AvroErrorCode skip_char(uint8_t** pos, const uint8_t* max, const char expected);

/* Skip an IEEE double-precision floating point value. */
AvroErrorCode skip_double(uint8_t** pos, const uint8_t* max);

/* Skip an IEEE single-precision floating point value. */
AvroErrorCode skip_float(uint8_t** pos, const uint8_t* max);

/* Skip a 32-bit integer value. Returns ERR_OVERFLOW if the value is invalid. */
AvroErrorCode skip_int(uint8_t** pos, const uint8_t* max);

/* Skip a 64-bit integer value. Returns ERR_OVERFLOW if the value is invalid. */
AvroErrorCode skip_long(uint8_t** pos, const uint8_t* max);

/* Skip consecutive whitespace (space, tab, LF, vtab, FF, CR) characters in a
   string value, until a non-whitespace character is found or the end of the
   buffer is reached. min_chars specifies the minimum number of expected
   whitespace characters; returns ERR_OVERFLOW if this condition is not met.
   When calling this function, set max to point to the end of the value
   containing the character, instead of the end of the overall buffer. */
AvroErrorCode skip_whitespace(uint8_t** pos, const uint8_t* max, const unsigned min_chars);

/*----------------------------------------------------------------------------*/

/* Functions to write binary-encoded Avro values into a buffer. All write
   functions take these parameters:

   pos: Pointer to a pointer to the position within the buffer to write the
        value. This will be updated on a successful write to point to the
        position immediately following the value that was written.

   max: Pointer to the end of the buffer (must be >= *pos). If this is reached
        before the value is completely written, ERR_EOF is returned.

   All functions return an error code, or ERR_NONE if successful. In case of
   error, no output parameters will be altered. */

/* Write a Boolean value b (1 = true, 0 = false). */
AvroErrorCode write_boolean(uint8_t** pos, const uint8_t* max, const char b);

/* Write a bytes or string value b of length len. */
AvroErrorCode write_bytes(uint8_t** pos, const uint8_t* max, const uint8_t* b, const Py_ssize_t len);

/* Write a character c as part of a string value. This assumes that the length
   of the string has been written separately. */
AvroErrorCode write_char(uint8_t** pos, const uint8_t* max, const char c);

/* Write an integer value i as a sequence of ASCII digits as part of a string
   value. This assumes that the length of the string has been written
   separately. The written value will be left-padded with zeroes as needed to
   ensure at least min_digits are written. */
AvroErrorCode write_digits(uint8_t** pos, const uint8_t* max, const int min_digits, const int i);

/* Write an IEEE double-precision floating point value d. */
AvroErrorCode write_double(uint8_t** pos, const uint8_t* max, const double d);

/* Write an IEEE single-precision floating point value f. */
AvroErrorCode write_float(uint8_t** pos, const uint8_t* max, const float f);

/* Write a 32-bit integer value i. */
AvroErrorCode write_int(uint8_t** pos, const uint8_t* max, const long i);

/* Write a 64-bit integer value l. */
AvroErrorCode write_long(uint8_t** pos, const uint8_t* max, const PY_LONG_LONG l);

/* Write a Py_ssize_t value (e.g. a bytes, string, array or map length) l. */
AvroErrorCode write_size(uint8_t** pos, const uint8_t* max, const Py_ssize_t l);

#endif /* _AVRO_H_ */
