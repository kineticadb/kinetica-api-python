/*----------------------------------------------------------------------------*/
/* platform.h: Platform specific defines.                                     */
/*----------------------------------------------------------------------------*/

#ifndef _PROTOCOL_PLATFORM_H
#define _PROTOCOL_PLATFORM_H

/* Defines for some Visual C++ versions which do not include the stdint
   types. */

#if defined(_MSC_VER)
    typedef signed   __int8  int8_t;
    typedef signed   __int16 int16_t;
    typedef signed   __int32 int32_t;
    typedef signed   __int64 int64_t;
    typedef unsigned __int8  uint8_t;
    typedef unsigned __int16 uint16_t;
    typedef unsigned __int32 uint32_t;
    typedef unsigned __int64 uint64_t;

    #define INT8_MIN  SCHAR_MIN
    #define INT8_MAX  SCHAR_MAX
    #define INT16_MIN SHRT_MIN
    #define INT16_MAX SHRT_MAX
    #define INT32_MIN INT_MIN
    #define INT32_MAX INT_MAX
    #define INT64_MIN _I64_MIN
    #define INT64_MAX _I64_MAX
#else
    #include <stdint.h>
#endif

#endif
