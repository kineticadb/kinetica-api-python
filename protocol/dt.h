/*----------------------------------------------------------------------------*/
/* dt.h: functions supporting the Kinetica date and time formats.             */
/*----------------------------------------------------------------------------*/

#ifndef _DT_H_
#define _DT_H_

#include <Python.h>
#include "platform.h"

/*----------------------------------------------------------------------------*/

/* Minimum allowed datetime = 1/1/1000 */
#define MIN_YEAR 1000
#define MIN_EPOCH_MS -30610224000000

/* Maximum allowed datetime = 12/31/2900 */
#define MAX_YEAR 2900
#define MAX_EPOCH_MS 29379542399999

/* Kinetica date/time formats */
#define BITS_PER_YEAR   11 /* 0 = 1900 */
#define BITS_PER_MONTH   4 /* 1 based */
#define BITS_PER_DAY     5 /* 1 based */
#define BITS_PER_HOUR    5 /* 0 based */
#define BITS_PER_MINUTE  6 /* 0 based */
#define BITS_PER_SEC     6 /* 0 based */
#define BITS_PER_MSEC   10 /* 0 based */
#define BITS_PER_YDAY    9 /* 1 based */
#define BITS_PER_WDAY    3 /* 1 based */

#define DATE_BASE_YEAR   1900
#define DATE_DEFAULT     -1887301620 /* 1/1/1000 */
#define DATE_SHIFT_YEAR  (32               - BITS_PER_YEAR)
#define DATE_SHIFT_MONTH (DATE_SHIFT_YEAR  - BITS_PER_MONTH)
#define DATE_SHIFT_DAY   (DATE_SHIFT_MONTH - BITS_PER_DAY)
#define DATE_SHIFT_YDAY  (DATE_SHIFT_DAY   - BITS_PER_YDAY)
#define DATE_SHIFT_WDAY  (DATE_SHIFT_YDAY  - BITS_PER_WDAY)
#define DATE_MASK_YEAR   ((int32_t)((1 << BITS_PER_YEAR)  - 1) << DATE_SHIFT_YEAR)
#define DATE_MASK_MONTH  ((int32_t)((1 << BITS_PER_MONTH) - 1) << DATE_SHIFT_MONTH)
#define DATE_MASK_DAY    ((int32_t)((1 << BITS_PER_DAY)   - 1) << DATE_SHIFT_DAY)
#define DATE_MASK_YDAY   ((int32_t)((1 << BITS_PER_YDAY)  - 1) << DATE_SHIFT_YDAY)
#define DATE_MASK_WDAY   ((int32_t)((1 << BITS_PER_WDAY)  - 1) << DATE_SHIFT_WDAY)
#define DATE_YEAR(d)     (((d & DATE_MASK_YEAR)  >> DATE_SHIFT_YEAR) + DATE_BASE_YEAR)
#define DATE_MONTH(d)    ((d  & DATE_MASK_MONTH) >> DATE_SHIFT_MONTH)
#define DATE_DAY(d)      ((d  & DATE_MASK_DAY)   >> DATE_SHIFT_DAY)
#define DATE_YDAY(d)     ((d  & DATE_MASK_YDAY)  >> DATE_SHIFT_YDAY)
#define DATE_WDAY(d)     ((d  & DATE_MASK_WDAY)  >> DATE_SHIFT_WDAY)

#define TIME_SHIFT_HOUR   (31                - BITS_PER_HOUR)
#define TIME_SHIFT_MINUTE (TIME_SHIFT_HOUR   - BITS_PER_MINUTE)
#define TIME_SHIFT_SEC    (TIME_SHIFT_MINUTE - BITS_PER_SEC)
#define TIME_SHIFT_MSEC   (TIME_SHIFT_SEC    - BITS_PER_MSEC)
#define TIME_MASK_HOUR    ((int32_t)((1 << BITS_PER_HOUR)   - 1) << TIME_SHIFT_HOUR)
#define TIME_MASK_MINUTE  ((int32_t)((1 << BITS_PER_MINUTE) - 1) << TIME_SHIFT_MINUTE)
#define TIME_MASK_SEC     ((int32_t)((1 << BITS_PER_SEC)    - 1) << TIME_SHIFT_SEC)
#define TIME_MASK_MSEC    ((int32_t)((1 << BITS_PER_MSEC)   - 1) << TIME_SHIFT_MSEC)
#define TIME_HOUR(t)      ((t & TIME_MASK_HOUR)   >> TIME_SHIFT_HOUR)
#define TIME_MINUTE(t)    ((t & TIME_MASK_MINUTE) >> TIME_SHIFT_MINUTE)
#define TIME_SEC(t)       ((t & TIME_MASK_SEC)    >> TIME_SHIFT_SEC)
#define TIME_MSEC(t)      ((t & TIME_MASK_MSEC)   >> TIME_SHIFT_MSEC)

#define DT_BASE_YEAR    1900
#define DT_DEFAULT      -8105898787127426688 /* 1/1/1000 00:00:00.000 */
#define DT_SHIFT_YEAR   (64              - BITS_PER_YEAR)
#define DT_SHIFT_MONTH  (DT_SHIFT_YEAR   - BITS_PER_MONTH)
#define DT_SHIFT_DAY    (DT_SHIFT_MONTH  - BITS_PER_DAY)
#define DT_SHIFT_HOUR   (DT_SHIFT_DAY    - BITS_PER_HOUR)
#define DT_SHIFT_MINUTE (DT_SHIFT_HOUR   - BITS_PER_MINUTE)
#define DT_SHIFT_SEC    (DT_SHIFT_MINUTE - BITS_PER_SEC)
#define DT_SHIFT_MSEC   (DT_SHIFT_SEC    - BITS_PER_MSEC)
#define DT_SHIFT_YDAY   (DT_SHIFT_MSEC   - BITS_PER_YDAY)
#define DT_SHIFT_WDAY   (DT_SHIFT_YDAY   - BITS_PER_WDAY)
#define DT_MASK_YEAR    ((int64_t)((1 << BITS_PER_YEAR)   - 1) << DT_SHIFT_YEAR)
#define DT_MASK_MONTH   ((int64_t)((1 << BITS_PER_MONTH)  - 1) << DT_SHIFT_MONTH)
#define DT_MASK_DAY     ((int64_t)((1 << BITS_PER_DAY)    - 1) << DT_SHIFT_DAY)
#define DT_MASK_HOUR    ((int64_t)((1 << BITS_PER_HOUR)   - 1) << DT_SHIFT_HOUR)
#define DT_MASK_MINUTE  ((int64_t)((1 << BITS_PER_MINUTE) - 1) << DT_SHIFT_MINUTE)
#define DT_MASK_SEC     ((int64_t)((1 << BITS_PER_SEC)    - 1) << DT_SHIFT_SEC)
#define DT_MASK_MSEC    ((int64_t)((1 << BITS_PER_MSEC)   - 1) << DT_SHIFT_MSEC)
#define DT_MASK_YDAY    ((int64_t)((1 << BITS_PER_YDAY)   - 1) << DT_SHIFT_YDAY)
#define DT_MASK_WDAY    ((int64_t)((1 << BITS_PER_WDAY)   - 1) << DT_SHIFT_WDAY)
#define DT_YEAR(dt)     (((dt & DT_MASK_YEAR)   >> DT_SHIFT_YEAR) + DT_BASE_YEAR)
#define DT_MONTH(dt)    ((dt  & DT_MASK_MONTH)  >> DT_SHIFT_MONTH)
#define DT_DAY(dt)      ((dt  & DT_MASK_DAY)    >> DT_SHIFT_DAY)
#define DT_HOUR(dt)     ((dt  & DT_MASK_HOUR)   >> DT_SHIFT_HOUR)
#define DT_MINUTE(dt)   ((dt  & DT_MASK_MINUTE) >> DT_SHIFT_MINUTE)
#define DT_SEC(dt)      ((dt  & DT_MASK_SEC)    >> DT_SHIFT_SEC)
#define DT_MSEC(dt)     ((dt  & DT_MASK_MSEC)   >> DT_SHIFT_MSEC)
#define DT_YDAY(dt)     ((dt  & DT_MASK_YDAY)   >> DT_SHIFT_YDAY)
#define DT_WDAY(dt)     ((dt  & DT_MASK_WDAY)   >> DT_SHIFT_WDAY)

/*----------------------------------------------------------------------------*/

/* Convert a Kinetica datetime into epoch milliseconds. Provided value must be
   a valid Kinetica datetime. */
PY_LONG_LONG datetime_to_epoch_ms(const PY_LONG_LONG datetime);

/* Convert epoch milliseconds into a Kinetica datetime. Provided value must be
   within the range of valid Kinetica datetimes. */
PY_LONG_LONG epoch_ms_to_datetime(const PY_LONG_LONG epoch_ms);

/* Encode year, month and day values into Kinetica date date. Provided month
   and day values must be between 1..12 and 1..31 respectively. Returns 1 if
   successful, or 0 if the combination of values is not a valid date or out
   of Kinetica valid date range. */
int encode_date(const int year, const int month, const int day, long* date);

/* Encode year, month, day, hour, minute, second and millisecond values into
   Kinetica datetime datetime. Provided month and day values must be between
   1..12 and 1..31 respectively; provided time values must be valid. Returns
   1 if successful, or 0 if the combination of values is not a valid date or
   out of Kinetica valid date range. */
int encode_datetime(const int year, const int month, const int day, const int hour, const int minute, const int second, const int millisecond, PY_LONG_LONG* datetime);

/* Encode hour, minute and second values into Kinetica time time. Provided
   values must be valid. */
void encode_time(const int hour, const int minute, const int second, const int millisecond, long* time);

#endif /* _DT_H_ */
