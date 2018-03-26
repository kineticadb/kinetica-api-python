/*----------------------------------------------------------------------------*/
/* dt.c: functions supporting the Kinetica date and time formats.             */
/*----------------------------------------------------------------------------*/

#include "dt.h"

/* Calculation constants */
#define BASE_EPOCH_MS       -62162035200000 /* March 1, 0000 */
#define CENTURIES_PER_CYCLE               4
#define YEARS_PER_CYCLE                 400
#define YEARS_PER_CENTURY               100
#define YEARS_PER_LEAP                    4
#define MONTHS_PER_YEAR                  12
#define DAYS_PER_CYCLE               146097 /* 365 * 400 + 97 leap days */
#define DAYS_PER_CENTURY              36524 /* 365 * 100 + 24 leap days */
#define DAYS_PER_LEAP                  1461 /* 365 *   4 +  1 leap day */
#define DAYS_PER_YEAR                   365
#define DAYS_PER_WEEK                     7
#define MINUTES_PER_HOUR                 60
#define SEC_PER_MINUTE                   60
#define MSEC_PER_DAY               86400000
#define MSEC_PER_HOUR               3600000
#define MSEC_PER_MINUTE               60000
#define MSEC_PER_SEC                   1000

PY_LONG_LONG datetime_to_epoch_ms(const PY_LONG_LONG datetime)
{
    int year;
    int month;

    /* 0 = March */
    static const int days_before_month[] = { 0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337 };

    year = DT_YEAR(datetime);
    month = DT_MONTH(datetime) - 3;

    if (month < 0)
    {
        month += MONTHS_PER_YEAR;
        --year;
    }

    return BASE_EPOCH_MS
           + (year * (long)DAYS_PER_YEAR
              + year / YEARS_PER_LEAP
              - year / YEARS_PER_CENTURY
              + year / YEARS_PER_CYCLE
              + days_before_month[month]
              + DT_DAY(datetime) - 1) * MSEC_PER_DAY
           + DT_HOUR(datetime) * MSEC_PER_HOUR
           + DT_MINUTE(datetime) * MSEC_PER_MINUTE
           + DT_SEC(datetime) * MSEC_PER_SEC
           + DT_MSEC(datetime);
}

PY_LONG_LONG epoch_ms_to_datetime(const PY_LONG_LONG epoch_ms)
{
    PY_LONG_LONG base_ms;
    long days;
    long milliseconds;
    int day_of_week;
    int cycles_since_base;
    int centuries_since_cycle;
    int leaps_since_century;
    int years_since_leap;
    int is_leap_year;
    int day_of_year;
    int year;
    int month;

    /* 0 = March */
    static const int days_in_month[] = { 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29 };

    /* 0 = March 1, 0000 */
    base_ms = epoch_ms - BASE_EPOCH_MS;

    days = base_ms / MSEC_PER_DAY;
    milliseconds = base_ms % MSEC_PER_DAY;

    /* March 1, 0000 = Wednesday (3) */
    day_of_week = (days + 3) % DAYS_PER_WEEK;

    cycles_since_base = days / DAYS_PER_CYCLE;
    days %= DAYS_PER_CYCLE;
    centuries_since_cycle = days / DAYS_PER_CENTURY;

    if (centuries_since_cycle == CENTURIES_PER_CYCLE)
    {
        /* True on leap day of cycle year */
        --centuries_since_cycle;
    }

    days -= centuries_since_cycle * DAYS_PER_CENTURY;
    leaps_since_century = days / DAYS_PER_LEAP;
    days -= leaps_since_century * DAYS_PER_LEAP;
    years_since_leap = days / DAYS_PER_YEAR;

    if (years_since_leap == YEARS_PER_LEAP)
    {
        /* True on leap day */
        --years_since_leap;
    }

    days -= years_since_leap * DAYS_PER_YEAR;
    is_leap_year = years_since_leap == 0 && (leaps_since_century != 0 || centuries_since_cycle == 0);
    day_of_year = days + 59 + is_leap_year;

    if (day_of_year >= DAYS_PER_YEAR + is_leap_year)
    {
        day_of_year -= DAYS_PER_YEAR + is_leap_year;
    }

    year = cycles_since_base * YEARS_PER_CYCLE
           + centuries_since_cycle * YEARS_PER_CENTURY
           + leaps_since_century * YEARS_PER_LEAP
           + years_since_leap;

    for (month = 0; days_in_month[month] <= days; ++month)
    {
        days -= days_in_month[month];
    }

    month += 3;

    if (month > MONTHS_PER_YEAR)
    {
        month -= MONTHS_PER_YEAR;
        ++year;
    }

    return (PY_LONG_LONG)(((int64_t)(year - DT_BASE_YEAR) << DT_SHIFT_YEAR)
           + ((int64_t)month << DT_SHIFT_MONTH)
           + ((int64_t)(days + 1) << DT_SHIFT_DAY)
           + ((int64_t)(milliseconds / MSEC_PER_HOUR) << DT_SHIFT_HOUR)
           + ((int64_t)(milliseconds / MSEC_PER_MINUTE % MINUTES_PER_HOUR) << DT_SHIFT_MINUTE)
           + ((int64_t)(milliseconds / MSEC_PER_SEC % SEC_PER_MINUTE) << DT_SHIFT_SEC)
           + ((int64_t)(milliseconds % MSEC_PER_SEC) << DT_SHIFT_MSEC)
           + ((int64_t)(day_of_year + 1) << DT_SHIFT_YDAY)
           + ((int64_t)(day_of_week + 1) << DT_SHIFT_WDAY));
}

static int compute_days(const int year, const int month, const int day, int* day_of_year, int* day_of_week)
{
    int not_leap_year;

    int m;
    int y;

    static const int days_in_month[] = { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    static const int days_before_month[] = { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 };
    static const int day_of_week_offset[] = { 0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4 };

    if (year < MIN_YEAR || year > MAX_YEAR)
    {
        return 0;
    }

    m = month - 1;

    if (day > days_in_month[m])
    {
        return 0;
    }

    not_leap_year = year % 4 != 0 || (year % 100 == 0 && year % 400 != 0);

    if (not_leap_year && month == 2 && day == 29)
    {
        return 0;
    }

    if (month < 3)
    {
        *day_of_year = days_before_month[m] + day;
        y = year - 1;
    }
    else
    {
        *day_of_year = days_before_month[m] - not_leap_year + day;
        y = year;
    }

    *day_of_week = (day + day_of_week_offset[m] + y + (y / 4) - (y / 100) + (y / 400)) % 7 + 1;
    return 1;
}

int encode_date(const int year, const int month, const int day, long* date)
{
    int day_of_year;
    int day_of_week;

    if (!compute_days(year, month, day, &day_of_year, &day_of_week))
    {
        return 0;
    }

    *date = (long)(((int32_t)(year - DATE_BASE_YEAR) << DATE_SHIFT_YEAR)
            + ((int32_t)month << DATE_SHIFT_MONTH)
            + ((int32_t)day << DATE_SHIFT_DAY)
            + ((int32_t)day_of_year << DATE_SHIFT_YDAY)
            + ((int32_t)day_of_week << DATE_SHIFT_WDAY));
    return 1;
}

int encode_datetime(const int year, const int month, const int day, const int hour, const int minute, const int second, const int millisecond, PY_LONG_LONG* datetime)
{
    int day_of_year;
    int day_of_week;

    if (!compute_days(year, month, day, &day_of_year, &day_of_week))
    {
        return 0;
    }

    *datetime = (PY_LONG_LONG)(((int64_t)(year - DT_BASE_YEAR) << DT_SHIFT_YEAR)
                + ((int64_t)month << DT_SHIFT_MONTH)
                + ((int64_t)day << DT_SHIFT_DAY)
                + ((int64_t)hour << DT_SHIFT_HOUR)
                + ((int64_t)minute << DT_SHIFT_MINUTE)
                + ((int64_t)second << DT_SHIFT_SEC)
                + ((int64_t)millisecond << DT_SHIFT_MSEC)
                + ((int64_t)day_of_year << DT_SHIFT_YDAY)
                + ((int64_t)day_of_week << DT_SHIFT_WDAY));
    return 1;
}

void encode_time(const int hour, const int minute, const int second, const int millisecond, long* time)
{
    *time = (long)(((int32_t)hour << TIME_SHIFT_HOUR)
            + ((int32_t)minute << TIME_SHIFT_MINUTE)
            + ((int32_t)second << TIME_SHIFT_SEC)
            + ((int32_t)millisecond << TIME_SHIFT_MSEC));
}
