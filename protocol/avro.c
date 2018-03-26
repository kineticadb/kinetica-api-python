#include "avro.h"

AvroErrorCode read_boolean(uint8_t** pos, const uint8_t* max, char* b)
{
    int8_t temp_b;

    if (*pos + 1 > max)
    {
        return ERR_EOF;
    }

    temp_b = (int8_t)**pos;

    if (temp_b != 0 && temp_b != 1)
    {
        return ERR_OVERFLOW;
    }

    *b = temp_b;
    *pos += 1;
    return ERR_NONE;
}

AvroErrorCode read_bytes_len(uint8_t** pos, const uint8_t* max, Py_ssize_t* len)
{
    Py_ssize_t temp_len;
    AvroErrorCode result;

    result = read_size(pos, max, &temp_len);

    if (result != ERR_NONE)
    {
        return result;
    }

    if (temp_len < 0)
    {
        return ERR_OVERFLOW;
    }

    if (*pos + temp_len > max)
    {
        return ERR_EOF;
    }

    *len = temp_len;
    return ERR_NONE;
}

void read_bytes_data(uint8_t** pos, const uint8_t* max, uint8_t* b, const Py_ssize_t len)
{
    memcpy(b, *pos, len);
    *pos += len;
}

AvroErrorCode read_digits(uint8_t** pos, const uint8_t* max, const unsigned min_digits, const unsigned max_digits, const long min_value, const long max_value, long* i, unsigned* digits)
{
    unsigned temp_digits = 0;
    long temp_i = 0;

    while (temp_digits <= max_digits && *pos < max && **pos >= '0' && **pos <= '9')
    {
        temp_i *= 10;
        temp_i += **pos - '0';
        ++temp_digits;
        ++*pos;
    }

    if (temp_digits < min_digits)
    {
        return (*pos == max) ? ERR_EOF : ERR_OVERFLOW;
    }
    else if (temp_i < min_value || temp_i > max_value)
    {
        return ERR_OVERFLOW;
    }

    *digits = temp_digits;
    *i = temp_i;
    return ERR_NONE;
}

AvroErrorCode read_double(uint8_t** pos, const uint8_t* max, double* d)
{
    if (*pos + 8 > max)
    {
        return ERR_EOF;
    }

    *d = *((double*)*pos);
    *pos += 8;
    return ERR_NONE;
}

AvroErrorCode read_float(uint8_t** pos, const uint8_t* max, float* f)
{
    if (*pos + 4 > max)
    {
        return ERR_EOF;
    }

    *f = *((float*)*pos);
    *pos += 4;
    return ERR_NONE;
}

AvroErrorCode read_int(uint8_t** pos, const uint8_t* max, long* i)
{
    int max_offset = (max - *pos < 5) ? (int)(max - *pos) : 5;
    int offset = 0;
    uint32_t value = 0;
    uint8_t b;

    do
    {
        if (offset == max_offset)
        {
            return (offset == 5) ? ERR_OVERFLOW : ERR_EOF;
        }

        b = (*pos)[offset];
        value |= (int64_t)(b & 0x7F) << (7 * offset);
        ++offset;
    }
    while (b & 0x80);

    *i = (int32_t)((value >> 1) ^ -(value & 1));
    *pos += offset;
    return ERR_NONE;
}

AvroErrorCode read_long(uint8_t** pos, const uint8_t* max, PY_LONG_LONG* l)
{
    int max_offset = (max - *pos < 10) ? (int)(max - *pos) : 10;
    int offset = 0;
    uint64_t value = 0;
    uint8_t b;

    do
    {
        if (offset == max_offset)
        {
            return (offset == 10) ? ERR_OVERFLOW : ERR_EOF;
        }

        b = (*pos)[offset];
        value |= (int64_t)(b & 0x7F) << (7 * offset);
        ++offset;
    }
    while (b & 0x80);

    *l = (int64_t)((value >> 1) ^ -(value & 1));
    *pos += offset;
    return ERR_NONE;
}

AvroErrorCode read_size(uint8_t** pos, const uint8_t* max, Py_ssize_t* l)
{
    PY_LONG_LONG temp_l;
    AvroErrorCode result;

    result = read_long(pos, max, &temp_l);

    if (result != ERR_NONE)
    {
        return result;
    }

    if (sizeof(Py_ssize_t) < sizeof(PY_LONG_LONG))
    {
        if (temp_l > PY_SSIZE_T_MAX || temp_l < PY_SSIZE_T_MIN)
        {
            return ERR_OVERFLOW;
        }
    }

    *l = (Py_ssize_t)temp_l;
    return ERR_NONE;
}

Py_ssize_t size_long(const PY_LONG_LONG l)
{
    Py_ssize_t len = 0;
    uint64_t n = (l << 1) ^ (l >> 63);

    while (n & ~0x7f)
    {
        ++len;
        n >>= 7;
    }

    return len + 1;
}

AvroErrorCode skip_bytes(uint8_t** pos, const uint8_t* max)
{
    Py_ssize_t temp_len;
    AvroErrorCode result;

    result = read_size(pos, max, &temp_len);

    if (result != ERR_NONE)
    {
        return result;
    }

    if (temp_len < 0)
    {
        return ERR_OVERFLOW;
    }

    if (*pos + temp_len > max)
    {
        return ERR_EOF;
    }

    *pos += temp_len;
    return ERR_NONE;
}

AvroErrorCode skip_char(uint8_t** pos, const uint8_t* max, const char expected)
{
    if (*pos >= max)
    {
        return ERR_EOF;
    }

    if (**pos != expected)
    {
        return ERR_OVERFLOW;
    }

    ++*pos;
    return ERR_NONE;
}

AvroErrorCode skip_double(uint8_t** pos, const uint8_t* max)
{
    if (*pos + 8 > max)
    {
        return ERR_EOF;
    }

    *pos += 8;
    return ERR_NONE;
}

AvroErrorCode skip_float(uint8_t** pos, const uint8_t* max)
{
    if (*pos + 4 > max)
    {
        return ERR_EOF;
    }

    *pos += 4;
    return ERR_NONE;
}

AvroErrorCode skip_int(uint8_t** pos, const uint8_t* max)
{
    long temp_int;

    return read_int(pos, max, &temp_int);
}

AvroErrorCode skip_long(uint8_t** pos, const uint8_t* max)
{
    PY_LONG_LONG temp_long;

    return read_long(pos, max, &temp_long);
}

AvroErrorCode skip_whitespace(uint8_t** pos, const uint8_t* max, const unsigned min_chars)
{
    uint8_t* org = *pos;

    while (*pos < max && (**pos == ' ' || **pos == 9 || **pos == 10 || **pos == 11 || **pos == 12 || **pos == 13))
    {
        ++*pos;
    }

    if (org + min_chars > *pos)
    {
        return (*pos == max) ? ERR_EOF : ERR_OVERFLOW;
    }

    return ERR_NONE;
}

AvroErrorCode write_boolean(uint8_t** pos, const uint8_t* max, const char b)
{
    if (*pos >= max)
    {
        return ERR_EOF;
    }

    if (b == 0)
    {
        **pos = 0;
    }
    else
    {
        **pos = 1;
    }

    ++*pos;
    return ERR_NONE;
}

AvroErrorCode write_bytes(uint8_t** pos, const uint8_t* max, const uint8_t* b, const Py_ssize_t len)
{
    AvroErrorCode result;

    result = write_size(pos, max, len);

    if (result != ERR_NONE)
    {
        return result;
    }

    if (*pos + len > max)
    {
        return ERR_EOF;
    }

    memcpy(*pos, b, len);
    *pos += len;
    return ERR_NONE;
}

AvroErrorCode write_char(uint8_t** pos, const uint8_t* max, const char c)
{
    if (*pos >= max)
    {
        return ERR_EOF;
    }

    **pos = c;
    ++*pos;
    return ERR_NONE;
}

AvroErrorCode write_digits(uint8_t** pos, const uint8_t* max, const int min_digits, const int i)
{
    int temp = i;
    int zeroes = min_digits;
    int digits = 0;
    uint8_t* digit_pos;

    while (zeroes > 0 && temp)
    {
        --zeroes;
        ++digits;
        temp /= 10;
    }

    while (temp)
    {
        ++digits;
        temp /= 10;
    }

    digits += zeroes;

    if (*pos + digits >= max)
    {
        return ERR_EOF;
    }

    *pos += digits;
    digit_pos = *pos - 1;

    temp = i;

    while (temp)
    {
        *digit_pos = (temp % 10) + '0';
        --digit_pos;
        temp /= 10;
    }

    while (zeroes > 0)
    {
        *digit_pos = '0';
        --zeroes;
        --digit_pos;
    }

    return ERR_NONE;
}

AvroErrorCode write_double(uint8_t** pos, const uint8_t* max, const double d)
{
    if (*pos + 8 > max)
    {
        return ERR_EOF;
    }

    *((double*)*pos) = d;
    *pos += 8;
    return ERR_NONE;
}

AvroErrorCode write_float(uint8_t** pos, const uint8_t* max, const float f)
{
    if (*pos + 4 > max)
    {
        return ERR_EOF;
    }

    *((float*)*pos) = f;
    *pos += 4;
    return ERR_NONE;
}

AvroErrorCode write_int(uint8_t** pos, const uint8_t* max, const long i)
{
    return write_long(pos, max, i);
}

AvroErrorCode write_long(uint8_t** pos, const uint8_t* max, const PY_LONG_LONG l)
{
    uint8_t buf[10];
    uint8_t bytes_written = 0;
    uint64_t n = (l << 1) ^ (l >> 63);

    while (n & ~0x7F)
    {
        buf[bytes_written++] = (((uint8_t)n) & 0x7F) | 0x80;
        n >>= 7;
    }

    buf[bytes_written++] = (uint8_t)n;

    if (*pos + bytes_written > max)
    {
        return ERR_EOF;
    }

    memcpy(*pos, &buf, bytes_written);
    *pos += bytes_written;
    return ERR_NONE;
}

AvroErrorCode write_size(uint8_t** pos, const uint8_t* max, const Py_ssize_t l)
{
    return write_long(pos, max, (PY_LONG_LONG)l);
}
