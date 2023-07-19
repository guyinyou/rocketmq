#include <arm_neon.h>
#include <arm_acle.h>
#include "ptg_optimize.h"

uint32_t crc32(const char* s, int len){

    uint32_t crc = 0xffffffff;
    size_t i = 0;
    for(int i = 0; i < len; i++){
        uint8_t byte = s[i];
        crc = crc ^ byte;
        for (uint8_t j = 0; j < 8; ++j)
        {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return (crc ^ 0xffffffff) & 0x7FFFFFFF;
}

uint32_t crc32_2(const char* s, int len){

    uint32_t crc = 0xffffffff;
    size_t i = 0;


    // for(int i = 0; i < len; i++){
    //     uint8_t byte = s[i];
    //     crc = __crc32b(crc, (uint8_t)byte);
    // }

    char* ptr = (char*)s;
    const char* end = s + len;
    
    while(ptr + 8 <= end){
        crc = __crc32d(crc, *(uint64_t*)ptr);
        ptr += 8;
    }

    while(ptr + 4 <= end){
        crc = __crc32w(crc, *(uint32_t*)ptr);
        ptr += 4;
    }

    while(ptr + 2 <= end){
        crc = __crc32h(crc, *(uint16_t*)ptr);
        ptr += 2;
    }

    while(ptr + 1 <= end){
        crc = __crc32b(crc, *(uint8_t*)ptr);
        ptr++;
    }

    // __crc32b 8
    // __crc32h 16
    // __crc32w 32
    // __crc32d 64
    return (crc ^ 0xffffffff) & 0x7FFFFFFF;
}

#define CRC32CX(crc, value) __asm__("crc32cx %w[c], %w[c], %x[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CW(crc, value) __asm__("crc32cw %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CH(crc, value) __asm__("crc32ch %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))
#define CRC32CB(crc, value) __asm__("crc32cb %w[c], %w[c], %w[v]":[c]"+r"(crc):[v]"r"(value))

uint32_t easy_docrc32c_arm64_opt(uint32_t crc, const void *data, size_t len)
{
    int64_t length = (int64_t)len;
    const uint8_t *p = (const uint8_t*)(data);

    for (; length > 2048; length -= 8 * sizeof(uint64_t)) {
        asm volatile ("PRFM PLDL1STRM,[%0]"::"r"(p + 2048));
        CRC32CX(crc, *((uint64_t *)p));
        CRC32CX(crc, *((uint64_t *)p + 1));
        CRC32CX(crc, *((uint64_t *)p + 2));
        CRC32CX(crc, *((uint64_t *)p + 3));
        CRC32CX(crc, *((uint64_t *)p + 4));
        CRC32CX(crc, *((uint64_t *)p + 5));
        CRC32CX(crc, *((uint64_t *)p + 6));
        CRC32CX(crc, *((uint64_t *)p + 7));
        p += 8 * sizeof(uint64_t);
    }
    for (; length >= 8 * sizeof(uint64_t); length -= 8 * sizeof(uint64_t)) {
        CRC32CX(crc, *((uint64_t *)p));
        CRC32CX(crc, *((uint64_t *)p + 1));
        CRC32CX(crc, *((uint64_t *)p + 2));
        CRC32CX(crc, *((uint64_t *)p + 3));
        CRC32CX(crc, *((uint64_t *)p + 4));
        CRC32CX(crc, *((uint64_t *)p + 5));
        CRC32CX(crc, *((uint64_t *)p + 6));
        CRC32CX(crc, *((uint64_t *)p + 7));
        p += 8 * sizeof(uint64_t);
    }

    while ((length -= sizeof(uint64_t)) >= 0) {
        CRC32CX(crc, *((uint64_t *)p));
        p += sizeof(uint64_t);
    }

    if (length & sizeof(uint32_t)) {
        CRC32CW(crc, *((uint32_t *)p));
        p += sizeof(uint32_t);
    }

    if (length & sizeof(uint16_t)) {
        CRC32CH(crc, *((uint16_t *)p));
        p += sizeof(uint16_t);
    }

    if (length & sizeof(uint8_t)) {
        CRC32CB(crc, *p);
    }

    return crc;
}