typedef int bool;
#define TRUE 1
#define FALSE 0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stddef.h>
#include "crc32.c"

#include <stdint.h>
// #include <arm_neon.h>

uint16_t reverseShortBytes(uint16_t i)
{
    // return ((i & 0xFF00) >> 8) | (i << 8);
    return __builtin_bswap16(i);
}

uint32_t reverseIntBytes(uint32_t i)
{
    // return ((i >> 24)           ) |
    //            ((i >>   8) &   0xFF00) |
    //            ((i <<   8) & 0xFF0000) |
    //            ((i << 24));
    return __builtin_bswap32(i);
}

uint64_t reverseLongBytes(uint64_t i)
{
    // i = (i & 0x00ff00ff00ff00ffL) << 8 | (i >> 8) & 0x00ff00ff00ff00ffL;
    // return (i << 48) | ((i & 0xffff0000L) << 16) |
    //     ((i >> 16) & 0xffff0000L) | (i >> 48);
    return __builtin_bswap64(i);
}

int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
    int bornhostLength = (sysFlag & 16) == 0 ? 8 : 20;
    int storehostAddressLength = (sysFlag & 32) == 0 ? 8 : 20;
    const int msgLen = 4 //TOTALSIZE
        + 4 //MAGICCODE
        + 4 //BODYCRC
        + 4 //QUEUEID
        + 4 //FLAG
        + 8 //QUEUEOFFSET
        + 8 //PHYSICALOFFSET
        + 4 //SYSFLAG
        + 8 //BORNTIMESTAMP
        + bornhostLength //BORNHOST
        + 8 //STORETIMESTAMP
        + storehostAddressLength //STOREHOSTADDRESS
        + 4 //RECONSUMETIMES
        + 8 //Prepared Transaction Offset
        + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
        + 1 + topicLength //TOPIC
        + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
        + 0;
    return msgLen;
}

long handle(unsigned char* in, int in_size, unsigned char* out, int topicLen, int batchPropLen, int sysFlag, int queueId, long bornTimestamp){
    unsigned char* buf = out;

    int bornhostLength = (sysFlag & 16) == 0 ? 8 : 20;
    int storehostAddressLength = (sysFlag & 32) == 0 ? 8 : 20;
    int paramSize = topicLen + batchPropLen + bornhostLength + storehostAddressLength;
    int bufPtr = paramSize;

    char* topicStr = out;
    char* batchPropStr = topicStr+topicLen;
    unsigned char* bornHostBytes = batchPropStr+batchPropLen;
    unsigned char* storeHostBytes = bornHostBytes+bornhostLength;

    // int queueId = 15;
    // uint64_t bornTimestamp = 1685954820835L;
    

    unsigned char* currentPtr = in;
    int currentPtrPos = 0;
    int batchSize = 0;
    while(currentPtrPos < in_size){
        batchSize++;
        int totalSize   = reverseIntBytes(*(int*)(currentPtr+currentPtrPos));
        // currentPtrPos += 4;
        int magicCode   = reverseIntBytes(*(int*)(currentPtr+currentPtrPos+4));
        // currentPtrPos += 4;
        int bodyCrc     = reverseIntBytes(*(int*)(currentPtr+currentPtrPos+8));
        // currentPtrPos += 4;
        int flag        = reverseIntBytes(*(int*)(currentPtr+currentPtrPos+12));
        // currentPtrPos += 4;
        int bodyLen     = reverseIntBytes(*(int*)(currentPtr+currentPtrPos+16));
        // currentPtrPos += 4;
        int bodyPos = currentPtrPos+20;
        bodyCrc = crc32_2(currentPtr+bodyPos, bodyLen);
        // currentPtrPos += 20;
        // currentPtrPos += bodyLen;
        uint16_t propertiesLen = reverseShortBytes(*(uint16_t*)(currentPtr+currentPtrPos+20+bodyLen));
        // currentPtrPos += 2;
        currentPtrPos += propertiesLen+22+bodyLen;
        int propertiesPos = currentPtrPos-propertiesLen;
        bool needAppendLastPropertySeparator = propertiesLen > 0 && batchPropLen > 0 && *(currentPtr+currentPtrPos-1) != 2;

        int totalPropLen = needAppendLastPropertySeparator ? propertiesLen + batchPropLen + 1
                    : propertiesLen + batchPropLen;
        int msgLen = calMsgLength(sysFlag, bodyLen, topicLen, totalPropLen);

        
        // 1 TOTALSIZE
        *(uint32_t*)(buf+bufPtr) = reverseIntBytes(msgLen);
        // bufPtr+=4;
        // 2 MAGICCODE
        *(uint32_t*)(buf+bufPtr+4) = reverseIntBytes(-626843481);
        // bufPtr+=4;
        // 3 BODYCRC
        *(uint32_t*)(buf+bufPtr+8) = reverseIntBytes(bodyCrc);
        // bufPtr+=4;
        // 4 QUEUEID
        *(uint32_t*)(buf+bufPtr+12) = reverseIntBytes(queueId);
        // bufPtr+=4;
        // 5 FLAG
        *(uint32_t*)(buf+bufPtr+16) = reverseIntBytes(flag);
        // bufPtr+=4;
        // 6 QUEUEOFFSET
        *(uint64_t*)(buf+bufPtr+20) = 0;
        // bufPtr+=8;
        // 7 PHYSICALOFFSET
        *(uint64_t*)(buf+bufPtr+28) = 0;
        // bufPtr+=8;
        // 8 SYSFLAG
        *(uint32_t*)(buf+bufPtr+36) = reverseIntBytes(sysFlag);
        // bufPtr+=4;
        // 9 BORNTIMESTAMP
        *(uint64_t*)(buf+bufPtr+40) = reverseLongBytes(bornTimestamp);
        // bufPtr+=8;

        // 10 BORNHOST
        *(uint64_t*)(buf+bufPtr+48) = *(uint64_t*)bornHostBytes;
        // bufPtr+=8;

        // 11 STORETIMESTAMP
        *(uint64_t*)(buf+bufPtr+56) = 0;
        // bufPtr+=8;

        // 12 STOREHOSTADDRESS
        *(uint64_t*)(buf+bufPtr+64) = *(uint64_t*)storeHostBytes;
        // bufPtr+=8;

        // 13 RECONSUMETIMES
        *(uint32_t*)(buf+bufPtr+72) = reverseIntBytes(0);
        // bufPtr+=4;
        // 14 Prepared Transaction Offset, batch does not support transaction
        *(uint64_t*)(buf+bufPtr+76) = 0;
        // bufPtr+=8;
        // 15 BODY
        *(uint32_t*)(buf+bufPtr+84) = reverseIntBytes(bodyLen);
        // bufPtr+=84;
        // bufPtr+=4;
        if (bodyLen > 0){
            memcpy(buf+bufPtr+88, currentPtr+bodyPos, bodyLen);
        }
        bufPtr+=88+bodyLen;

        // 16 TOPIC
        *(buf+bufPtr) = (unsigned char)topicLen;
        bufPtr+=1;
        memcpy(buf+bufPtr, topicStr, topicLen);
        bufPtr+=topicLen;
        // 17 PROPERTIES
        *(uint16_t*)(buf+bufPtr) = reverseShortBytes(totalPropLen);
        bufPtr+=2;
        if (propertiesLen > 0) {
            memcpy(buf+bufPtr, currentPtr+propertiesPos, propertiesLen);
            bufPtr+=propertiesLen;
        }
        if (batchPropLen > 0) {
            if (needAppendLastPropertySeparator) {
                *(buf+bufPtr) = (unsigned char)2;
                bufPtr+=1;
            }
            memcpy(buf+bufPtr, batchPropStr, batchPropLen);
            bufPtr+=batchPropLen;
        }
    }
    long dataSize = bufPtr-paramSize;
    long ret = dataSize<<32|batchSize;
    return ret;
}