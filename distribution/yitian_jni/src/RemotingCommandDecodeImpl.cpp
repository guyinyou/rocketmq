#include "jni.h"
#include "org_apache_rocketmq_remoting_protocol_RemotingCommandCodec.h"
#include "stdio.h"
#include <vector>
#include <iostream>
#include <string>
#include <string_view>
#include <cstring>
#include <fstream>
#include "utils.c"

JNIEXPORT void JNICALL Java_org_apache_rocketmq_remoting_protocol_RemotingCommandCodec_decodeRemotingCommand
  (JNIEnv *env, jclass classz, jlong dataAddr, jint dataLen, jlong outAddr){
    char* data = (char*)dataAddr;
    char* out = (char*)outAddr;

    bool isKey = true;

    std::string_view key;
    int extFieldsCnt = 0;
    int remarkPosition = -1;
    int remarkLen = 0;
    int extFieldsPosition = -1;
    int extFieldsLen = 0;
    for(int i = 1; i < dataLen; i++){
        char c = data[i];
        switch(c){
            case '"':
                {
                    int headIndex = i;
                    int endIndex = i+1;
                    //加速到字符串结束
                    for( ; endIndex < dataLen; endIndex++){
                        char tc = data[endIndex];
                        if(tc == '\\'){
                            endIndex++;
                            continue;
                        }
                        if(tc == '"'){
                            break;
                        }
                    }
                    int size = endIndex-headIndex-1;
                    std::string_view str(data+headIndex+1, size);
                    if(isKey){
                      key = str;
                    }else{
                      if(key == "remark"){
                        remarkLen = size;
                        remarkPosition = headIndex+1;
                      }
                    }
                    i = endIndex;
                }
            break;
            case ':':
            case ',':
                isKey = !isKey;
            break;
            case '{':
                {
                    int headIndex = i;
                    int endIndex = i+1;
                    int cnt = 0;
                    //加速到map结束
                    for( ; endIndex < dataLen; endIndex++){
                        char tc = data[endIndex];
                        if(tc == '\\'){
                            endIndex++;
                            continue;
                        }
                        //统计字符串个数
                        if(tc == '"'){
                          cnt++;
                        }
                        if(tc == '}'){
                            break;
                        }
                    }
                    extFieldsCnt = cnt/2;
                    int size = endIndex-headIndex-1;
                    std::string_view str(data+headIndex+1, size);
                    if(!isKey){
                      if(key == "extFields"){
                        extFieldsLen = size;
                        extFieldsPosition = headIndex+1;
                      }
                    }
                    i = endIndex;
                }
            break;
            default:
            {
                if(c >= '0' && c <= '9'){
                    uint32_t num = c - '0';

                    int headIndex = i;
                    int endIndex = i+1;
                    //加速到map结束
                    char tc = data[endIndex];
                    while(tc >= '0' && tc <= '9'){
                        num = num * 10 + tc - '0';
                        endIndex++;
                        tc = data[endIndex];
                    }
                    /*
                      通过hash值直接比较，仅在已知key的哈希不冲突情况下可用
                      code: 3059181
                      version: 351608024
                      opaque: -1010695135
                      flag: 3145580
                    */
                    if(!isKey){
                      switch(jhash(key.data(), key.length())){
                        case 3059181:
                          *(uint16_t*)(out) = ((uint16_t)num);
                        break;
                        case 351608024:
                          *(uint16_t*)(out+3) = ((uint16_t)num);
                        break;
                        case -1010695135:
                          *(uint32_t*)(out+5) = ((uint32_t)num);
                        break;
                        case 3145580:
                          *(uint32_t*)(out+9) = ((uint32_t)num);
                        break;
                      }
                    }
                    i = endIndex-1;
                }
            }
        }
    }
    *(uint32_t*)(out+13) = ((uint32_t)remarkLen);
    *(uint32_t*)(out+17) = ((uint32_t)remarkPosition);

    *(uint32_t*)(out+21) = ((uint32_t)extFieldsLen);
    *(uint32_t*)(out+25) = ((uint32_t)extFieldsPosition);

    /*
    0
    ┌──────────┬──────────────┬─────────────┬─────────────┐
    │ code (2B)│ language (1B)│ version (2B)│ opaque (4B) │
    └──────────┴──────────────┴─────────────┴─────────────┘
    9
    ┌──────────┬────────────────┬─────────────────────┐
    │ flag (4B)│ remarkLen (4B) | remarkPosition (4B) │
    └──────────┴────────────────┴─────────────────────┘
    21
    ┌──────────────────┬────────────────────────┐
    │ extFieldsLen (4B)│ extFieldsPosition (4B) |
    └──────────────────┴────────────────────────┘
    */
    
}