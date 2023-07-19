#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

int32_t jhash(const char* str, int len){
    int32_t h = 0;
    for(int i = 0; i < len; i++){
        h = (h<<5) - h + (str[i] & 0xff);
    }
    return h;
}

// int main(){
//     printf("%d\n", jhash("code", 4));
//     printf("%d\n", jhash("version", 7));
//     printf("%d\n", jhash("opaque", 6));
//     printf("%d\n", jhash("flag", 4));

//     printf("%d\n", jhash("KEYS", 4));
//     printf("%d\n", jhash("UNIQ_KEY", 8));
//     printf("%d\n", jhash("DUP_INFO", 8));
//     printf("%d\n", jhash("TAGS", 4));
//     printf("%d\n", jhash("DELAY", 5));
//     printf("%d\n", jhash("SCHEDULE_TOPIC_XXXX", 19));

//     // KEYS 2303476
//     // UNIQ_KEY 488471169
//     // DUP_INFO -1239261426
//     // TAGS 2567193
//     // DELAY 64930147
//     // SCHEDULE_TOPIC_XXXX -1995822632
// }