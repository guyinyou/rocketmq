#include "jni.h"
#include "org_apache_rocketmq_common_utils_ARMSpinLock.h"
#include <unistd.h>
#include "ptg_optimize.h"
#include <time.h>
#include <pthread.h>

#define CAS(a_ptr, a_oldVal, a_newVal) __sync_bool_compare_and_swap(a_ptr, a_oldVal, a_newVal)

JNIEXPORT jboolean JNICALL Java_org_apache_rocketmq_common_utils_ARMSpinLock_lock
  (JNIEnv *env, jobject obj, jlong addr){
    int cnt = 0;
    int cnt2 = 0;
    int* cmpCnt = (int*)(addr+16);
    int* yiedCnt = (int*)(addr+32);
    int n = 0;
    if((*cmpCnt) > 0){
      if(*yiedCnt < 1){
        *yiedCnt = 1;
      }
      n = (int)((double)(*cmpCnt) / (double)(*yiedCnt));
    }
    n = 100000000;
    while(CAS((uint32_t*)(addr), 0, 1) != true){
        if((++cnt % n) == 0){
          cnt2++;
          sched_yield();
          // usleep(0);
        }
    }
    double bl = (double)(*cmpCnt) / (double)cnt;
    if(bl < 1){
      cnt2 /= 2;
    }else if(bl > 1){
      cnt2 *= 2;
    }
    *cmpCnt = (int)((double)(*cmpCnt) * 0.999 + (double)cnt * 0.001);
    *yiedCnt = (int)((double)(*yiedCnt) * 0.999 + (double)cnt2 * 0.001);
    return JNI_TRUE;
  }

/*
 * Class:     org_apache_rocketmq_common_utils_ARMSpinLock
 * Method:    unlock
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_rocketmq_common_utils_ARMSpinLock_unlock
  (JNIEnv *env, jobject obj, jlong addr){
    CAS((uint32_t*)(addr), 1, 0);
  }

// int main(){
//     clock_t start_time = clock();
//     int data = 0;
//     for(int i = 0; i < 1000000; i++){
//         CAS(&data, 1, 2);
//         // sched_yield();
//     }

//     clock_t end_time = clock();
//     double duration = (double)(end_time - start_time) / CLOCKS_PER_SEC;
//     printf("Code block duration: %lf seconds.\n", duration);
// }