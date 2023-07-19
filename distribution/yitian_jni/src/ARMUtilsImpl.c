#include "jni.h"
#include "org_apache_rocketmq_store_util_ARMUtils.h"
#include <arm_neon.h>
#include <arm_acle.h>
#include <inttypes.h>
#include "MsgEncode.c"

JNIEXPORT jlong JNICALL Java_org_apache_rocketmq_store_util_ARMUtils_encode
  (JNIEnv *env, jclass classz, jbyteArray inArray, jlong outAddr, jint topicLen, jint batchPropLen, jint sysFlag, jint queueId, jlong bornTimestamp){
    jboolean isCopy = JNI_TRUE;
    jbyte* in = (jbyte*)((*env)->GetPrimitiveArrayCritical(env, inArray, NULL));
    int inlen = (*env)->GetArrayLength(env, inArray);//获取数组长度

    unsigned char* out = (unsigned char*)outAddr;
    long ret = handle(in, inlen, out, topicLen, batchPropLen, sysFlag, queueId, bornTimestamp);
    (*env)->ReleasePrimitiveArrayCritical(env, inArray, in, JNI_ABORT);//释放数组
    return ret;
  }