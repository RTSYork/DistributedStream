#define _GNU_SOURCE

#include <jni.h>
#include <pthread.h>

#include "ThreadAffinity.h"

JNIEXPORT void JNICALL Java_ThreadAffinity_setThreadAffinity(JNIEnv *env, jobject obj, jobject bits)
{
	jclass BitSet = (*env)->GetObjectClass(env, bits);
	jmethodID nextSetBit = (*env)->GetMethodID(env, BitSet, "nextSetBit", "(I)I");
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	jint bit = 0;
	while ((bit = (*env)->CallIntMethod(env, bits, nextSetBit, bit)) != -1)
		CPU_SET(bit++, &cpuset);
	if (pthread_setaffinity_np(pthread_self(), sizeof (cpu_set_t), &cpuset) != 0)
		printf("pthread_setaffinity_np FAILED\n");
}
