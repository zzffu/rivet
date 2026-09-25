#include <jni.h>

/* The native suite emits ASCII JSON, which is also valid modified UTF-8. */
jstring rivet_smoke_new_string(JNIEnv *env, const char *json) {
    return (*env)->NewStringUTF(env, json);
}
