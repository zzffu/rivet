mod smoke;

use std::ffi::{CString, c_char, c_void};

unsafe extern "C" {
    fn rivet_smoke_new_string(env: *mut c_void, json: *const c_char) -> *mut c_void;
}

#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_dev_rivet_smoke_MainActivity_runNative(
    env: *mut c_void,
    _class: *mut c_void,
    active_network: i64,
) -> *mut c_void {
    let report = std::panic::catch_unwind(|| smoke::json(active_network as u64)).unwrap_or_else(|_| {
        "{\"status\":\"failed\",\"stage\":\"native-panic\",\"error\":\"native smoke panicked; consult logcat for the panic\"}".to_owned()
    });
    // The JSON writer escapes all NUL and non-ASCII code points. The C adapter
    // uses the installed NDK jni.h rather than guessed JNI table offsets.
    let report = CString::new(report).expect("JSON cannot contain a raw NUL");
    unsafe { rivet_smoke_new_string(env, report.as_ptr()) }
}
