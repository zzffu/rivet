use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=src/jni.c");
    println!("cargo:rerun-if-env-changed=ANDROID_NDK_HOME");
    let target = env::var("TARGET").expect("Cargo target");
    assert!(
        target == "x86_64-linux-android" || target == "aarch64-linux-android",
        "the smoke library targets Android x86_64/arm64 only"
    );
    let ndk = PathBuf::from(
        env::var_os("ANDROID_NDK_HOME").expect("set ANDROID_NDK_HOME to NDK 30 or later"),
    );
    let host = if cfg!(windows) {
        "windows-x86_64"
    } else if cfg!(target_os = "macos") {
        "darwin-x86_64"
    } else {
        "linux-x86_64"
    };
    let toolchain = ndk.join("toolchains/llvm/prebuilt").join(host);
    let clang = toolchain
        .join("bin")
        .join(if cfg!(windows) { "clang.exe" } else { "clang" });
    let object = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("jni.o");
    let status = Command::new(clang)
        .arg(format!("--target={target}23"))
        .arg(format!("--sysroot={}", toolchain.join("sysroot").display()))
        .args(["-fPIC", "-O2", "-c", "src/jni.c", "-o"])
        .arg(&object)
        .status()
        .expect("run the NDK C compiler");
    assert!(status.success(), "JNI bridge compilation failed");
    println!("cargo:rustc-link-arg={}", object.display());
    println!("cargo:rustc-link-arg=-Wl,-z,max-page-size=16384");
    println!("cargo:rustc-link-arg=-Wl,-z,common-page-size=16384");
}
