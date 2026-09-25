param(
    [ValidateSet('x86_64', 'arm64-v8a')][string]$Abi = 'x86_64',
    [string]$Sdk = "$env:LOCALAPPDATA/Android/Sdk",
    [string]$NdkVersion = '30.0.16248370',
    [string]$Platform = 'android-37.2',
    [string]$BuildTools = '37.0.0'
)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$out = Join-Path $root "build/$Abi"
$ndk = Join-Path $Sdk "ndk/$NdkVersion"
$tools = Join-Path $Sdk "build-tools/$BuildTools"
$androidJar = Join-Path $Sdk "platforms/$Platform/android.jar"
$toolchain = Join-Path $ndk 'toolchains/llvm/prebuilt/windows-x86_64'
$target = if ($Abi -eq 'x86_64') { 'x86_64-linux-android' } else { 'aarch64-linux-android' }
$javaBin = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin' } else { '' }
function JavaTool([string]$name) {
    if ($javaBin) { return (Join-Path $javaBin "$name.exe") }
    return "$name.exe"
}
function Invoke-Checked([string]$program, [string[]]$arguments) {
    & $program @arguments
    if ($LASTEXITCODE -ne 0) { throw "$program failed with exit code $LASTEXITCODE" }
}
foreach ($directory in @($out, "$out/classes", "$out/dex")) {
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
}
foreach ($required in @($androidJar, "$tools/aapt2.exe", "$toolchain/bin/clang.exe")) {
    if (-not (Test-Path $required)) { throw "Required Android tool missing: $required" }
}

# Changes apply only to this build process. No AVD, global SDK, or VPN is touched.
$oldNdk = $env:ANDROID_NDK_HOME
$oldTargetDir = $env:CARGO_TARGET_DIR
$linkerName = 'CARGO_TARGET_' + $target.Replace('-', '_').ToUpperInvariant() + '_LINKER'
$oldLinker = [Environment]::GetEnvironmentVariable($linkerName, 'Process')
try {
    $env:ANDROID_NDK_HOME = $ndk
    $env:CARGO_TARGET_DIR = Join-Path $root 'build/cargo'
    [Environment]::SetEnvironmentVariable($linkerName, "$toolchain/bin/${target}29-clang.cmd", 'Process')
    Invoke-Checked 'cargo' @('build', '--manifest-path', "$root/native/Cargo.toml", '--target', $target, '--release')
    Invoke-Checked (JavaTool 'javac') @('--release', '17', '-classpath', $androidJar,
        '-d', "$out/classes", "$root/app/src/main/java/dev/rivet/smoke/MainActivity.java",
        "$root/app/src/main/java/dev/rivet/smoke/SmokeResult.java")
    Invoke-Checked (JavaTool 'jar') @('--create', '--file', "$out/classes.jar", '-C', "$out/classes", '.')
    Invoke-Checked "$tools/d8.bat" @('--lib', $androidJar, '--min-api', '29', '--output', "$out/dex", "$out/classes.jar")
    Invoke-Checked "$tools/aapt2.exe" @('link', '--manifest', "$root/app/src/main/AndroidManifest.xml",
        '-I', $androidJar, '--min-sdk-version', '29', '--target-sdk-version', '37', '-o', "$out/base.apk")
    Copy-Item "$out/base.apk" "$out/unaligned.apk" -Force
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::Open("$out/unaligned.apk", [System.IO.Compression.ZipArchiveMode]::Update)
    try {
        [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, "$out/dex/classes.dex", 'classes.dex', [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
    } finally { $zip.Dispose() }
    # .NET Framework's NoCompression still emits DEFLATE. Android requires a
    # genuinely STORED entry when extractNativeLibs=false.
    $nativeRoot = Join-Path $out 'native-apk'
    $nativeDirectory = Join-Path $nativeRoot "lib/$Abi"
    [System.IO.Directory]::CreateDirectory($nativeDirectory) | Out-Null
    Copy-Item "$env:CARGO_TARGET_DIR/$target/release/librivet_android_smoke.so" "$nativeDirectory/librivet_android_smoke.so" -Force
    Invoke-Checked (JavaTool 'jar') @('--update', '--file', "$out/unaligned.apk", '--no-compress',
        '-C', $nativeRoot, "lib/$Abi/librivet_android_smoke.so")
    Invoke-Checked "$tools/zipalign.exe" @('-P', '16', '-f', '4', "$out/unaligned.apk", "$out/aligned.apk")
    $keystore = Join-Path $root 'build/smoke.keystore'
    if (-not (Test-Path $keystore)) {
        Invoke-Checked (JavaTool 'keytool') @('-genkeypair', '-keystore', $keystore, '-alias', 'smoke',
            '-storepass', 'android', '-keypass', 'android', '-keyalg', 'RSA', '-keysize', '2048', '-validity', '3650',
            '-dname', 'CN=Rivet isolated verification', '-noprompt')
    }
    $apk = "$out/rivet-smoke.apk"
    Invoke-Checked "$tools/apksigner.bat" @('sign', '--ks', $keystore, '--ks-key-alias', 'smoke', '--ks-pass', 'pass:android',
        '--key-pass', 'pass:android', '--min-sdk-version', '29', '--out', $apk, "$out/aligned.apk")
    Invoke-Checked "$tools/zipalign.exe" @('-c', '-P', '16', '4', $apk)
    Invoke-Checked "$tools/apksigner.bat" @('verify', '--verbose', $apk)
    Write-Output "APK: $apk"
    Write-Output 'Launch dev.rivet.smoke/.MainActivity in an isolated emulator; results appear on screen and files/smoke-result.json.'
} finally {
    $env:ANDROID_NDK_HOME = $oldNdk
    $env:CARGO_TARGET_DIR = $oldTargetDir
    [Environment]::SetEnvironmentVariable($linkerName, $oldLinker, 'Process')
}
