#!/usr/bin/env python3
"""Prepare and run signed, diskless Linux x86_64 verification guests.

Run under Debian 13 / WSL Debian, with python3, curl, dpkg-deb and sqv already
available. `prepare` downloads pinned packages and extracts them to --state;
it never installs packages, runs package scripts, builds code, or boots a VM.
`run` accepts one existing static x86_64 Linux ELF; `run-suite` accepts a JSON
array of {name, executable, args} cases and executes them in one guest boot.
Every payload is hashed and checked independently. The selected kernel release,
guest configuration, all case results and the final shutdown must match.
No host directory or disk is attached to the guest, and no host network
interface, TAP, forwarding rule, sysctl or boot setting is changed.

Windows commands (from the repository root; build only after integration):
  wsl -d Debian -- python3 /mnt/c/project/rivet/tools/verification/linux_vm.py --kernel 6.18 prepare
  rustup target add x86_64-unknown-linux-musl
  # Use rust-lld and -C target-feature=+crt-static for the parent-owned smoke build.
  # In PowerShell, set this only for the build process, not a global Cargo config:
  # $env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = 'rust-lld'
  # cargo build --target x86_64-unknown-linux-musl --tests --examples
  # Remove-Item Env:CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER
  wsl -d Debian -- python3 /mnt/c/project/rivet/tools/verification/linux_vm.py --kernel 6.18 run --accel tcg /mnt/c/PATH/TO/STATIC-SMOKE

KVM is the default accelerator and never silently falls back to TCG. Check
the current /dev/kvm availability and permissions; historical host access is
not evidence of current access. Explicit TCG needs no host virtualization change.
Kernel selectors cover 6.6, 6.12, 6.18 (default) and the retained 7.2.7 guest.

All downloads and run reports live in --state (default:
~/.cache/rivet-verification/linux-<selector>). No Cargo or kernel build is
performed by this script. A passing loopback run is not NIC RX zero-copy,
ordinary-user permission coverage or performance proof.
"""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
STATE_ROOT = Path.home() / ".cache/rivet-verification"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def checked(command, timeout=120, env=None):
    return subprocess.run(
        [str(arg) for arg in command], check=True, timeout=timeout,
        capture_output=True, text=True, env=env,
    ).stdout.strip()


def download(artifact, directory):
    path = directory / artifact["file"]
    if path.exists():
        if sha256(path) != artifact["sha256"]:
            raise RuntimeError(f"Cached checksum mismatch: {path}; refusing to overwrite")
        return path
    partial = path.with_name(path.name + ".partial")
    checked([
        "curl", "--fail", "--silent", "--show-error", "--location",
        "--proto", "=https", "--proto-redir", "=https",
        "--connect-timeout", "20", "--max-time", "600",
        "--retry", "2", "--retry-all-errors", "--retry-delay", "1", "--retry-max-time", "600",
        "--output", partial, artifact["url"],
    ], timeout=620)
    if sha256(partial) != artifact["sha256"]:
        raise RuntimeError(f"Downloaded checksum mismatch: {partial}")
    partial.rename(path)
    return path


def tool_environment(state):
    root = state / "tools"
    environment = os.environ.copy()
    # Only the child process sees these paths. Never mutate the host loader config.
    environment["LD_LIBRARY_PATH"] = ":".join(str(root / item) for item in (
        "usr/lib/x86_64-linux-gnu", "usr/lib", "lib/x86_64-linux-gnu", "lib",
    ))
    return environment


def static_x86_64(path):
    """Reject Windows, wrong-architecture, and dynamic-loader-dependent inputs."""
    with path.open("rb") as source:
        header = source.read(64)
        if len(header) != 64 or header[:6] != b"\x7fELF\x02\x01":
            raise RuntimeError(f"Not a little-endian ELF64 executable: {path}")
        kind, machine = struct.unpack_from("<HH", header, 16)
        if kind not in (2, 3) or machine != 62:
            raise RuntimeError(f"Not an x86_64 ELF executable: {path}")
        table_offset = struct.unpack_from("<Q", header, 32)[0]
        entry_size, count = struct.unpack_from("<HH", header, 54)
        if entry_size < 56 or count == 0 or count > 1024:
            raise RuntimeError(f"Invalid ELF program headers: {path}")
        for index in range(count):
            source.seek(table_offset + index * entry_size)
            entry = source.read(56)
            if len(entry) != 56:
                raise RuntimeError(f"Truncated ELF program header: {path}")
            if struct.unpack_from("<I", entry)[0] == 3:  # PT_INTERP
                raise RuntimeError(f"Dynamic ELF needs a loader; supply a static musl binary: {path}")


def prepare(state, kernel):
    for tool in ("curl", "dpkg-deb", "sqv"):
        if shutil.which(tool) is None:
            raise RuntimeError(f"Missing host prerequisite: {tool}; no host package changes made")
    tools = load_json(HERE / "debian-tools.lock.json")
    downloads = state / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    root = state / "tools"
    root.mkdir(exist_ok=True)
    marker = root / ".rivet-packages.json"
    manifest_hash = sha256(HERE / "debian-tools.lock.json")
    if marker.exists() and load_json(marker)["manifest_sha256"] != manifest_hash:
        raise RuntimeError("Tool lock changed; use a new --state directory instead of mixing tools")
    for package in tools["packages"]:
        archive = download(package, downloads)
        if not marker.exists():
            checked(["dpkg-deb", "--extract", archive, root])
        print(f"Verified tool package: {package['package']}={package['version']}", flush=True)
    save_json(marker, {"manifest_sha256": manifest_hash})
    image_package = download(kernel["package"], downloads)
    signature = download(kernel["signature"], downloads)
    certificate = download(kernel["certificate"], downloads)
    fingerprint = checked([
        "sqv", "--keyring", certificate, "--signature-file", signature, image_package,
    ])
    if fingerprint.splitlines() != [kernel["certificate"]["fingerprint"]]:
        raise RuntimeError(f"Kernel signature was not made by the pinned certificate: {fingerprint}")
    environment = tool_environment(state)
    zstd = root / "usr/bin/zstd"
    image_directory = state / "kernel"
    image_directory.mkdir(exist_ok=True)
    image = image_directory / "vmlinuz"
    # Only selected regular files are read from the authenticated package. No
    # package hooks, modules, symlinks or absolute archive paths are installed.
    with tempfile.TemporaryDirectory(prefix="kernel-unpack-", dir=state) as temp:
        unpacked = Path(temp) / "kernel.tar"
        checked([zstd, "--quiet", "--decompress", "-o", unpacked, image_package],
                timeout=180, env=environment)
        with tarfile.open(unpacked, "r:") as archive:
            images = [member for member in archive if member.name.endswith("/vmlinuz")]
            expected = f"usr/lib/modules/{kernel['release']}/vmlinuz"
            if len(images) != 1 or images[0].name.lstrip("./") != expected or not images[0].isfile():
                raise RuntimeError("Signed package does not contain the expected unique kernel image")
            with archive.extractfile(images[0]) as source, image.open("wb") as target:
                shutil.copyfileobj(source, target)
            for name in (".PKGINFO", ".BUILDINFO"):
                member = archive.getmember(name)
                if not member.isfile():
                    raise RuntimeError(f"Invalid package metadata: {name}")
                with archive.extractfile(member) as source:
                    (image_directory / name.lstrip(".")).write_bytes(source.read())
    busybox = root / "usr/bin/busybox"
    if not busybox.is_file():
        busybox = root / "bin/busybox"
    static_x86_64(busybox)
    qemu = root / "usr/bin/qemu-system-x86_64"
    bios = root / "usr/share/seabios/bios-256k.bin"
    if not bios.is_file():
        raise RuntimeError("Pinned SeaBIOS package did not provide bios-256k.bin")
    qemu_version = checked([qemu, "--version"], env=environment)
    prepared = {
        "kernel": kernel,
        "kernel_image_sha256": sha256(image),
        "busybox": str(busybox.relative_to(state)),
        "busybox_sha256": sha256(busybox),
        "qemu_sha256": sha256(qemu),
        "qemu_version": qemu_version,
        "bios_sha256": sha256(bios),
        "tools_manifest_sha256": manifest_hash,
        "signature_verifier": checked(["sqv", "--version"]),
        "verified_signer": fingerprint,
        "host_uname": list(platform.uname()),
        "guest_booted": False,
    }
    save_json(state / "prepared.json", prepared)
    print(json.dumps(prepared, indent=2))
    print(f"Prepared artifacts only; no guest was booted. State: {state}")
    return 0


def cpio_entry(target, inode, name, mode, payload=b"", device=(0, 0)):
    name_bytes = name.encode("utf-8") + b"\0"
    fields = (inode, mode, 0, 0, 1, 0, len(payload), 0, 0,
              device[0], device[1], len(name_bytes), 0)
    header = b"070701" + b"".join(f"{value:08x}".encode("ascii") for value in fields)
    target.write(header)
    target.write(name_bytes)
    target.write(b"\0" * (-(len(header) + len(name_bytes)) % 4))
    target.write(payload)
    target.write(b"\0" * (-len(payload) % 4))


def guest_init(kernel_release, required_config, run_id, cases, seconds):
    commands = []
    for index, case in enumerate(cases):
        command = shlex.join([f"/cases/{index}", *case["args"]])
        commands.append(f"""
actual=$(/bin/busybox sha256sum /cases/{index})
actual=${{actual%% *}}
printf 'RIVET_VM_CASE_SHA256 {run_id} {index} %s\\n' "$actual"
[ "$actual" = {shlex.quote(case["sha256"])} ] || finish 125
printf 'RIVET_VM_CASE_EXEC {run_id} {index}\\n'
/bin/busybox timeout -s KILL {seconds} {command}
code="$?"
printf '\\nRIVET_VM_CASE_RESULT {run_id} {index} exit=%s\\n' "$code"
[ "$code" -eq 0 ] || finish "$code"
""")
    configuration = shlex.join(required_config)
    executions = "".join(commands)
    return f"""#!/bin/sh
export PATH=/bin
export RUST_BACKTRACE=1
finish() {{
    code="$1"
    printf '\\nRIVET_VM_RESULT {run_id} exit=%s\\n' "$code"
    /bin/busybox sync
    printf 'RIVET_VM_DONE {run_id}\\n'
    /bin/busybox poweroff -f
    while :; do /bin/busybox sleep 1; done
}}
/bin/busybox mount -t proc proc /proc || finish 125
/bin/busybox mount -t sysfs sysfs /sys || finish 125
/bin/busybox mount -t devtmpfs devtmpfs /dev || finish 125
release=$(/bin/busybox uname -r)
printf 'RIVET_VM_UNAME {run_id} %s\\n' "$release"
/bin/busybox uname -a
/bin/busybox cat /proc/version
[ "$release" = {shlex.quote(kernel_release)} ] || finish 125
/bin/busybox zcat /proc/config.gz > /run/kernel.config || finish 125
for option in {configuration}; do
    /bin/busybox grep -x "CONFIG_${{option}}=y" /run/kernel.config || finish 125
done
ulimit -n 65536 || finish 125
ulimit -l 262144 || finish 125
/bin/busybox ip link set lo up || finish 125
/bin/busybox ip address show dev lo
{executions}
finish 0
""".encode("utf-8")


def make_initramfs(path, busybox, cases, init):
    entries = [(name, stat.S_IFDIR | 0o755, b"", (0, 0))
               for name in ("bin", "cases", "dev", "proc", "sys", "run", "tmp")]
    entries.extend([
        ("dev/console", stat.S_IFCHR | 0o600, b"", (5, 1)),
        ("dev/null", stat.S_IFCHR | 0o666, b"", (1, 3)),
        ("bin/busybox", stat.S_IFREG | 0o755, busybox.read_bytes(), (0, 0)),
        ("bin/sh", stat.S_IFLNK | 0o777, b"busybox", (0, 0)),
        ("init", stat.S_IFREG | 0o755, init, (0, 0)),
    ])
    with path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as target:
        for inode, (name, mode, payload, device) in enumerate(entries, 1):
            cpio_entry(target, inode, name, mode, payload, device)
        for index, case in enumerate(cases):
            cpio_entry(target, len(entries) + index + 1, f"cases/{index}",
                       stat.S_IFREG | 0o755, case["path"].read_bytes())
        cpio_entry(target, len(entries) + len(cases) + 1, "TRAILER!!!", 0)


def payloads(args):
    if args.command == "run":
        definitions = [{"name": args.smoke.name, "executable": str(args.smoke),
                        "args": args.smoke_args}]
        base = Path.cwd()
    else:
        manifest = args.manifest.resolve(strict=True)
        definitions = load_json(manifest)
        base = manifest.parent
    if not isinstance(definitions, list) or not 1 <= len(definitions) <= 128:
        raise RuntimeError("A suite must contain between 1 and 128 cases")
    cases = []
    total_bytes = 0
    names = set()
    for definition in definitions:
        if not isinstance(definition, dict) or set(definition) != {"name", "executable", "args"}:
            raise RuntimeError("Each case must contain exactly name, executable and args")
        name, executable, arguments = (definition[key] for key in ("name", "executable", "args"))
        if not isinstance(name, str) or not name or name in names:
            raise RuntimeError("Case names must be nonempty and unique")
        if not isinstance(executable, str) or not isinstance(arguments, list) or any(
                not isinstance(value, str) or "\0" in value for value in arguments):
            raise RuntimeError("Case executable and arguments must be strings without NUL")
        path = (base / executable).resolve(strict=True)
        static_x86_64(path)
        total_bytes += path.stat().st_size
        if total_bytes > 256 * 1024 * 1024:
            raise RuntimeError("Combined payloads exceed the 256 MiB initramfs input limit")
        names.add(name)
        cases.append({"name": name, "path": path, "args": arguments, "sha256": sha256(path)})
    return cases


def run_guest(state, kernel, required_config, args):
    prepared = load_json(state / "prepared.json")
    if prepared["kernel"] != kernel:
        raise RuntimeError("Kernel lock changed; prepare a new state directory")
    if sha256(HERE / "debian-tools.lock.json") != prepared["tools_manifest_sha256"]:
        raise RuntimeError("Tool lock changed; prepare a new state directory")
    root = state / "tools"
    image = state / "kernel/vmlinuz"
    busybox = state / prepared["busybox"]
    qemu = root / "usr/bin/qemu-system-x86_64"
    bios = root / "usr/share/seabios/bios-256k.bin"
    for path, key in ((image, "kernel_image_sha256"), (busybox, "busybox_sha256"),
                      (qemu, "qemu_sha256"), (bios, "bios_sha256")):
        if sha256(path) != prepared[key]:
            raise RuntimeError(f"Prepared artifact changed: {path}")
    if args.accel == "kvm" and not os.access("/dev/kvm", os.R_OK | os.W_OK):
        raise RuntimeError("KVM access unavailable; explicitly select --accel tcg")
    cases = payloads(args)
    run_id = uuid.uuid4().hex
    directory = state / "runs" / run_id
    directory.mkdir(parents=True)
    initramfs = directory / "initramfs.cpio.gz"
    make_initramfs(initramfs, busybox, cases,
                   guest_init(kernel["release"], required_config, run_id, cases, args.smoke_timeout))
    command = [
        str(qemu), "-machine", "q35", "-accel", args.accel,
        "-cpu", "host" if args.accel == "kvm" else "max",
        "-smp", str(args.cpus), "-m", str(args.memory_mib),
        "-L", str(root / "usr/share/qemu"), "-bios", str(bios),
        "-nodefaults", "-no-user-config", "-display", "none", "-monitor", "none",
        "-serial", "stdio", "-nic", "none", "-no-reboot",
        "-kernel", str(image), "-initrd", str(initramfs),
        "-append", "console=ttyS0,115200 rdinit=/init panic=1 loglevel=6",
    ]
    serial = directory / "serial.log"
    report = {
        "run_id": run_id, "status": "starting", "host_uname": list(platform.uname()),
        "kernel_package_sha256": kernel["package"]["sha256"],
        "kernel_image_sha256": prepared["kernel_image_sha256"],
        "expected_guest_release": kernel["release"], "required_config": required_config,
        "cases": [{"name": case["name"], "executable": str(case["path"]),
                   "sha256": case["sha256"], "args": case["args"]} for case in cases],
        "initramfs_sha256": sha256(initramfs), "accelerator": args.accel,
        "cpus": args.cpus, "memory_mib": args.memory_mib,
        "smoke_timeout_seconds": args.smoke_timeout, "vm_timeout_seconds": args.vm_timeout,
        "command": command, "serial_log": str(serial),
        "network": "guest loopback only; no NIC, no host mount, no disk",
        "limitations": "Not hardware RX zero-copy, NIC NAPI/RSS, or performance evidence",
    }
    report_path = directory / "report.json"
    save_json(report_path, report)
    started = time.monotonic()
    timed_out = False
    with serial.open("wb") as output:
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output,
                                   stderr=subprocess.STDOUT, env=tool_environment(state),
                                   start_new_session=True)
        try:
            process.wait(timeout=args.vm_timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
    text = serial.read_text(encoding="utf-8", errors="replace").replace("\r", "")
    releases = re.findall(rf"^RIVET_VM_UNAME {run_id} (.+)$", text, re.MULTILINE)
    hashes = re.findall(rf"^RIVET_VM_CASE_SHA256 {run_id} (\d+) ([0-9a-f]{{64}})$", text, re.MULTILINE)
    executed = re.findall(rf"^RIVET_VM_CASE_EXEC {run_id} (\d+)$", text, re.MULTILINE)
    results = re.findall(rf"^RIVET_VM_CASE_RESULT {run_id} (\d+) exit=(\d+)$", text, re.MULTILINE)
    exits = re.findall(rf"^RIVET_VM_RESULT {run_id} exit=(\d+)$", text, re.MULTILINE)
    finished = f"RIVET_VM_DONE {run_id}" in text.splitlines()
    expected_indices = [str(index) for index in range(len(cases))]
    passed = (not timed_out and process.returncode == 0 and releases == [kernel["release"]]
              and hashes == [(str(index), case["sha256"]) for index, case in enumerate(cases)]
              and executed == expected_indices
              and results == [(index, "0") for index in expected_indices]
              and exits == ["0"] and finished)
    for index, case in enumerate(report["cases"]):
        key = str(index)
        case.update({
            "guest_hashes": [digest for case_index, digest in hashes if case_index == key],
            "guest_exit_codes": [int(code) for case_index, code in results if case_index == key],
            "executed": key in executed,
        })
    report.update({
        "status": "passed" if passed else ("timed_out" if timed_out else "failed"),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "qemu_exit_code": process.returncode, "guest_releases": releases,
        "guest_exit_codes": [int(code) for code in exits],
        "guest_case_executions": executed, "guest_finish_observed": finished,
    })
    save_json(report_path, report)
    print(text, end="")
    print(f"\nVerification report: {report_path}\nStatus: {report['status']}")
    return 0 if passed else 1


def main():
    kernels = load_json(HERE / "linux-kernel.lock.json")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kernel", choices=kernels["kernels"], default=kernels["default"],
                        help="Pinned guest selector; not an application version requirement")
    parser.add_argument("--state", type=Path, help="Isolated downloads/tools/reports directory")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare", help="Download, authenticate and extract only; never build or boot")
    for name, help_text in (("run", "Execute one existing static ELF"),
                            ("run-suite", "Execute a JSON suite of existing static ELFs in one boot")):
        run = sub.add_parser(name, help=help_text)
        run.add_argument("--accel", choices=("kvm", "tcg"), default="kvm")
        run.add_argument("--cpus", type=int, choices=range(1, 17), default=2)
        run.add_argument("--memory-mib", type=int, default=2048)
        run.add_argument("--smoke-timeout", type=int, default=120)
        run.add_argument("--vm-timeout", type=int, default=240)
        if name == "run":
            run.add_argument("smoke", type=Path)
            run.add_argument("smoke_args", nargs=argparse.REMAINDER,
                             help="Arguments passed verbatim to the binary")
        else:
            run.add_argument("manifest", type=Path,
                             help="JSON cases; relative executable paths resolve from this file")
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        parser.error("Run inside x86_64 Debian 13 / WSL Debian, not directly under Windows")
    selection = kernels["kernels"][args.kernel]
    kernel = selection["artifact"]
    state = (args.state or STATE_ROOT / f"linux-{args.kernel}").expanduser().resolve()
    if args.command == "prepare":
        return prepare(state, kernel)
    if not 512 <= args.memory_mib <= 16384:
        parser.error("--memory-mib must be between 512 and 16384")
    if not 1 <= args.smoke_timeout <= 3600 or not args.smoke_timeout + 30 <= args.vm_timeout <= 7200:
        parser.error("Use smoke-timeout 1..3600 and vm-timeout at least 30 seconds longer, at most 7200")
    return run_guest(state, kernel, selection["required_config"], args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"Verification environment error: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr, file=sys.stderr)
        sys.exit(2)
