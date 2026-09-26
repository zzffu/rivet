use futures_lite::future::{block_on, poll_once};
use parking_lot::Mutex;
use rivet::signal::{ShutdownSignals, SignalKind};
use std::{
    future::Future,
    io::{BufRead, BufReader, Write},
    pin::{Pin, pin},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, mpsc},
    task::{Context, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

const CHILD_CASE: &str = "RIVET_SIGNAL_CHILD_CASE";
const REQUEST: &str = "RIVET_SIGNAL_REQUEST ";

#[cfg(unix)]
const FIRST: SignalKind = SignalKind::Interrupt;
#[cfg(unix)]
const SECOND: SignalKind = SignalKind::Terminate;
#[cfg(windows)]
const FIRST: SignalKind = SignalKind::CtrlC;
#[cfg(windows)]
const SECOND: SignalKind = SignalKind::CtrlBreak;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// Signals are never sent to this test runner. Unix targets exactly the isolated
// child PID; Windows gives the child its own console, and only that child calls
// GenerateConsoleCtrlEvent after verifying it is the sole console client.
fn run_child(case: &str) -> ExitStatus {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "signal_child", "--nocapture", "--test-threads=1"])
        .env(CHILD_CASE, case)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::CREATE_NEW_CONSOLE;
        command.creation_flags(CREATE_NEW_CONSOLE);
    }
    let mut child = ChildGuard(command.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line.unwrap()).is_err() {
                return;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut transcript = Vec::new();
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "signal child {case} timed out: {transcript:?}"
        );
        match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(line) => {
                if let Some((_, kind)) = line.split_once(REQUEST) {
                    assert!(matches!(kind, "first" | "second"));
                    #[cfg(unix)]
                    {
                        let signal = if kind == "first" {
                            libc::SIGINT
                        } else {
                            libc::SIGTERM
                        };
                        let pid = i32::try_from(child.0.id()).unwrap();
                        assert_ne!(pid, unsafe { libc::getpid() });
                        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
                    }
                    // For Windows this grants permission to signal its dedicated
                    // console. On Unix it acknowledges that kill(child) returned.
                    // A default-action child can exit before accepting the ack.
                    let _ = child.0.stdin.as_mut().unwrap().write_all(b"delivered\n");
                }
                transcript.push(line);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => thread::sleep(Duration::from_millis(1)),
        }
    };
    reader.join().unwrap();
    assert_ne!(
        status.code(),
        Some(101),
        "signal child {case} panicked: {transcript:?}"
    );
    status
}

fn request(kind: SignalKind) {
    let name = if kind == FIRST { "first" } else { "second" };
    println!("{REQUEST}{name}");
    std::io::stdout().flush().unwrap();
    let mut acknowledgement = String::new();
    std::io::stdin().read_line(&mut acknowledgement).unwrap();
    assert_eq!(acknowledgement, "delivered\n");
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Console::{
            CTRL_BREAK_EVENT, CTRL_C_EVENT, GenerateConsoleCtrlEvent,
        };
        assert_isolated_console();
        let event = if kind == FIRST {
            CTRL_C_EVENT
        } else {
            CTRL_BREAK_EVENT
        };
        assert_ne!(unsafe { GenerateConsoleCtrlEvent(event, 0) }, 0);
    }
}

#[cfg(windows)]
fn assert_isolated_console() {
    use windows_sys::Win32::System::Console::GetConsoleProcessList;
    let mut processes = [0u32; 2];
    let count = unsafe { GetConsoleProcessList(processes.as_mut_ptr(), processes.len() as u32) };
    assert_eq!(count, 1, "refusing to signal a shared console");
    assert_eq!(processes[0], std::process::id());
}

fn check_pair(signals: &mut ShutdownSignals) {
    let a = block_on(signals.recv()).unwrap();
    let b = block_on(signals.recv()).unwrap();
    assert!(matches!((a, b), (x, y) if (x == FIRST && y == SECOND) || (x == SECOND && y == FIRST)));
}

fn exercise_broadcast_and_cancellation() {
    let mut first = ShutdownSignals::new().unwrap();
    let mut second = ShutdownSignals::new().unwrap();
    // This pending future is dropped, including its registered listener.
    assert!(block_on(poll_once(first.recv())).is_none());
    {
        let mut waiting = pin!(first.recv());
        assert!(block_on(poll_once(waiting.as_mut())).is_none());
        request(FIRST);
        assert_eq!(block_on(waiting).unwrap(), FIRST);
    }
    assert_eq!(block_on(second.recv()).unwrap(), FIRST);

    // Cancel after requesting an OS signal but before polling its completion.
    // Whether delivery raced the drop or not, the retry must retain the event.
    {
        let mut waiting = pin!(first.recv());
        assert!(block_on(poll_once(waiting.as_mut())).is_none());
        request(SECOND);
    }
    assert_eq!(block_on(first.recv()).unwrap(), SECOND);
    assert_eq!(block_on(second.recv()).unwrap(), SECOND);

    // Distinct kinds must not overwrite each other while a consumer is idle.
    request(FIRST);
    request(SECOND);
    check_pair(&mut first);
    check_pair(&mut second);

    assert!(block_on(poll_once(first.recv())).is_none());
    drop(second);
    request(SECOND);
    assert_eq!(block_on(first.recv()).unwrap(), SECOND);
    drop(first);

    // A complete teardown must permit a fresh subscription with fresh state.
    let mut restarted = ShutdownSignals::new().unwrap();
    request(FIRST);
    assert_eq!(block_on(restarted.recv()).unwrap(), FIRST);
}

type SignalTask = Pin<Box<dyn Future<Output = ()> + Send>>;

struct CancelOnWake {
    task: Mutex<Option<SignalTask>>,
    completed: mpsc::Sender<()>,
}

impl Wake for CancelOnWake {
    fn wake(self: Arc<Self>) {
        let task = self.task.lock().take();
        drop(task);
        let _ = self.completed.send(());
    }
}

fn drop_pending_task_from_waker() {
    let mut signals = ShutdownSignals::new().unwrap();
    let (completed, destroyed) = mpsc::channel();
    let cancellation = Arc::new(CancelOnWake {
        task: Mutex::new(None),
        completed,
    });
    let waker = Waker::from(cancellation.clone());
    let mut task = Box::pin(async move {
        let _ = signals.recv().await;
    });
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    *cancellation.task.lock() = Some(task);
    request(FIRST);
    destroyed
        .recv_timeout(Duration::from_secs(5))
        .expect("signal Waker could not destroy its pending task and last subscription");

    // Synchronous destruction must disarm the old registration and permit a new
    // dispatcher, even before the old dispatcher returns from the user Waker.
    let mut restarted = ShutdownSignals::new().unwrap();
    request(SECOND);
    assert_eq!(block_on(restarted.recv()).unwrap(), SECOND);
}

fn default_after_drop(kind: SignalKind) {
    let first = ShutdownSignals::new().unwrap();
    let second = ShutdownSignals::new().unwrap();
    drop(first);
    drop(second);
    request(kind);
    thread::sleep(Duration::from_secs(5));
    panic!("last subscription swallowed the restored default signal action");
}

#[cfg(unix)]
fn action(signal: libc::c_int) -> libc::sigaction {
    let mut current = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::sigaction(signal, std::ptr::null(), &mut current) },
        0
    );
    current
}

#[cfg(unix)]
fn set_action(signal: libc::c_int, handler: libc::sighandler_t) {
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = handler;
    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    assert_eq!(
        unsafe { libc::sigaction(signal, &action, std::ptr::null_mut()) },
        0
    );
}

#[cfg(unix)]
fn ignored_after_drop() {
    set_action(libc::SIGINT, libc::SIG_IGN);
    set_action(libc::SIGTERM, libc::SIG_IGN);
    let mut signals = ShutdownSignals::new().unwrap();
    request(FIRST);
    assert_eq!(block_on(signals.recv()).unwrap(), FIRST);
    request(SECOND);
    assert_eq!(block_on(signals.recv()).unwrap(), SECOND);
    drop(signals);
    assert_eq!(action(libc::SIGINT).sa_sigaction, libc::SIG_IGN);
    assert_eq!(action(libc::SIGTERM).sa_sigaction, libc::SIG_IGN);
    request(FIRST);
    request(SECOND);
}

#[cfg(unix)]
static CUSTOM_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn custom_handler(signal: libc::c_int) {
    CUSTOM_SIGNAL.store(signal, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(unix)]
fn expect_custom(signal: libc::c_int) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while CUSTOM_SIGNAL.load(std::sync::atomic::Ordering::Relaxed) != signal {
        assert!(
            Instant::now() < deadline,
            "foreign signal handler did not run"
        );
        thread::yield_now();
    }
}

#[cfg(unix)]
fn existing_handler_is_preserved() {
    set_action(libc::SIGTERM, custom_handler as *const () as usize);
    let error = match ShutdownSignals::new() {
        Ok(_) => panic!("accepted ownership of a foreign handler"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(action(libc::SIGINT).sa_sigaction, libc::SIG_DFL);
    assert_eq!(
        action(libc::SIGTERM).sa_sigaction,
        custom_handler as *const () as usize
    );
    request(SECOND);
    expect_custom(libc::SIGTERM);
}

#[cfg(unix)]
fn replacement_handler_is_preserved() {
    let signals = ShutdownSignals::new().unwrap();
    set_action(libc::SIGINT, custom_handler as *const () as usize);
    drop(signals);
    assert_eq!(
        action(libc::SIGINT).sa_sigaction,
        custom_handler as *const () as usize
    );
    assert_eq!(action(libc::SIGTERM).sa_sigaction, libc::SIG_DFL);
    request(FIRST);
    expect_custom(libc::SIGINT);
}

#[cfg(windows)]
static CUSTOM_EVENTS: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[cfg(windows)]
unsafe extern "system" fn custom_console_handler(event: u32) -> i32 {
    use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, CTRL_C_EVENT};
    let bit = match event {
        CTRL_C_EVENT => 1,
        CTRL_BREAK_EVENT => 2,
        _ => return 0,
    };
    CUSTOM_EVENTS.fetch_or(bit, std::sync::atomic::Ordering::Relaxed);
    1
}

#[cfg(windows)]
fn existing_console_handler_is_preserved() {
    use windows_sys::Win32::System::Console::SetConsoleCtrlHandler;
    assert_ne!(
        unsafe { SetConsoleCtrlHandler(Some(custom_console_handler), 1) },
        0
    );
    let mut signals = ShutdownSignals::new().unwrap();
    request(FIRST);
    assert_eq!(block_on(signals.recv()).unwrap(), FIRST);
    assert_eq!(CUSTOM_EVENTS.load(std::sync::atomic::Ordering::Relaxed), 0);
    drop(signals);
    request(FIRST);
    request(SECOND);
    let deadline = Instant::now() + Duration::from_secs(2);
    while CUSTOM_EVENTS.load(std::sync::atomic::Ordering::Relaxed) != 3 {
        assert!(
            Instant::now() < deadline,
            "foreign console handler did not run"
        );
        thread::yield_now();
    }
    assert_ne!(
        unsafe { SetConsoleCtrlHandler(Some(custom_console_handler), 0) },
        0
    );
}

// The parent invokes only this entry point in each fresh process. Running the
// integration suite normally never performs signals in the cargo test process.
#[test]
fn signal_child() {
    let Some(case) = std::env::var_os(CHILD_CASE) else {
        return;
    };
    #[cfg(unix)]
    {
        set_action(libc::SIGINT, libc::SIG_DFL);
        set_action(libc::SIGTERM, libc::SIG_DFL);
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Console::SetConsoleCtrlHandler;
        assert_isolated_console();
        // Ignore state can be inherited from a test-launching shell. Establish
        // the ordinary default only inside this child's private console.
        assert_ne!(unsafe { SetConsoleCtrlHandler(None, 0) }, 0);
    }
    match case.to_str().unwrap() {
        "broadcast" => exercise_broadcast_and_cancellation(),
        "drop-from-waker" => drop_pending_task_from_waker(),
        "drop-first" => default_after_drop(FIRST),
        "drop-second" => default_after_drop(SECOND),
        #[cfg(unix)]
        "ignored" => ignored_after_drop(),
        #[cfg(unix)]
        "existing-handler" => existing_handler_is_preserved(),
        #[cfg(unix)]
        "replacement-handler" => replacement_handler_is_preserved(),
        #[cfg(windows)]
        "existing-handler" => existing_console_handler_is_preserved(),
        unknown => panic!("unknown child case: {unknown}"),
    }
}

#[test]
fn broadcasts_both_signals_and_retries_cancelled_receives() {
    assert!(run_child("broadcast").success());
}

#[test]
fn signal_waker_can_drop_its_pending_task_and_last_subscription() {
    assert!(run_child("drop-from-waker").success());
}

#[test]
fn last_drop_restores_both_default_exit_actions() {
    for (case, kind) in [("drop-first", FIRST), ("drop-second", SECOND)] {
        let status = run_child(case);
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let signal = if kind == FIRST {
                libc::SIGINT
            } else {
                libc::SIGTERM
            };
            assert_eq!(status.signal(), Some(signal));
        }
        #[cfg(windows)]
        {
            let _ = kind;
            assert_eq!(status.code(), Some(0xc000013au32 as i32));
        }
    }
}

#[cfg(unix)]
#[test]
fn last_drop_restores_ignored_dispositions() {
    assert!(run_child("ignored").success());
}

#[cfg(unix)]
#[test]
fn never_overwrites_another_unix_handler() {
    assert!(run_child("existing-handler").success());
    assert!(run_child("replacement-handler").success());
}

#[cfg(windows)]
#[test]
fn preserves_another_console_handler() {
    assert!(run_child("existing-handler").success());
}
