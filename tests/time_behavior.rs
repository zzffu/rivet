use futures_lite::future::{block_on, poll_once};
use rivet::{
    Runtime, RuntimeConfig, runtime,
    time::{self, MissedTickBehavior},
};
use std::{
    io,
    time::{Duration, Instant},
};

fn runtime(capacity: usize) -> Runtime {
    let mut config = RuntimeConfig::single_thread();
    config.limits.max_tasks = 4;
    config.limits.max_sockets = 4;
    config.limits.max_operations = capacity;
    config.limits.max_pending_receives = 1;
    config.limits.max_pending_accepts = 1;
    config.limits.pool.bytes = 64 * 1024;
    config.limits.pool.block_size = 16 * 1024;
    config.limits.pool.max_leases = 8;
    Runtime::new(config).unwrap()
}

#[test]
fn sleep_reset_reuses_unpolled_active_and_completed_timers_at_capacity_one() {
    let mut runtime = runtime(1);
    runtime.block_on(async {
        let distant = Instant::now() + Duration::from_secs(3600);
        let mut sleep = time::sleep_until(Instant::now());
        sleep.reset(distant).unwrap();
        assert!(poll_once(&mut sleep).await.is_none());

        for iteration in 0..1024 {
            sleep
                .reset(distant + Duration::from_secs(iteration % 2))
                .unwrap();
        }
        assert_eq!(
            time::sleep_until(distant).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        sleep.reset(Instant::now()).unwrap();
        (&mut sleep).await.unwrap();

        sleep.reset(distant).unwrap();
        assert!(poll_once(&mut sleep).await.is_none());
        sleep.reset(Instant::now()).unwrap();
        (&mut sleep).await.unwrap();

        // A genuinely future deadline exercises native timer wakeup, without
        // asserting how long the operating system took to schedule this task.
        sleep
            .reset(Instant::now() + Duration::from_millis(1))
            .unwrap();
        (&mut sleep).await.unwrap();
    });
}

#[test]
fn expired_sleep_reset_cannot_rewrite_a_new_owner_of_its_old_slot() {
    let mut runtime = runtime(1);
    runtime.block_on(async {
        let distant = Instant::now() + Duration::from_secs(3600);
        let mut expired = time::sleep_until(distant);
        assert!(poll_once(&mut expired).await.is_none());
        expired.reset(Instant::now()).unwrap();
        // Give the timer queue a turn to expire the entry before polling the
        // sleep itself. The next sleep reuses that freed slot.
        runtime::yield_now().await;
        let mut occupant = time::sleep_until(distant);
        assert!(poll_once(&mut occupant).await.is_none());
        expired.reset(distant).unwrap();
        assert_eq!(
            (&mut expired).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(poll_once(&mut occupant).await.is_none());
        drop(occupant);
        expired.reset(distant).unwrap();
        assert!(poll_once(&mut expired).await.is_none());
        expired.reset(Instant::now()).unwrap();
        (&mut expired).await.unwrap();
    });
}

#[test]
fn stopped_runtime_is_an_error_even_for_elapsed_or_completed_sleep() {
    let mut runtime = runtime(1);
    let (mut elapsed, mut completed) = runtime.block_on(async {
        let mut elapsed = time::sleep(Duration::from_secs(3600));
        assert!(poll_once(&mut elapsed).await.is_none());
        elapsed.reset(Instant::now()).unwrap();
        let mut completed = time::sleep_until(Instant::now());
        (&mut completed).await.unwrap();
        (elapsed, completed)
    });
    drop(runtime);
    assert_eq!(
        block_on(&mut elapsed).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    for sleep in [&mut elapsed, &mut completed] {
        assert_eq!(
            sleep.reset(Instant::now()).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

#[test]
fn reset_does_not_move_a_completed_sleep_to_another_worker() {
    let mut owner = runtime(1);
    let mut other = runtime(1);
    let mut sleep = time::sleep_until(Instant::now());
    owner.block_on(async {
        (&mut sleep).await.unwrap();
    });
    other.block_on(async {
        assert_eq!(
            sleep.reset(Instant::now()).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    });
    owner.block_on(async {
        sleep.reset(Instant::now()).unwrap();
        (&mut sleep).await.unwrap();
    });
}

#[test]
fn cancelled_interval_wait_keeps_its_timer_until_interval_drop() {
    let mut runtime = runtime(1);
    runtime.block_on(async {
        let distant = Instant::now() + Duration::from_secs(3600);
        let mut interval = time::interval_at(distant, Duration::from_secs(1)).unwrap();
        {
            let mut tick = std::pin::pin!(interval.tick());
            assert!(poll_once(&mut tick).await.is_none());
        }
        assert_eq!(
            time::sleep_until(distant).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(interval);
        let mut replacement = time::sleep_until(distant);
        assert!(poll_once(&mut replacement).await.is_none());
    });
}

#[test]
fn interval_wakes_with_planned_instants_and_cancelled_wait_does_not_skip_tick() {
    let mut runtime = runtime(1);
    runtime.block_on(async {
        let period = Duration::from_millis(1);
        let start = Instant::now() + period;
        let mut interval = time::interval_at(start, period).unwrap();
        interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let ready = {
            let mut waiter = std::pin::pin!(interval.tick());
            poll_once(&mut waiter).await
        };
        // The waiter is canceled if pending. If the host was descheduled long
        // enough for it to be ready, do not assert a fragile timing threshold.
        let first = match ready {
            Some(result) => result.unwrap(),
            None => interval.tick().await.unwrap(),
        };
        assert_eq!(first, start);
        assert_eq!(interval.tick().await.unwrap(), start + period);
    });
}
