use std::{io, task::Waker, time::Instant};

struct Entry {
    deadline: Instant,
    waker: Waker,
    heap_index: usize,
}
struct Slot {
    generation: u32,
    entry: Option<Entry>,
}
/// An indexed heap permits eager cancellation; cancelled sleeps cannot leave
/// unbounded stale nodes behind a distant deadline.
pub(crate) struct TimerQueue {
    slots: Box<[Slot]>,
    free: Vec<u32>,
    heap: Vec<u32>,
    closed: bool,
}
impl TimerQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity)
                .map(|_| Slot {
                    generation: 1,
                    entry: None,
                })
                .collect(),
            free: (0..capacity as u32).rev().collect(),
            heap: Vec::with_capacity(capacity),
            closed: false,
        }
    }
    pub fn insert(&mut self, deadline: Instant, waker: &mut Option<Waker>) -> io::Result<u64> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "timer runtime has stopped",
            ));
        }
        let index = self.free.pop().ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "timer admission limit reached")
        })?;
        let position = self.heap.len();
        let slot = &mut self.slots[index as usize];
        let token = (u64::from(slot.generation) << 32) | u64::from(index);
        slot.entry = Some(Entry {
            deadline,
            waker: waker.take().expect("timer insertion requires a waker"),
            heap_index: position,
        });
        self.heap.push(index);
        self.up(position);
        Ok(token)
    }
    pub fn update(&mut self, token: u64, waker: &mut Option<Waker>) -> bool {
        let Some(slot) = self.slots.get_mut(token as u32 as usize) else {
            return false;
        };
        if slot.generation != (token >> 32) as u32 {
            return false;
        }
        let Some(entry) = slot.entry.as_mut() else {
            return false;
        };
        if !entry.waker.will_wake(waker.as_ref().unwrap()) {
            std::mem::swap(&mut entry.waker, waker.as_mut().unwrap());
        }
        true
    }
    pub fn reset(&mut self, token: u64, deadline: Instant) -> bool {
        let Some(slot) = self.slots.get_mut(token as u32 as usize) else {
            return false;
        };
        if slot.generation != (token >> 32) as u32 {
            return false;
        }
        let Some(entry) = slot.entry.as_mut() else {
            return false;
        };
        let previous = entry.deadline;
        let position = entry.heap_index;
        entry.deadline = deadline;
        if deadline < previous {
            self.up(position);
        } else if deadline > previous {
            self.down(position);
        }
        true
    }
    pub fn remove(&mut self, token: u64) -> Option<Waker> {
        let index = token as u32 as usize;
        let slot = self.slots.get(index)?;
        if slot.generation != (token >> 32) as u32 {
            return None;
        }
        let position = slot.entry.as_ref()?.heap_index;
        Some(self.remove_at(position))
    }
    fn remove_at(&mut self, position: usize) -> Waker {
        let index = self.heap.swap_remove(position);
        let slot = &mut self.slots[index as usize];
        let entry = slot.entry.take().unwrap();
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(index);
        }
        if position < self.heap.len() {
            self.slots[self.heap[position] as usize]
                .entry
                .as_mut()
                .unwrap()
                .heap_index = position;
            let position = self.up(position);
            self.down(position);
        }
        entry.waker
    }
    fn less(&self, left: usize, right: usize) -> bool {
        self.slots[self.heap[left] as usize]
            .entry
            .as_ref()
            .unwrap()
            .deadline
            < self.slots[self.heap[right] as usize]
                .entry
                .as_ref()
                .unwrap()
                .deadline
    }
    fn swap(&mut self, left: usize, right: usize) {
        self.heap.swap(left, right);
        self.slots[self.heap[left] as usize]
            .entry
            .as_mut()
            .unwrap()
            .heap_index = left;
        self.slots[self.heap[right] as usize]
            .entry
            .as_mut()
            .unwrap()
            .heap_index = right;
    }
    fn up(&mut self, mut position: usize) -> usize {
        while position != 0 {
            let parent = (position - 1) / 2;
            if !self.less(position, parent) {
                break;
            }
            self.swap(position, parent);
            position = parent;
        }
        position
    }
    fn down(&mut self, mut position: usize) {
        loop {
            let left = position * 2 + 1;
            if left >= self.heap.len() {
                return;
            }
            let right = left + 1;
            let child = if right < self.heap.len() && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(child, position) {
                return;
            }
            self.swap(position, child);
            position = child;
        }
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.heap
            .first()
            .map(|&i| self.slots[i as usize].entry.as_ref().unwrap().deadline)
    }
    pub fn expire(&mut self, now: Instant) -> Option<Waker> {
        if self.next_deadline().is_none_or(|deadline| deadline > now) {
            return None;
        }
        Some(self.remove_at(0))
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn clear(&mut self) -> Option<Waker> {
        self.closed = true;
        if self.heap.is_empty() {
            None
        } else {
            Some(self.remove_at(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
        time::Duration,
    };

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    impl WakeCount {
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn expire(timers: &mut TimerQueue, now: Instant, budget: usize) {
        for _ in 0..budget {
            let Some(waker) = timers.expire(now) else {
                break;
            };
            waker.wake();
        }
    }

    fn clear(timers: &mut TimerQueue) {
        while let Some(waker) = timers.clear() {
            waker.wake();
        }
    }

    #[test]
    fn reset_moves_deadlines_in_both_heap_directions() {
        let now = Instant::now();
        let mut timers = TimerQueue::new(3);
        let first = Arc::new(WakeCount::default());
        let second = Arc::new(WakeCount::default());
        let third = Arc::new(WakeCount::default());
        let first_token = timers
            .insert(
                now + Duration::from_secs(20),
                &mut Some(Waker::from(first.clone())),
            )
            .unwrap();
        timers
            .insert(
                now + Duration::from_secs(30),
                &mut Some(Waker::from(second.clone())),
            )
            .unwrap();
        let third_token = timers
            .insert(
                now + Duration::from_secs(40),
                &mut Some(Waker::from(third.clone())),
            )
            .unwrap();

        assert!(timers.reset(third_token, now + Duration::from_secs(10)));
        expire(&mut timers, now + Duration::from_secs(9), 3);
        assert_eq!((first.count(), second.count(), third.count()), (0, 0, 0));
        expire(&mut timers, now + Duration::from_secs(10), 3);
        assert_eq!((first.count(), second.count(), third.count()), (0, 0, 1));

        assert!(timers.reset(first_token, now + Duration::from_secs(50)));
        expire(&mut timers, now + Duration::from_secs(30), 3);
        assert_eq!((first.count(), second.count(), third.count()), (0, 1, 1));
        expire(&mut timers, now + Duration::from_secs(49), 3);
        assert_eq!(first.count(), 0);
        expire(&mut timers, now + Duration::from_secs(50), 3);
        assert_eq!(first.count(), 1);
        assert!(timers.next_deadline().is_none());
    }

    #[test]
    fn reset_stays_bounded_and_expired_tokens_cannot_touch_reused_slots() {
        let now = Instant::now();
        let mut timers = TimerQueue::new(1);
        let heap_capacity = timers.heap.capacity();
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        let token = timers
            .insert(now + Duration::from_secs(10), &mut Some(waker.clone()))
            .unwrap();
        for iteration in 0..8192 {
            let seconds = if iteration % 2 == 0 { 5 } else { 10 };
            assert!(timers.reset(token, now + Duration::from_secs(seconds)));
        }
        assert_eq!(timers.heap.len(), 1);
        assert_eq!(timers.heap.capacity(), heap_capacity);
        assert_eq!(
            timers
                .insert(now, &mut Some(waker.clone()))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        expire(&mut timers, now + Duration::from_secs(9), 1);
        assert_eq!(wakes.count(), 0);
        expire(&mut timers, now + Duration::from_secs(10), 1);
        assert_eq!(wakes.count(), 1);

        timers
            .insert(now + Duration::from_secs(20), &mut Some(waker))
            .unwrap();
        assert!(!timers.reset(token, now));
        drop(timers.remove(token));
        expire(&mut timers, now + Duration::from_secs(19), 1);
        assert_eq!(wakes.count(), 1);
        expire(&mut timers, now + Duration::from_secs(20), 1);
        assert_eq!(wakes.count(), 2);
    }

    #[test]
    fn shutdown_wakes_waiters_and_permanently_closes_admission() {
        let mut timers = TimerQueue::new(1);
        let wakes = Arc::new(WakeCount::default());
        let waker = Waker::from(wakes.clone());
        timers
            .insert(Instant::now(), &mut Some(waker.clone()))
            .unwrap();
        clear(&mut timers);
        clear(&mut timers);
        assert_eq!(wakes.count(), 1);
        assert!(timers.is_closed());
        assert_eq!(
            timers
                .insert(Instant::now(), &mut Some(waker))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}
