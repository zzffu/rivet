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
        }
    }
    pub fn insert(&mut self, deadline: Instant, waker: Waker) -> io::Result<u64> {
        let index = self.free.pop().ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "timer admission limit reached")
        })?;
        let position = self.heap.len();
        let slot = &mut self.slots[index as usize];
        let token = (u64::from(slot.generation) << 32) | u64::from(index);
        slot.entry = Some(Entry {
            deadline,
            waker,
            heap_index: position,
        });
        self.heap.push(index);
        self.up(position);
        Ok(token)
    }
    pub fn update(&mut self, token: u64, waker: &Waker) -> bool {
        let Some(slot) = self.slots.get_mut(token as u32 as usize) else {
            return false;
        };
        if slot.generation != (token >> 32) as u32 {
            return false;
        }
        let Some(entry) = slot.entry.as_mut() else {
            return false;
        };
        if !entry.waker.will_wake(waker) {
            entry.waker = waker.clone();
        }
        true
    }
    pub fn remove(&mut self, token: u64) {
        let index = token as u32 as usize;
        let Some(slot) = self.slots.get(index) else {
            return;
        };
        if slot.generation != (token >> 32) as u32 {
            return;
        }
        let Some(entry) = slot.entry.as_ref() else {
            return;
        };
        let position = entry.heap_index;
        self.remove_at(position);
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
    pub fn expire(&mut self, now: Instant, budget: usize) {
        for _ in 0..budget {
            if self.next_deadline().is_none_or(|deadline| deadline > now) {
                break;
            }
            self.remove_at(0).wake();
        }
    }
    pub fn clear(&mut self) {
        while !self.heap.is_empty() {
            self.remove_at(0).wake();
        }
    }
}
