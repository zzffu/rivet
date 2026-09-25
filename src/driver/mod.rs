//! Internal platform seam: semantic events, never synthetic readiness wrappers.

use crate::buffer::{ReadBuf, SendPayload};
use std::{io, net::SocketAddr};

#[cfg(target_os = "android")]
pub(crate) mod android;
#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "windows")]
pub(crate) mod windows;

#[cfg(target_os = "android")]
pub(crate) use android::{Driver, Notifier, Shared};
#[cfg(target_os = "linux")]
pub(crate) use linux::{Driver, Notifier, Shared};
#[cfg(target_os = "windows")]
pub(crate) use windows::{Driver, Notifier, Shared};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Token(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SocketId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SocketKind {
    TcpStream,
    TcpListener,
    Udp,
}

#[derive(Clone, Debug)]
pub(crate) struct SocketInfo {
    pub id: SocketId,
    pub kind: SocketKind,
    pub local_addr: SocketAddr,
    pub peer_addr: Option<SocketAddr>,
}

#[derive(Debug)]
pub struct Received {
    pub data: ReadBuf,
    pub peer: Option<SocketAddr>,
    pub truncated: bool,
    pub original_len: Option<usize>,
    pub gro_segment_size: Option<u16>,
}

#[derive(Debug)]
pub struct SendOutcome {
    pub result: io::Result<usize>,
    pub data: SendPayload,
}

#[derive(Debug)]
pub(crate) enum Event {
    Connected {
        token: Token,
        result: io::Result<SocketInfo>,
    },
    Accepted {
        token: Token,
        result: io::Result<SocketInfo>,
    },
    Received {
        token: Token,
        result: io::Result<Received>,
    },
    ReceiveEof {
        token: Token,
    },
    Sent {
        token: Token,
        outcome: SendOutcome,
        memory_released: bool,
    },
    #[cfg(all(target_os = "linux", feature = "zc-tx"))]
    Released {
        token: Token,
    },
    #[cfg(all(target_os = "linux", feature = "tcp-splice"))]
    Spliced {
        token: Token,
        result: io::Result<usize>,
    },
    Stopped {
        token: Token,
        result: io::Result<()>,
    },
}

/// A fixed-capacity generational arena. Entries never move while occupied.
/// Backend kernel pointers may refer to occupied entries, but remove must wait
/// until all corresponding completions and memory-release notifications retire.
pub(crate) struct Arena<T> {
    slots: Box<[Slot<T>]>,
    free: Vec<u32>,
    used: usize,
}

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

impl<T> Arena<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity <= u32::MAX as usize);
        let slots = (0..capacity)
            .map(|_| Slot {
                generation: 1,
                value: None,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let free = (0..capacity as u32).rev().collect();
        Self {
            slots,
            free,
            used: 0,
        }
    }
    pub fn insert(&mut self, value: T) -> Result<u64, T> {
        let Some(index) = self.free.pop() else {
            return Err(value);
        };
        let slot = &mut self.slots[index as usize];
        slot.value = Some(value);
        self.used += 1;
        Ok((u64::from(slot.generation) << 32) | u64::from(index))
    }
    pub fn get(&self, key: u64) -> Option<&T> {
        let slot = self.slots.get(key as u32 as usize)?;
        (slot.generation == (key >> 32) as u32)
            .then_some(slot.value.as_ref())
            .flatten()
    }
    pub fn get_mut(&mut self, key: u64) -> Option<&mut T> {
        let slot = self.slots.get_mut(key as u32 as usize)?;
        if slot.generation != (key >> 32) as u32 {
            return None;
        }
        slot.value.as_mut()
    }
    pub fn remove(&mut self, key: u64) -> Option<T> {
        let index = key as u32;
        let slot = self.slots.get_mut(index as usize)?;
        if slot.generation != (key >> 32) as u32 {
            return None;
        }
        let value = slot.value.take()?;
        // Retire an exhausted generation instead of reviving an ancient token.
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(index);
        }
        self.used -= 1;
        Some(value)
    }
    #[cfg(any(windows, test))]
    pub fn len(&self) -> usize {
        self.used
    }
    pub fn is_empty(&self) -> bool {
        self.used == 0
    }
    pub fn available(&self) -> usize {
        self.free.len()
    }
    pub fn iter(&self) -> impl Iterator<Item = (u64, &T)> {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.value
                .as_ref()
                .map(|value| ((u64::from(slot.generation) << 32) | index as u64, value))
        })
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (u64, &mut T)> {
        self.slots
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| {
                let key = (u64::from(slot.generation) << 32) | index as u64;
                slot.value.as_mut().map(|value| (key, value))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::Arena;

    #[test]
    fn retired_key_cannot_remove_a_reused_slot() {
        let mut arena = Arena::new(1);
        let first = arena.insert(11).unwrap();
        assert_eq!(arena.remove(first), Some(11));
        let second = arena.insert(22).unwrap();
        assert_eq!(arena.get(first), None);
        assert_eq!(arena.remove(first), None);
        assert_eq!(arena.get(second), Some(&22));
        assert_eq!(arena.len(), 1);
    }

    #[test]
    fn exhausted_generation_never_revives_a_stale_key() {
        let mut arena = Arena::new(1);
        arena.slots[0].generation = u32::MAX;
        let last = arena.insert(7).unwrap();
        assert_eq!(arena.remove(last), Some(7));
        assert!(arena.is_empty());
        assert_eq!(arena.available(), 0);
        assert_eq!(arena.insert(8), Err(8));
        assert_eq!(arena.get(last), None);
    }
}
