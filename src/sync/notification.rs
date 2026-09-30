use parking_lot::Mutex;
use std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

/// Rivet's notifications never invoke user Waker code while holding a lock.
/// Listeners live inside their pinned futures; notification and cancellation do
/// not allocate. The optional single permit is used only by `Notify`.
#[derive(Default)]
pub(crate) struct Event {
    list: Mutex<List>,
}

// Waker callbacks run only after list invariants have been restored. Preserve
// the unwind auto traits of the notifications these private types replace.
impl std::panic::UnwindSafe for Event {}
impl std::panic::RefUnwindSafe for Event {}

#[derive(Default)]
struct List {
    generation: u64,
    permit: bool,
    head: Option<NonNull<Node>>,
    tail: Option<NonNull<Node>>,
}

// SAFETY: Every node is pinned before insertion, holds a borrow of its Event,
// and unlinks itself before destruction. All node access uses this Event's lock.
unsafe impl Send for List {}

struct Node {
    generation: u64,
    previous: Option<NonNull<Node>>,
    next: Option<NonNull<Node>>,
    linked: bool,
    selected: bool,
    waker: Option<Waker>,
}

impl List {
    /// The caller holds the Event lock and supplies a live, pinned node.
    unsafe fn insert(&mut self, mut node: NonNull<Node>) {
        // SAFETY: The lock excludes all other node access. The listener's pin
        // and Drop keep every linked node alive at the same address.
        unsafe {
            node.as_mut().previous = self.tail;
            node.as_mut().next = None;
            node.as_mut().linked = true;
            if let Some(mut tail) = self.tail {
                tail.as_mut().next = Some(node);
            } else {
                self.head = Some(node);
            }
            self.tail = Some(node);
        }
    }

    /// The caller holds the Event lock and supplies a live, linked node.
    unsafe fn remove(&mut self, mut node: NonNull<Node>) {
        // SAFETY: All neighboring nodes are pinned and live while linked. No
        // Waker operation runs here, so user code cannot invalidate a neighbor.
        unsafe {
            let previous = node.as_ref().previous;
            let next = node.as_ref().next;
            if let Some(mut previous) = previous {
                previous.as_mut().next = next;
            } else {
                self.head = next;
            }
            if let Some(mut next) = next {
                next.as_mut().previous = previous;
            } else {
                self.tail = previous;
            }
            node.as_mut().linked = false;
            node.as_mut().previous = None;
            node.as_mut().next = None;
        }
    }

    fn select_one(&mut self) -> Option<Waker> {
        let mut next = self.head;
        while let Some(mut node) = next {
            // SAFETY: Called only with the Event lock held. The pointer never
            // survives unlocking; only the moved Waker leaves this method.
            unsafe {
                next = node.as_ref().next;
                // An in-progress broadcast owns older-generation waiters.
                if node.as_ref().generation == self.generation {
                    self.remove(node);
                    node.as_mut().selected = true;
                    return node.as_mut().waker.take();
                }
            }
        }
        None
    }
}

impl Event {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn listen(&self) -> Listener<'_> {
        Listener {
            event: self,
            node: UnsafeCell::new(Node {
                generation: self.list.lock().generation,
                previous: None,
                next: None,
                linked: false,
                selected: false,
                waker: None,
            }),
            _pin: PhantomPinned,
        }
    }

    pub(crate) fn notify_one(&self) {
        let waker = {
            let mut list = self.list.lock();
            if list.permit {
                return;
            }
            list.permit = true;
            list.select_one()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn notify_all(&self) {
        let generation = {
            let mut list = self.list.lock();
            list.generation = list
                .generation
                .checked_add(1)
                .expect("notification generation exhausted");
            list.generation
        };
        loop {
            let waker = {
                let mut list = self.list.lock();
                let Some(mut node) = list.head else {
                    return;
                };
                // SAFETY: The Event lock keeps this pinned node alive. Unlink
                // and move its Waker before unlocking. In particular, neither
                // this node nor a saved `next` is accessed after calling wake.
                unsafe {
                    // Poll appends only current-generation nodes, so this
                    // cutoff also excludes every later node, including any
                    // listener registered by a reentrant Waker.
                    if node.as_ref().generation >= generation {
                        return;
                    }
                    list.remove(node);
                    node.as_mut().waker.take()
                }
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event").finish_non_exhaustive()
    }
}

pub(crate) struct Listener<'a> {
    event: &'a Event,
    node: UnsafeCell<Node>,
    _pin: PhantomPinned,
}

// SAFETY: Node mutation, including removal during Drop, always holds the Event
// lock. Moving an unpinned listener is safe because it has not been inserted.
unsafe impl Send for Listener<'_> {}
// SAFETY: Shared access cannot move or poll the future. Even notification through
// another thread accesses its node only under the same Event lock.
unsafe impl Sync for Listener<'_> {}
impl std::panic::UnwindSafe for Listener<'_> {}
impl std::panic::RefUnwindSafe for Listener<'_> {}

impl Future for Listener<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.as_ref().get_ref();
        // Clone may run arbitrary application code, including another notify.
        let mut incoming = Some(cx.waker().clone());
        let (result, previous, forwarded) = {
            let mut list = this.event.list.lock();
            // SAFETY: Pin fixes the node's address, and all access to its
            // UnsafeCell and linked neighbors is serialized by this lock.
            unsafe {
                let mut pointer = NonNull::new_unchecked(this.node.get());
                let broadcast = pointer.as_ref().generation != list.generation;
                if broadcast || list.permit {
                    if pointer.as_ref().linked {
                        list.remove(pointer);
                    }
                    let previous = pointer.as_mut().waker.take();
                    let selected = std::mem::replace(&mut pointer.as_mut().selected, false);
                    let forwarded = if broadcast {
                        // Broadcast completion must not consume a stored permit.
                        if selected && list.permit {
                            list.select_one()
                        } else {
                            None
                        }
                    } else {
                        list.permit = false;
                        None
                    };
                    (Poll::Ready(()), previous, forwarded)
                } else {
                    pointer.as_mut().selected = false;
                    let previous = std::mem::replace(&mut pointer.as_mut().waker, incoming.take());
                    if !pointer.as_ref().linked {
                        list.insert(pointer);
                    }
                    (Poll::Pending, previous, None)
                }
            }
        };
        // Replacing or discarding a Waker can reenter just as wake can.
        drop(previous);
        drop(incoming);
        if let Some(waker) = forwarded {
            waker.wake();
        }
        result
    }
}

impl Drop for Listener<'_> {
    fn drop(&mut self) {
        let (previous, forwarded) = {
            let mut list = self.event.list.lock();
            // SAFETY: This node stays alive through Drop. It was pinned before
            // insertion and is unlinked under the lock before its storage dies.
            unsafe {
                let mut pointer = NonNull::new_unchecked(self.node.get());
                if pointer.as_ref().linked {
                    list.remove(pointer);
                }
                let previous = pointer.as_mut().waker.take();
                let forwarded = if pointer.as_ref().selected && list.permit {
                    list.select_one()
                } else {
                    None
                };
                (previous, forwarded)
            }
        };
        drop(previous);
        if let Some(waker) = forwarded {
            waker.wake();
        }
    }
}
