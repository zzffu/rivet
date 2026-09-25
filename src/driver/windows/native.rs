use std::ptr::NonNull;

/// Cold-allocated native storage with no Rust reference to its contents while
/// Windows may write them. Borrowing the owner only borrows a raw pointer, not
/// the allocation (including through an enclosing `&mut Driver` or `&mut Rio`).
/// The owning driver must drain requests / close the CQ before dropping this.
pub(super) struct Native<T: ?Sized> {
    pointer: NonNull<T>,
}

impl<T: ?Sized> Native<T> {
    pub fn new(storage: Box<T>) -> Self {
        Self {
            pointer: NonNull::new(Box::into_raw(storage)).unwrap(),
        }
    }

    pub fn as_ptr(&self) -> *mut T {
        self.pointer.as_ptr()
    }
}

impl<T: ?Sized> Drop for Native<T> {
    fn drop(&mut self) {
        // No native access remains when the owner releases this allocation.
        unsafe {
            drop(Box::from_raw(self.pointer.as_ptr()));
        }
    }
}
