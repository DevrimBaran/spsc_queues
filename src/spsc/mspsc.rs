// src/spsc/mspsc.rs
use crate::spsc::LamportQueue;
use crate::SpscQueue;
use std::{
    cell::RefCell,
    fmt,
    mem::MaybeUninit,
    ptr,
    sync::atomic::{AtomicBool, Ordering},
};

const LOCAL_BUF: usize = 16;

pub struct MultiPushQueue<T: Send + 'static> {
    inner: *mut LamportQueue<T>,
    local: RefCell<Vec<T>>,
    shared: AtomicBool,
}

unsafe impl<T: Send> Sync for MultiPushQueue<T> {}
unsafe impl<T: Send> Send for MultiPushQueue<T> {}

impl<T: Send + 'static> MultiPushQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let boxed = Box::new(LamportQueue::with_capacity(cap));
        let ptr = Box::into_raw(boxed);
        MultiPushQueue {
            inner: ptr,
            local: RefCell::new(Vec::with_capacity(LOCAL_BUF)),
            shared: AtomicBool::new(false),
        }
    }

    /// Safety: `mem` must be at least `shared_size(cap)` bytes,
    /// writable and `MAP_SHARED`.
    pub unsafe fn init_in_shared(mem: *mut u8, cap: usize) -> &'static mut Self {
        let header = mem as *mut MaybeUninit<Self>;
        let ring_ptr = mem.add(std::mem::size_of::<Self>());
        let ring = LamportQueue::init_in_shared(ring_ptr, cap) as *mut _;
        ptr::write(
            header,
            MaybeUninit::new(MultiPushQueue {
                inner: ring,
                local: RefCell::new(Vec::with_capacity(LOCAL_BUF)),
                shared: AtomicBool::new(true),
            }),
        );
        &mut *(*header).as_mut_ptr()
    }

    pub const fn shared_size(cap: usize) -> usize {
        std::mem::size_of::<Self>() + LamportQueue::<T>::shared_size(cap)
    }

    #[inline]
    fn ring(&self) -> &LamportQueue<T> {
        unsafe { &*self.inner }
    }
    #[inline]
    fn ring_mut(&self) -> &mut LamportQueue<T> {
        unsafe { &mut *self.inner }
    }

    #[inline]
    fn flush_local(&self) {
        use std::hint::spin_loop;
        let mut buf = self.local.borrow_mut();
        while let Some(item) = buf.pop() {
            while !self.ring().available() {
                spin_loop();
            }
            let _ = self.ring_mut().push(item);
        }
    }
}

// **Here’s the change**: carry the `'static` bound through
impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {
        // flush any leftover items
        self.flush_local();
        // free the heap‐backed variant only
        if !self.shared.load(Ordering::Relaxed) {
            unsafe { drop(Box::from_raw(self.inner)) };
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
    type PushError = ();
    type PopError  = <LamportQueue<T> as SpscQueue<T>>::PopError;

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let mut buf = self.local.borrow_mut();
        buf.push(item);
        if buf.len() == LOCAL_BUF {
            drop(buf);
            self.flush_local();
        }
        Ok(())
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.ring().pop()
    }

    fn available(&self) -> bool {
        self.ring().available()
    }

    fn empty(&self) -> bool {
        self.ring().empty()
    }
}

impl<T: Send> fmt::Debug for MultiPushQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiPushQueue")
         .field("shared", &self.shared.load(Ordering::Relaxed))
         .finish()
    }
}
