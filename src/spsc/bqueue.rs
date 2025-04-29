#![allow(clippy::cast_possible_truncation)]

use crate::SpscQueue;
use std::cell::Cell;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr;

// B-Queue from Wang
#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    buf: *mut MaybeUninit<Option<T>>,
    cap: usize,
    mask: usize,
    head: Cell<usize>, // next slot to write (producer)
    batch_head: Cell<usize>, // probe boundary (producer)
    tail: Cell<usize>, // next slot to read (consumer)
    batch_tail: Cell<usize>, // probe boundary (consumer)
    history: Cell<usize>, // adaptive backtracking start size
}

// Fixed batch size constant, available throughout the module
const BATCH_SIZE: usize = 256;

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    // Create an in-process queue with `capacity` = power-of-two.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let mut v: Vec<MaybeUninit<Option<T>>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(MaybeUninit::new(None));
        }
        let buf = Box::into_raw(v.into_boxed_slice()) as *mut MaybeUninit<Option<T>>;
        let edge = capacity - 1;
        let init_hist = BATCH_SIZE.min(capacity);
        BQueue {
            buf,
            cap: capacity,
            mask: edge,
            head: Cell::new(0),
            batch_head: Cell::new(edge),
            tail: Cell::new(0),
            batch_tail: Cell::new(edge),
            history: Cell::new(init_hist),
        }
    }

    // Bytes needed for header + buffer slots.
    pub const fn shared_size(capacity: usize) -> usize {
        mem::size_of::<Self>() + capacity * mem::size_of::<MaybeUninit<Option<T>>>()
    }

    // Initialize a shared-memory queue in-place.
    // Safety: mem` must point to at least `shared_size(capacity)` bytes of MAP_SHARED.
    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let header = mem as *mut Self;
        let buf_ptr = mem.add(mem::size_of::<Self>()) as *mut MaybeUninit<Option<T>>;
        ptr::write(header, BQueue::new_inplace(buf_ptr, capacity));
        for i in 0..capacity {
            ptr::write(buf_ptr.add(i), MaybeUninit::new(None));
        }
        &mut *header
    }

    // Internal constructor for in-place init.
    fn new_inplace(buf: *mut MaybeUninit<Option<T>>, capacity: usize) -> Self {
        let edge = capacity - 1;
        BQueue {
            buf,
            cap: capacity,
            mask: edge,
            head: Cell::new(0),
            batch_head: Cell::new(edge),
            tail: Cell::new(0),
            batch_tail: Cell::new(edge),
            history: Cell::new(BATCH_SIZE.min(capacity)),
        }
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask
    }

    // Enqueue an item. Returns Err(item) if queue is full.
    pub fn push(&self, item: T) -> Result<(), T> {
        let h = self.head.get();
        if h == self.batch_head.get() {
            let mut bh = (h + BATCH_SIZE - 1) & self.mask;
            unsafe {
                if (*self.buf.add(bh)).assume_init_ref().is_some() {
                    return Err(item);
                }
            }
            self.batch_head.set(bh);
        }
        unsafe {
            let slot = self.buf.add(h) as *mut Option<T>;
            ptr::write(slot, Some(item));
        }
        self.head.set(self.next(h));
        Ok(())
    }

    // Dequeue an item. Returns Err(()) if queue is empty.
    pub fn pop(&self) -> Result<T, ()> {
        let t = self.tail.get();
        if t != self.batch_tail.get() {
            unsafe {
                let slot = self.buf.add(t) as *mut Option<T>;
                if let Some(v) = ptr::read(slot) {
                    ptr::write(slot, None);
                    self.tail.set(self.next(t));
                    return Ok(v);
                }
            }
        }
        // backtracking with adaptive start
        let mut size = self.history.get().min(self.cap);
        let mut bt = t;
        loop {
            bt = (bt + size - 1) & self.mask;
            unsafe {
                if (*self.buf.add(bt)).assume_init_ref().is_some() {
                    self.batch_tail.set(bt);
                    self.history.set(size);
                    break;
                }
            }
            size >>= 1;
            if size == 0 {
                self.history.set(BATCH_SIZE.min(self.cap));
                return Err(());
            }
        }
        // retry fast path
        let t2 = self.tail.get();
        unsafe {
            let slot = self.buf.add(t2) as *mut Option<T>;
            if let Some(v) = ptr::read(slot) {
                ptr::write(slot, None);
                self.tail.set(self.next(t2));
                let dist = (bt.wrapping_sub(t) & self.mask).wrapping_add(1);
                self.history.set(dist.min(self.cap));
                return Ok(v);
            }
        }
        Err(())
    }

    // True when a subsequent push may succeed.
    pub fn available(&self) -> bool {
        let h = self.head.get();
        let bh = self.batch_head.get();
        if h != bh { return true; }
        unsafe { (*self.buf.add(bh)).assume_init_ref().is_none() }
    }

    // True when a subsequent pop will fail.
    pub fn empty(&self) -> bool {
        let t = self.tail.get();
        let bt = self.batch_tail.get();
        if t != bt { return false; }
        unsafe { (*self.buf.add(bt)).assume_init_ref().is_none() }
    }
}

impl<T: Send + 'static> SpscQueue<T> for BQueue<T> {
    type PushError = ();
    type PopError = ();
    fn push(&self, item: T) -> Result<(), Self::PushError> { self.push(item).map_err(|_| ()) }
    fn pop(&self) -> Result<T, Self::PopError> { self.pop() }
    fn available(&self) -> bool { self.available() }
    fn empty(&self) -> bool { self.empty() }
}

impl<T: Send + 'static> Drop for BQueue<T> {
    fn drop(&mut self) {
        // drain any remaining
        while let Ok(_) = unsafe { &*self }.pop() {}
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.buf, self.cap));
        }
    }
}
