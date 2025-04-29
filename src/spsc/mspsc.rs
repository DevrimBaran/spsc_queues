// multi push spsc queue from Torquati

use crate::spsc::LamportQueue;
use crate::SpscQueue;
use std::{
    cell::UnsafeCell,
    fmt,
    mem::MaybeUninit,
    ptr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence},
};

// Pre-allocated local buffer for batch operations
// Size from the paper (Fig. 4)
const LOCAL_BUF: usize = 16;

pub struct MultiPushQueue<T: Send + 'static> {
    inner: *mut LamportQueue<T>,
    // UnsafeCell for local buffer and counter
    local_buf: UnsafeCell<[MaybeUninit<T>; LOCAL_BUF]>,
    local_count: AtomicUsize,
    shared: AtomicBool,
}

unsafe impl<T: Send> Sync for MultiPushQueue<T> {}
unsafe impl<T: Send> Send for MultiPushQueue<T> {}

impl<T: Send + 'static> MultiPushQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let boxed = Box::new(LamportQueue::with_capacity(cap));
        let ptr = Box::into_raw(boxed);
        
        // Initialize with empty buffer
        let buffer = unsafe { MaybeUninit::<[MaybeUninit<T>; LOCAL_BUF]>::uninit().assume_init() };
        
        MultiPushQueue {
            inner: ptr,
            local_buf: UnsafeCell::new(buffer),
            local_count: AtomicUsize::new(0),
            shared: AtomicBool::new(false),
        }
    }

    // Safety: `mem` must be at least `shared_size(cap)` bytes,writable and `MAP_SHARED`.
    pub unsafe fn init_in_shared(mem: *mut u8, cap: usize) -> &'static mut Self {
        let header = mem as *mut MaybeUninit<Self>;
        let ring_ptr = mem.add(std::mem::size_of::<Self>());
        let ring = LamportQueue::init_in_shared(ring_ptr, cap) as *mut _;
        
        // Initialize with empty buffer
        let buffer = MaybeUninit::<[MaybeUninit<T>; LOCAL_BUF]>::uninit().assume_init();
        
        ptr::write(
            header,
            MaybeUninit::new(MultiPushQueue {
                inner: ring,
                local_buf: UnsafeCell::new(buffer),
                local_count: AtomicUsize::new(0),
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

    // Implementation of the multipush method as described in the paper
    #[inline]
    fn multipush(&self) -> bool {
        let buf = unsafe { &mut *self.local_buf.get() };
        let count = self.local_count.load(Ordering::Relaxed);
        
        if count == 0 {
            return true;
        }

        // Check if there's enough space in the underlying queue
        if !self.ring().available() {
            return false;
        }
        
        // Write items in backward order to enforce distance
        // between producer and consumer pointers
        let mut success = true;
        
        // insert in reverse order like in paper
        for i in (0..count).rev() {
            // Get the item pointer
            let item_ptr = buf[i].as_mut_ptr();
            
            // Move the item to the ring
            let push_result = unsafe {
                let item = ptr::read(item_ptr);
                self.ring_mut().push(item)
            };
            
            if push_result.is_err() {
                success = false;
                break;
            }
        }
        
        // If we successfully pushed all items, reset the counter
        if success {
            self.local_count.store(0, Ordering::Relaxed);
        }
        
        success
    }
}

impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {
        // Try to flush remaining items
        self.multipush();
        
        // Free the heap-backed variant only
        if !self.shared.load(Ordering::Relaxed) {
            unsafe { 
                // Drop any remaining items in the local buffer
                let buf = &mut *self.local_buf.get();
                let count = self.local_count.load(Ordering::Relaxed);
                
                for i in 0..count {
                    ptr::drop_in_place(buf[i].as_mut_ptr());
                }
                
                drop(Box::from_raw(self.inner));
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
    type PushError = ();
    type PopError  = <LamportQueue<T> as SpscQueue<T>>::PopError;

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let count = self.local_count.load(Ordering::Relaxed);
        
        // If buffer is full, flush it
        if count >= LOCAL_BUF {
            // Try to flush the buffer
            if !self.multipush() {
                // If flush failed, try direct push
                return self.ring_mut().push(item);
            }
            // Reset count since multipush succeeded
            // (done inside multipush method)
        }
        
        // Add to local buffer - simpler approach for SPSC
        let idx = self.local_count.fetch_add(1, Ordering::Relaxed);
        if idx < LOCAL_BUF {
            unsafe {
                let buf = &mut *self.local_buf.get();
                ptr::write(buf[idx].as_mut_ptr(), item);
            }
            
            // If buffer is now full, try to flush it
            if idx + 1 == LOCAL_BUF {
                self.multipush();
            }
            
            Ok(())
        } else {
            // Buffer was already full
            // direct push
            self.ring_mut().push(item)
        }
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.ring().pop()
    }

    fn available(&self) -> bool {
        let count = self.local_count.load(Ordering::Relaxed);
        self.ring().available() || count < LOCAL_BUF
    }

    fn empty(&self) -> bool {
        let count = self.local_count.load(Ordering::Relaxed);
        self.ring().empty() && count == 0
    }
}

impl<T: Send> fmt::Debug for MultiPushQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiPushQueue")
        .field("shared", &self.shared.load(Ordering::Relaxed))
        .field("local_count", &self.local_count.load(Ordering::Relaxed))
        .finish()
    }
}