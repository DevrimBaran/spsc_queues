// multi push spsc queue from Torquati

use crate::spsc::LamportQueue;
use crate::SpscQueue;
use std::{
    cell::UnsafeCell,
    fmt,
    mem::MaybeUninit,
    ptr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

// Pre-allocated local buffer for batch operations
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

    /// Safety: `mem` must be at least `shared_size(cap)` bytes,
    /// writable and `MAP_SHARED`.
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

    // flush operation: flushes as many items as possible
    #[inline]
    fn flush_local(&self) -> bool {
        let buf = unsafe { &mut *self.local_buf.get() };
        let count = self.local_count.load(Ordering::Relaxed);
        
        if count == 0 {
            return true;
        }
        
        let mut new_count = count;
        let mut flushed = false;
        
        // Try to flush items, but don't loop indefinitely
        for i in (0..count).rev() {
            if !self.ring().available() {
                break;
            }
            
            // Get the item pointer without moving it yet
            let item_ptr = buf[i].as_mut_ptr();
            
            // Create a copy of the item
            let success = unsafe {
                let item = ptr::read(item_ptr);
                self.ring_mut().push(item).is_ok()
            };
            
            if success {
                // Success - mark as flushed
                new_count = i;
                flushed = true;
            } else {
                // Push failed - we do not need to restore the item
                // because we used ptr::read which makes a copy
                break;
            }
        }
        
        // Update the count if we flushed anything
        if flushed {
            self.local_count.store(new_count, Ordering::Relaxed);
        }
        
        flushed
    }
}

impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {
        // Try to flush remaining items but don't block
        self.flush_local();
        
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
        
        // If buffer is full, try to flush
        if count == LOCAL_BUF {
            self.flush_local();
            
            // Check again after flushing
            let new_count = self.local_count.load(Ordering::Relaxed);
            
            // If still full after flushing, try direct push to ring
            if new_count == LOCAL_BUF {
                // Direct push to the ring (consumes item)
                return self.ring_mut().push(item);
            }
            
            // Use the new count after flushing
            let idx = new_count;
            
            // Try to increment the count atomically
            if self.local_count.compare_exchange(
                new_count, 
                new_count + 1, 
                Ordering::AcqRel, 
                Ordering::Relaxed
            ).is_ok() {
                // Successfully claimed a slot
                unsafe {
                    let buf = &mut *self.local_buf.get();
                    ptr::write(buf[idx].as_mut_ptr(), item);
                }
                
                return Ok(());
            } else {
                // Someone else took our slot (shouldn't happen in SPSC),
                // try direct push
                return self.ring_mut().push(item);
            }
        }
        
        // Buffer is not full, try to add the item
        // Use the compare_exchange to ensure we get the right slot
        let idx = count;
        
        if self.local_count.compare_exchange(
            count, 
            count + 1, 
            Ordering::AcqRel, 
            Ordering::Relaxed
        ).is_ok() {
            // Successfully claimed a slot
            unsafe {
                let buf = &mut *self.local_buf.get();
                ptr::write(buf[idx].as_mut_ptr(), item);
            }
            
            // If buffer is full now, try to flush
            if idx + 1 == LOCAL_BUF {
                self.flush_local();
            }
            
            Ok(())
        } else {
            // Slot was taken by another thread (shouldn't happen in SPSC),
            // try direct push (consumes item)
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