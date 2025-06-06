// lamports buffer ring

use crate::SpscQueue;
use std::{
   cell::UnsafeCell,
   mem::ManuallyDrop,
   sync::atomic::{AtomicUsize, Ordering},
};

// Ring header

#[derive(Debug)]
pub struct LamportQueue<T: Send> {
   mask: usize, // cap − 1
   pub buf : ManuallyDrop<Box<[UnsafeCell<Option<T>>]>>, // shared ring storage (pub so dspsc can use it)
   head: AtomicUsize, // mutated by consumer
   tail: AtomicUsize, // mutated by producer
}

unsafe impl<T: Send> Sync for LamportQueue<T> {}
unsafe impl<T: Send> Send for LamportQueue<T> {}

// heap-backed constructor
impl<T: Send> LamportQueue<T> {
   // Build a queue that lives on the Rust heap.
   pub fn with_capacity(cap: usize) -> Self {
      assert!(cap.is_power_of_two(), "capacity must be power of two");

      let boxed = (0..cap)
         .map(|_| UnsafeCell::new(None))
         .collect::<Vec<_>>()
         .into_boxed_slice();

      Self {
         mask: cap - 1,
         buf : ManuallyDrop::new(boxed),
         head: AtomicUsize::new(0),
         tail: AtomicUsize::new(0),
      }
   }

   #[inline]
   fn idx(&self, i: usize) -> usize {
      i & self.mask
   }
}

// shared-memory in-place constructor
impl<T: Send> LamportQueue<T> {
   /// Bytes required for header + `cap` elements.
   pub const fn shared_size(cap: usize) -> usize {
      std::mem::size_of::<Self>()
      + cap * std::mem::size_of::<UnsafeCell<Option<T>>>()
   }

   // Safety: `mem` must point to a writable, process-shared mapping of at least
   // `shared_size(cap)` bytes which lives for `'static`.
   pub unsafe fn init_in_shared(mem: *mut u8, cap: usize) -> &'static mut Self {
      assert!(cap.is_power_of_two());

      let header = mem as *mut Self;
      let buf_ptr = mem.add(std::mem::size_of::<Self>())
                     as *mut UnsafeCell<Option<T>>;

      let slice = std::slice::from_raw_parts_mut(buf_ptr, cap);
      let boxed = Box::from_raw(slice);

      header.write(Self {
         mask: cap - 1,
         buf : ManuallyDrop::new(boxed),
         head: AtomicUsize::new(0),
         tail: AtomicUsize::new(0),
      });

      &mut *header
   }
}

// helper for mspsc:
impl<T: Send> LamportQueue<T> {
   /// Ring capacity (power‑of‑two)
   #[inline] pub fn capacity(&self) -> usize { self.mask + 1 }

   /// **Producer** cursor (called `head` in Torquati’s multipush code).
   #[inline] pub fn head_relaxed(&self) -> usize {           // producer‑only
       self.tail.load(Ordering::Relaxed)
   }

   /// **Consumer** cursor (`tail` in Torquati’s notation).
   #[inline] pub fn tail_relaxed(&self) -> usize {           // consumer‑only
       self.head.load(Ordering::Relaxed)
   }

   /// Write without checking space.  Caller guarantees at least one free slot.
   /// Used only by the producer side of MultiPushQueue.
   #[inline]
   pub unsafe fn push_unchecked(&mut self, item: T) {
       let tail = self.tail.load(Ordering::Relaxed);
       let slot = self.idx(tail);
       (*self.buf[slot].get()) = Some(item);
       self.tail.store(tail.wrapping_add(1), Ordering::Relaxed);
   }
}

// queue operations
impl<T: Send + 'static> SpscQueue<T> for LamportQueue<T> {
   type PushError = ();
   type PopError  = ();

   #[inline]
   fn push(&self, item: T) -> Result<(), ()> {
      // CHANGES: Relaxed -> Acquire/Release memory ordering for process safety
      
      // Load the current tail position
      let tail = self.tail.load(Ordering::Acquire);
      let next = tail + 1;

      // Check if queue is full by calculating the next tail position
      // and comparing with head (adjusting for mask)
      let head = self.head.load(Ordering::Acquire);
      if next == head + self.mask + 1 {
         return Err(());
      }

      // Store the item at the current tail position
      // one producer ⇒ this is safe
      let slot = self.idx(tail);
      unsafe { *self.buf[slot].get() = Some(item) };
      
      // Update the tail position with a release memory ordering
      // to ensure the item is visible before incrementing the tail
      self.tail.store(next, Ordering::Release);
      Ok(())
   }

   #[inline]
   fn pop(&self) -> Result<T, ()> {
      // CHANGES: Process-safer implementation with strong memory orderings
      
      // Check if the queue is empty
      let head = self.head.load(Ordering::Acquire);
      let tail = self.tail.load(Ordering::Acquire);
      
      if head == tail {
         return Err(());
      }

      // Calculate the slot index for the current head
      let slot = self.idx(head);
      
      // Take the item from the queue
      // We use take() to move the value out, leaving None in its place
      let cell_ptr = &self.buf[slot];
      let val = unsafe {         
         // Extract the value
         (*cell_ptr.get()).take()
      };

      // Process the result
      match val {
         Some(v) => {
            // Increment the head position with a release memory ordering
            self.head.store(head + 1, Ordering::Release);
            Ok(v)
         }
         None => Err(()) // Either a double-pop or uninitialized (shouldn't happen)
      }
   }

   #[inline]
   fn available(&self) -> bool {
      // changes: Use consistent memory ordering
      let tail = self.tail.load(Ordering::Acquire);
      let head = self.head.load(Ordering::Acquire);
      tail.wrapping_sub(head) < self.mask
   }

   #[inline]
   fn empty(&self) -> bool {
      // changes: Use consistent memory ordering for both loads
      let head = self.head.load(Ordering::Acquire);
      let tail = self.tail.load(Ordering::Acquire);
      head == tail
   }
}