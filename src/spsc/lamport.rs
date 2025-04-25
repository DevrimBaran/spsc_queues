// Wait-free bounded single-producer/single-consumer (SPSC) queue

// A direct Rust translation of Leslie Lamport’s 1979 ring-buffer algorithm.
// The implementation is *wait-free* (each operation completes after a
// bounded number of steps) and therefore also lock-free.

use crate::SpscQueue;
use std::{
   cell::UnsafeCell,
   sync::atomic::{AtomicUsize, Ordering},
};

// Bounded SPSC queue like Lamport’s original
#[derive(Debug)]
pub struct LamportQueue<T: Send> {
   mask: usize, // `cap - 1`, allowing us to calculate `idx = i & mask` cheaply.
   buf:  Box<[UnsafeCell<Option<T>>]>, // Ring storage, `UnsafeCell` is needed to allow mutable access to the ring buffer
   head: AtomicUsize, // mutated only by the consumer
   tail: AtomicUsize, // mutated only by the producer
}

// we declare the queue as `Send` and `Sync` to allow it to be shared between processes.
// This is safe because synchronisation happens through atomic operations.
unsafe impl<T: Send> Sync for LamportQueue<T> {}

impl<T: Send> LamportQueue<T> {
   // Build a queue that lives on the Rust heap.
   pub fn with_capacity(cap: usize) -> Self {
      assert!(cap.is_power_of_two(), "capacity must be power of two");
      // initialise the ring buffer with `None` values
      let buf = (0..cap)
         .map(|_| UnsafeCell::new(None))
         .collect::<Vec<_>>()
         .into_boxed_slice();

      Self {
         mask: cap - 1,
         buf,
         head: AtomicUsize::new(0),
         tail: AtomicUsize::new(0),
      }
   }

   #[inline]
   // Convert unbounded monotonically increasing index into ring index
   fn idx(&self, i: usize) -> usize {
      i & self.mask
   }

   // Get bytes that are required for a queue of cap elements
   #[inline]
   pub const fn shared_size(cap: usize) -> usize {
      std::mem::size_of::<Self>()
         + cap * std::mem::size_of::<UnsafeCell<Option<T>>>()
   }

   // Initialise a queue in place inside the memory block at mem.
   pub unsafe fn init_in_shared(mem: *mut u8, cap: usize) -> &'static mut Self {
      assert!(cap.is_power_of_two());

      // carve block into header + ring storage
      let header_ptr = mem as *mut Self;
      let buf_ptr = mem.add(std::mem::size_of::<Self>())
         as *mut UnsafeCell<Option<T>>;

      // turn ring storage into a boxed slice *without allocating*
      let buf_slice = std::slice::from_raw_parts_mut(buf_ptr, cap);
      let boxed: Box<[UnsafeCell<Option<T>>]> = Box::from_raw(buf_slice);

      // write header in place
      header_ptr.write(Self {
         mask: cap - 1,
         buf:  boxed,
         head: AtomicUsize::new(0),
         tail: AtomicUsize::new(0),
      });

      &mut *header_ptr // return a mutable reference to the header
   }
}

//  SpscQueue impl of lamport
impl<T: Send + 'static> SpscQueue<T> for LamportQueue<T> {
   type PushError = ();
   type PopError  = ();

   // Producer side, try to enqueue an item
   #[inline]
   fn push(&self, item: T) -> Result<(), ()> {
      let tail = self.tail.load(Ordering::Relaxed);
      let next = tail + 1;
      
      // check if the queue is full
      if next == self.head.load(Ordering::Acquire) + self.mask + 1 {
         return Err(()); 
      }
      // acutally safe, since we have only one producer writing to this slot
      unsafe { *self.buf[self.idx(tail)].get() = Some(item) };
      self.tail.store(next, Ordering::Release);
      Ok(())
   }

   // Consumer side, try to dequeue an item
   #[inline]
   fn pop(&self) -> Result<T, ()> {
      let head = self.head.load(Ordering::Relaxed);

      // check if the queue is empty
      if head == self.tail.load(Ordering::Acquire) {
         return Err(());
      }

      // acutally safe, since we have only one consumer reading from this slot
      let res = unsafe { (*self.buf[self.idx(head)].get()).take().unwrap() };
      self.head.store(head + 1, Ordering::Release);
      Ok(res)
   }

   #[inline]
   fn available(&self) -> bool {
      let tail = self.tail.load(Ordering::Relaxed);
      let head = self.head.load(Ordering::Acquire);
      tail.wrapping_sub(head) < self.mask 
   }

   #[inline]
   fn empty(&self) -> bool {
      self.head.load(Ordering::Relaxed) == self.tail.load(Ordering::Relaxed)
   }
}
