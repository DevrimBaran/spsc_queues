// B-Queue – batched SPSC queue (Wang 2013)

use crate::spsc::LamportQueue;
use crate::SpscQueue;
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Compile-time batch size (must be < ring capacity).
const BS: usize = 64;

pub struct BQueue<T: Send + 'static> {
   ring: LamportQueue<T>, // underlying bounded ring buffer
   // per-thread “local counters” – plain `Cell` is fine in true SPSC
   p_cnt: Cell<usize>,
   c_cnt: Cell<usize>,
   // shared commit counters
   p_commit: AtomicUsize,
   c_commit: AtomicUsize,
}

impl<T: Send + 'static> BQueue<T> {
   /// Capacity must be a power of two and ≥ `BS`.
   pub fn new(capacity: usize) -> Self {
      assert!(capacity.is_power_of_two());
      assert!(capacity >= BS);
      Self {
         ring: LamportQueue::with_capacity(capacity),
         p_cnt: Cell::new(0),
         c_cnt: Cell::new(0),
         p_commit: AtomicUsize::new(0),
         c_commit: AtomicUsize::new(0),
      }
   }
}

impl<T: Send + 'static> SpscQueue<T> for BQueue<T> {
   type PushError = ();
   type PopError  = ();

   // producer side
   fn push(&self, item: T) -> Result<(), ()> {
      // try the fast path
      self.ring.push(item)?;

      let produced = self.p_cnt.get() + 1;
      if produced == BS {
         // commit one full batch
         self.p_commit.store(BS, Ordering::Release);
         self.p_cnt.set(0);
      } else {
         self.p_cnt.set(produced);
      }
      Ok(())
   }

   // consumer side
   fn pop(&self) -> Result<T, ()> {
      // fast path
      if let Ok(v) = self.ring.pop() {
         let consumed = self.c_cnt.get() + 1;
         if consumed == BS {
               // let the producer know we consumed a batch
               self.c_commit.store(BS, Ordering::Release);
               self.c_cnt.set(0);
         } else {
               self.c_cnt.set(consumed);
         }
         return Ok(v);
      }

      // slow path – check if the producer has committed a new batch
      if self.p_commit.swap(0, Ordering::Acquire) > 0 {
         // there should be data now; one more try
         return self.ring.pop();
      }

      Err(())
   }

   // status helpers
   fn available(&self) -> bool {
      self.ring.available() || self.p_commit.load(Ordering::Acquire) > 0
   }

   fn empty(&self) -> bool {
      self.ring.empty() && self.p_commit.load(Ordering::Acquire) == 0
   }
}
