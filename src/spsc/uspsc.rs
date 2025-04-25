use crate::{spsc::LamportQueue, SpscQueue};
use crossbeam::queue::ArrayQueue;
use std::cell::UnsafeCell;

const BUF_CAP: usize = 1024;
const POOL_CAP: usize = 32;

pub struct UnboundedQueue<T: Send + 'static> {
   // the producer mutates `write`, the consumer mutates `read`
   write: UnsafeCell<LamportQueue<T>>,
   read:  UnsafeCell<LamportQueue<T>>,
   pool:  ArrayQueue<LamportQueue<T>>,
}

// SPSC ⇒ at most one process reads each field; the impls are sound
unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
   pub fn new() -> Self {
      Self {
         write: UnsafeCell::new(LamportQueue::with_capacity(BUF_CAP)),
         read:  UnsafeCell::new(LamportQueue::with_capacity(BUF_CAP)),
         pool:  ArrayQueue::new(POOL_CAP),
      }
   }

   #[inline(always)]
   fn w(&self) -> &LamportQueue<T> { unsafe { &*self.write.get() } }
   #[inline(always)]
   fn w_mut(&self) -> &mut LamportQueue<T> { unsafe { &mut *self.write.get() } }
   #[inline(always)]
   fn r(&self) -> &LamportQueue<T> { unsafe { &*self.read.get() } }
   #[inline(always)]
   fn r_mut(&self) -> &mut LamportQueue<T> { unsafe { &mut *self.read.get() } }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
   type PushError = ();           // never fails (unbounded)
   type PopError  = ();           // empty queue

   fn push(&self, item: T) -> Result<(), Self::PushError> {
   // guarantee space in the current write-buffer;
   // we *haven’t* moved `item` yet, so no ownership issues.
   if !self.w().available() {
         let new_buf  = LamportQueue::with_capacity(BUF_CAP);
         let full_buf = std::mem::replace(self.w_mut(), new_buf);
         let _ = self.pool.push(full_buf);           // silently drop on overflow
      }

      // now this must succeed
      let _ = self.w().push(item);
      Ok(())
   }

   fn pop(&self) -> Result<T, Self::PopError> {
      // fast path
      if let Ok(v) = self.r().pop() {
         return Ok(v);
      }

      // current read-buffer empty – try to grab the next one
      if let Some(next) = self.pool.pop() {
         let old = std::mem::replace(self.r_mut(), next);
         // old buffer is empty, put it back for reuse
         let _ = self.pool.push(old);
         return self.r().pop();     // second try (OK or still empty)
      }

      Err(())  // completely empty
   }

   fn available(&self) -> bool { self.w().available() || !self.pool.is_empty() }
   fn empty(&self)      -> bool { self.r().empty()     &&  self.pool.is_empty() }
}
