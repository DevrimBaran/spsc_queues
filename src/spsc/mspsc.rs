// mSPSC (Lamport + batch back‑write)  – simplified cache‑friendly version
use crate::spsc::LamportQueue;
use crate::SpscQueue;

const LOCAL_BUF: usize = 16;

pub struct MultiPushQueue<T: Send> {
      inner: LamportQueue<T>,
      local: std::cell::RefCell<Vec<T>>,
}
impl<T: Send + 'static> MultiPushQueue<T> {
      pub fn with_capacity(c: usize) -> Self {
         Self { inner: LamportQueue::with_capacity(c), local: std::cell::RefCell::new(Vec::with_capacity(LOCAL_BUF)) }
      }
      #[inline] fn flush_local(&self) {
         let mut local = self.local.borrow_mut();
         while let Some(v) = local.pop() {
            // write backwards for distance
            let _ = self.inner.push(v);
         }
      }
}
impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
      type PushError = ();
      type PopError = <LamportQueue<T> as SpscQueue<T>>::PopError;
      fn push(&self, item: T) -> Result<(), Self::PushError> {
         let mut l = self.local.borrow_mut();
         l.push(item);
         if l.len()==LOCAL_BUF { drop(l); self.flush_local(); }
         Ok(())
      }
      fn pop(&self) -> Result<T, Self::PopError> { self.inner.pop() }
      fn available(&self) -> bool { self.inner.available() }
      fn empty(&self) -> bool { self.inner.empty() }
}