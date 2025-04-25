// dSPSC – dynamic list with node cache
use crate::SpscQueue;
use std::ptr::{null_mut, NonNull};
use std::sync::atomic::{AtomicPtr, Ordering};

struct Node<T> { val: Option<T>, next: *mut Node<T> }

pub struct DynListQueue<T: Send + 'static> {
      head: AtomicPtr<Node<T>>, // consumer reads
      tail: AtomicPtr<Node<T>>, // producer writes
      cache: crossbeam::queue::ArrayQueue<NonNull<Node<T>>>,
}
unsafe impl<T: Send> Sync for DynListQueue<T> {}
unsafe impl<T: Send + 'static> Send for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
      pub fn new(cache_sz: usize) -> Self {
         let dummy = Box::into_raw(Box::new(Node{val:None,next:null_mut()}));
         Self{ head:AtomicPtr::new(dummy), tail:AtomicPtr::new(dummy), cache:crossbeam::queue::ArrayQueue::new(cache_sz)}
      }
      fn alloc_node(&self, v:T)->*mut Node<T>{
         if let Some(nn) = self.cache.pop(){ 
            unsafe{
               let p = nn.as_ptr();
               
               (*p).val=Some(v);
               (*p).next=null_mut();
               return p;
            } 
         }
         Box::into_raw(Box::new(Node{val:Some(v),next:null_mut()}))
      }
}
impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
      type PushError = ();
      type PopError = ();
      fn push(&self,item:T)->Result<(),Self::PushError>{
         let n=self.alloc_node(item);
         unsafe{(*self.tail.load(Ordering::Relaxed)).next=n};
         self.tail.store(n,Ordering::Release);
         Ok(())
      }
      fn pop(&self)->Result<T,Self::PopError>{
         let head=self.head.load(Ordering::Acquire);
         let next=unsafe{(*head).next};
         if next.is_null(){ return Err(());}            
         let res=unsafe{(*next).val.take().unwrap()};
         self.head.store(next,Ordering::Release);
         if self
            .cache
            .push(NonNull::new(head).unwrap())
            .is_err()
         {
            unsafe { drop(Box::from_raw(head)); }
         }
         Ok(res)
      }
      fn available(&self)->bool{true}
      fn empty(&self)->bool{ unsafe{(*self.head.load(Ordering::Relaxed)).next}.is_null() }
}