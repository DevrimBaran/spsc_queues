use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use crate::SpscQueue;

#[derive(Debug)]
pub struct DehnaviQueue<T: Send + 'static> { 
   pub(crate) buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
   pub capacity: usize, 
   pub wc: AtomicUsize, 
   pub rc: AtomicUsize, 
   pub(crate) pclaim: AtomicBool, 
   pub(crate) cclaim: AtomicBool, 
}

#[derive(Debug, PartialEq, Eq)]
pub struct PushError<T>(pub T); 

#[derive(Debug, PartialEq, Eq)]
pub struct PopError; 

impl<T: Send + 'static> DehnaviQueue<T> { 
   pub fn new(capacity: usize) -> Self {
      // CRITICAL: This assertion must be active for the test to pass.
      assert!(capacity > 1, "Capacity (k) must be greater than 1 for DehnaviQueue.");
      
      let buffer_size = capacity;
      let mut buffer_vec = Vec::with_capacity(buffer_size);
      for _ in 0..buffer_size {
         buffer_vec.push(UnsafeCell::new(MaybeUninit::uninit()));
      }
      Self {
         buffer: buffer_vec.into_boxed_slice(),
         capacity: buffer_size, 
         wc: AtomicUsize::new(0),
         rc: AtomicUsize::new(0),
         pclaim: AtomicBool::new(false),
         cclaim: AtomicBool::new(false),
      }
   }
   
   pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
      // CRITICAL: This assertion must be active.
      assert!(capacity > 1, "Capacity (k) must be greater than 1 for DehnaviQueue.");
      let buffer_size = capacity;

      let header_ptr = mem as *mut Self;
      let buffer_data_ptr = mem.add(std::mem::size_of::<Self>()) as *mut UnsafeCell<MaybeUninit<T>>; 

      for i in 0..buffer_size {
         ptr::write(buffer_data_ptr.add(i), UnsafeCell::new(MaybeUninit::uninit()));
      }
      
      let buffer_slice = std::slice::from_raw_parts_mut(buffer_data_ptr, buffer_size);
      let boxed_buffer = Box::from_raw(buffer_slice as *mut [_]);

      ptr::write(header_ptr, Self {
         buffer: boxed_buffer,
         capacity: buffer_size,
         wc: AtomicUsize::new(0),
         rc: AtomicUsize::new(0),
         pclaim: AtomicBool::new(false),
         cclaim: AtomicBool::new(false),
      });

      &mut *header_ptr
   }

   pub const fn shared_size(capacity: usize) -> usize {
      std::mem::size_of::<Self>() + capacity * std::mem::size_of::<UnsafeCell<MaybeUninit<T>>>()
   }

   #[inline]
   fn is_full(&self, current_wc: usize, current_rc: usize) -> bool {
      // capacity is asserted to be > 1, so self.capacity will not be 0 or 1 here.
      (current_wc + 1) % self.capacity == current_rc
   }

   #[inline]
   fn is_empty(&self, current_wc: usize, current_rc: usize) -> bool {
      current_wc == current_rc
   }
}

impl<T: Send + 'static> SpscQueue<T> for DehnaviQueue<T> {
   type PushError = PushError<T>; 
   type PopError = PopError;

   fn push(&self, item: T) -> Result<(), Self::PushError> {
      let mut current_wc = self.wc.load(Ordering::Acquire);
      
      // Since capacity > 1, no special k=1 logic is needed here.
      while self.is_full(current_wc, self.rc.load(Ordering::Acquire)) {
         if !self.cclaim.load(Ordering::Acquire) { 
               self.pclaim.store(true, Ordering::Release); 
               if !self.cclaim.load(Ordering::Acquire) { 
                  let new_rc = (self.rc.load(Ordering::Acquire) + 1) % self.capacity;
                  self.rc.store(new_rc, Ordering::Release); 
                  self.pclaim.store(false, Ordering::Release); 
                  break; 
               } else {
                  self.pclaim.store(false, Ordering::Release);
                  std::hint::spin_loop(); 
               }
         } else {
               std::hint::spin_loop(); 
         }
         current_wc = self.wc.load(Ordering::Acquire);
      }
      
      current_wc = self.wc.load(Ordering::Acquire); 

      unsafe {
         ptr::write((*self.buffer.get_unchecked(current_wc)).get(), MaybeUninit::new(item));
      }

      self.wc.store((current_wc + 1) % self.capacity, Ordering::Release);
      Ok(())
   }

   fn pop(&self) -> Result<T, Self::PopError> {
      let mut current_rc = self.rc.load(Ordering::Acquire);
      let current_wc = self.wc.load(Ordering::Acquire);

      if self.is_empty(current_wc, current_rc) {
         return Err(PopError);
      }

      // Since capacity > 1, no special k=1 logic is needed here.
      self.cclaim.store(true, Ordering::Release);
      while self.pclaim.load(Ordering::Acquire) { 
         std::hint::spin_loop();
      }
      
      current_rc = self.rc.load(Ordering::Acquire); 
      let new_current_wc = self.wc.load(Ordering::Acquire); 
      if self.is_empty(new_current_wc, current_rc) {
         self.cclaim.store(false, Ordering::Release);
         return Err(PopError);
      }

      let maybe_uninit_item = unsafe {
         ptr::read((*self.buffer.get_unchecked(current_rc)).get())
      };

      self.rc.store((current_rc + 1) % self.capacity, Ordering::Release);
      self.cclaim.store(false, Ordering::Release);

      unsafe { Ok(maybe_uninit_item.assume_init()) }
   }

   fn available(&self) -> bool {
      let wc = self.wc.load(Ordering::Relaxed); 
      let rc = self.rc.load(Ordering::Relaxed);
      // Since capacity > 1, no special k=1 logic is needed here.
      !self.is_full(wc, rc)
   }

   fn empty(&self) -> bool {
      let wc = self.wc.load(Ordering::Relaxed); 
      let rc = self.rc.load(Ordering::Relaxed);
      self.is_empty(wc, rc)
   }
}

impl<T: Send + 'static> Drop for DehnaviQueue<T> { 
   fn drop(&mut self) {
      if !std::mem::needs_drop::<T>() || self.buffer.is_empty() {
         return;
      }
      
      let mut current_rc_val = *self.rc.get_mut(); 
      let current_wc_val = *self.wc.get_mut();

      while current_rc_val != current_wc_val {
         unsafe {
               let mu_item_ptr = (*self.buffer.get_unchecked_mut(current_rc_val)).get();
               MaybeUninit::assume_init_drop( &mut *mu_item_ptr );
         }
         current_rc_val = (current_rc_val + 1) % self.capacity;
      }
   }
}

unsafe impl<T: Send + 'static> Send for DehnaviQueue<T> {} 
unsafe impl<T: Send + 'static> Sync for DehnaviQueue<T> {}
