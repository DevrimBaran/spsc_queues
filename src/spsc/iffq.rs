use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

// H_PARTITION_SIZE: As described in the paper (Section 4.2), H is a small multiple of K.
// K is the number of items per cache line. For 8-byte items and 64-byte cache lines, K=8.
// The paper's experiments use H = 4K = 32.
// H must be a power of two if the mask `H-1` is used as in Figure 11's next_clear calculation.
const H_PARTITION_SIZE: usize = 32; 

type Slot<T> = Option<T>;

#[repr(C, align(64))] // Used literal 64 for alignment
struct ProducerFields {
   write: AtomicUsize, 
   limit: AtomicUsize, 
}

#[repr(C, align(64))] // Used literal 64 for alignment
struct ConsumerFields {
   read: AtomicUsize,  
   clear: AtomicUsize, 
}

/// Improved FastForward Queue (IFFQ) from Maffione et al. 2018.
#[repr(C, align(64))] // Used literal 64 for alignment
pub struct IffqQueue<T: Send + 'static> {
   prod: ProducerFields,
   cons: ConsumerFields,
   capacity: usize, 
   mask: usize,     
   h_mask: usize,   
   buffer: *mut UnsafeCell<MaybeUninit<Slot<T>>>, 
   owns_buffer: bool,
}

unsafe impl<T: Send> Send for IffqQueue<T> {}
unsafe impl<T: Send> Sync for IffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPushError<T>(pub T); 

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPopError;

impl<T: Send + 'static> IffqQueue<T> {
   pub fn with_capacity(capacity: usize) -> Self {
      assert!(capacity.is_power_of_two(), "Capacity must be a power of two.");
      assert_eq!(
         capacity % H_PARTITION_SIZE,
         0,
         "Capacity must be a multiple of H_PARTITION_SIZE ({}).", H_PARTITION_SIZE
      );
      assert!(
         capacity >= 2 * H_PARTITION_SIZE,
         "Capacity must be at least 2 * H_PARTITION_SIZE."
      );

      let mut buffer_mem: Vec<UnsafeCell<MaybeUninit<Slot<T>>>> = Vec::with_capacity(capacity);
      for _ in 0..capacity {
         buffer_mem.push(UnsafeCell::new(MaybeUninit::new(None))); 
      }
      let buffer_ptr = buffer_mem.as_mut_ptr();
      mem::forget(buffer_mem); 

      Self {
         prod: ProducerFields {
               write: AtomicUsize::new(H_PARTITION_SIZE), 
               limit: AtomicUsize::new(2 * H_PARTITION_SIZE), 
         },
         cons: ConsumerFields {
               read: AtomicUsize::new(H_PARTITION_SIZE),  
               clear: AtomicUsize::new(0), 
         },
         capacity,
         mask: capacity - 1,
         h_mask: H_PARTITION_SIZE -1, 
         buffer: buffer_ptr,
         owns_buffer: true, 
      }
   }

   pub fn shared_size(capacity: usize) -> usize {
      assert!(capacity > 0 && capacity.is_power_of_two(), "Capacity must be a power of two and > 0.");
      assert_eq!(capacity % H_PARTITION_SIZE, 0, "Capacity must be a multiple of H_PARTITION_SIZE.");
      assert!(capacity >= 2 * H_PARTITION_SIZE, "Capacity must be at least 2 * H_PARTITION_SIZE.");

      let layout = std::alloc::Layout::new::<Self>();
      let buffer_layout = std::alloc::Layout::array::<UnsafeCell<MaybeUninit<Slot<T>>>>(capacity).unwrap();
      layout.extend(buffer_layout).unwrap().0.size()
   }

   pub unsafe fn init_in_shared(mem_ptr: *mut u8, capacity: usize) -> &'static mut Self {
      assert!(capacity.is_power_of_two(), "Capacity must be a power of two.");
      assert_eq!(capacity % H_PARTITION_SIZE, 0, "Capacity must be a multiple of H_PARTITION_SIZE.");
      assert!(capacity >= 2 * H_PARTITION_SIZE, "Capacity must be at least 2 * H_PARTITION_SIZE.");
      
      let queue_ptr = mem_ptr as *mut Self;
      let buffer_data_ptr = mem_ptr.add(std::mem::size_of::<Self>()) as *mut UnsafeCell<MaybeUninit<Slot<T>>>;

      for i in 0..capacity {
         ptr::write(buffer_data_ptr.add(i), UnsafeCell::new(MaybeUninit::new(None)));
      }

      ptr::write(
         queue_ptr,
         Self {
               prod: ProducerFields {
                  write: AtomicUsize::new(H_PARTITION_SIZE),
                  limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
               },
               cons: ConsumerFields {
                  read: AtomicUsize::new(H_PARTITION_SIZE),
                  clear: AtomicUsize::new(0),
               },
               capacity,
               mask: capacity - 1,
               h_mask: H_PARTITION_SIZE - 1,
               buffer: buffer_data_ptr,
               owns_buffer: false, 
         },
      );
      &mut *queue_ptr
   }

   #[inline]
   fn get_slot(&self, index: usize) -> &UnsafeCell<MaybeUninit<Slot<T>>> {
      unsafe { &*self.buffer.add(index & self.mask) }
   }
   
   fn enqueue_internal(&self, item: T) -> Result<(), IffqPushError<T>> { 
      let current_write = self.prod.write.load(Ordering::Relaxed);
      let mut current_limit = self.prod.limit.load(Ordering::Acquire);

      if current_write == current_limit {
         let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
         let slot_to_check_idx = next_limit_potential & self.mask; 
         
         let slot_state = unsafe { (*self.get_slot(slot_to_check_idx).get()).assume_init_read() };

         if slot_state.is_some() { 
               return Err(IffqPushError(item)); 
         }
         
         self.prod.limit.store(next_limit_potential, Ordering::Release);
         current_limit = next_limit_potential;

         if current_write == current_limit { 
               return Err(IffqPushError(item)); 
         }
      }

      let slot_ptr = self.get_slot(current_write).get();
      unsafe {
         ptr::write(slot_ptr, MaybeUninit::new(Some(item)));
      }
      self.prod.write.store(current_write.wrapping_add(1), Ordering::Release);
      Ok(())
   }

   fn dequeue_internal(&self) -> Result<T, IffqPopError> {
      let current_read = self.cons.read.load(Ordering::Relaxed);
      let slot_ptr = self.get_slot(current_read).get();
      
      let item_opt = unsafe { (*slot_ptr).assume_init_read() }; 

      if let Some(item) = item_opt {
         self.cons.read.store(current_read.wrapping_add(1), Ordering::Release); 
         
         let current_clear = self.cons.clear.load(Ordering::Relaxed);
         let read_partition_start = current_read & !self.h_mask; 
         let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

         let mut temp_clear = current_clear;
         let mut advanced_clear = false;
         while temp_clear != next_clear_target {
               if temp_clear == self.cons.read.load(Ordering::Acquire) { break; }

               let clear_slot_ptr = self.get_slot(temp_clear).get();
               unsafe {
                  if std::mem::needs_drop::<Slot<T>>() {
                     let mu_slot = ptr::read(clear_slot_ptr); 
                     drop(mu_slot.assume_init());
                  }
                  ptr::write(clear_slot_ptr, MaybeUninit::new(None)); 
               }
               temp_clear = temp_clear.wrapping_add(1);
               advanced_clear = true;
         }
         if advanced_clear {
               self.cons.clear.store(temp_clear, Ordering::Release);
         }
         
         Ok(item)
      } else {
         Err(IffqPopError)
      }
   }
}

impl<T: Send + 'static> SpscQueue<T> for IffqQueue<T> {
   type PushError = IffqPushError<T>;
   type PopError = IffqPopError;

   #[inline]
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      self.enqueue_internal(item)
   }

   #[inline]
   fn pop(&self) -> Result<T, Self::PopError> {
      self.dequeue_internal()
   }

   #[inline]
   fn available(&self) -> bool {
      let write = self.prod.write.load(Ordering::Relaxed);
      let limit = self.prod.limit.load(Ordering::Acquire);
      if write != limit {
         return true;
      }
      let next_limit_potential = limit.wrapping_add(H_PARTITION_SIZE);
      let slot_to_check_idx = next_limit_potential & self.mask;
      unsafe { (*self.get_slot(slot_to_check_idx).get()).assume_init_read().is_none() }
   }

   #[inline]
   fn empty(&self) -> bool {
      let current_read = self.cons.read.load(Ordering::Acquire);
      let slot_state = unsafe { (*self.get_slot(current_read).get()).assume_init_read() };
      slot_state.is_none()
   }
}

impl<T: Send + 'static> Drop for IffqQueue<T> {
   fn drop(&mut self) {
      if self.owns_buffer {
         if std::mem::needs_drop::<T>() {
               let mut current_read = *self.cons.read.get_mut(); 
               let current_write = *self.prod.write.get_mut(); 
               while current_read != current_write {
                  let slot_ptr = self.get_slot(current_read).get();
                  unsafe {
                     let mu_opt_t = ptr::read(slot_ptr); 
                     if let Some(item) = mu_opt_t.assume_init() {
                           drop(item);
                     }
                  }
                  current_read = current_read.wrapping_add(1);
               }
         }
         unsafe {
               let buffer_slice = std::slice::from_raw_parts_mut(self.buffer, self.capacity);
               let _ = Box::from_raw(buffer_slice); 
         }
      }
   }
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for IffqQueue<T> {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("IffqQueue")
         .field("capacity", &self.capacity)
         .field("mask", &self.mask)
         .field("h_mask", &self.h_mask)
         .field("write", &self.prod.write.load(Ordering::Relaxed))
         .field("limit", &self.prod.limit.load(Ordering::Relaxed))
         .field("read", &self.cons.read.load(Ordering::Relaxed))
         .field("clear", &self.cons.clear.load(Ordering::Relaxed))
         .field("owns_buffer", &self.owns_buffer)
         .finish()
   }
}
