use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32; 
const LOCAL_BATCH_SIZE: usize = 32; 

type Slot<T> = Option<T>;

#[repr(C, align(64))] 
struct ProducerFieldsB<T: Send + 'static> { 
   write: AtomicUsize,
   limit: AtomicUsize,
   local_buffer: UnsafeCell<[MaybeUninit<T>; LOCAL_BATCH_SIZE]>,
   local_count: AtomicUsize, 
}

#[repr(C, align(64))] 
struct ConsumerFieldsB { 
   read: AtomicUsize,
   clear: AtomicUsize,
}

#[repr(C, align(64))] 
pub struct BiffqQueue<T: Send + 'static> {
   prod: ProducerFieldsB<T>, 
   cons: ConsumerFieldsB,    
   capacity: usize,
   mask: usize,
   h_mask: usize,
   buffer: *mut UnsafeCell<MaybeUninit<Slot<T>>>,
   owns_buffer: bool,
}

unsafe impl<T: Send> Send for BiffqQueue<T> {}
unsafe impl<T: Send> Sync for BiffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPopError; 

impl<T: Send + 'static> BiffqQueue<T> {
   pub fn with_capacity(capacity: usize) -> Self {
      assert!(capacity.is_power_of_two(), "Capacity must be a power of two.");
      assert_eq!(capacity % H_PARTITION_SIZE, 0, "Capacity must be a multiple of H_PARTITION_SIZE.");
      assert!(capacity >= 2 * H_PARTITION_SIZE, "Capacity must be at least 2 * H_PARTITION_SIZE.");

      let mut buffer_mem: Vec<UnsafeCell<MaybeUninit<Slot<T>>>> = Vec::with_capacity(capacity);
      for _ in 0..capacity {
         buffer_mem.push(UnsafeCell::new(MaybeUninit::new(None)));
      }
      let buffer_ptr = buffer_mem.as_mut_ptr();
      mem::forget(buffer_mem);

      let local_buf_uninit: [MaybeUninit<T>; LOCAL_BATCH_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
      
      Self {
         prod: ProducerFieldsB {
               write: AtomicUsize::new(H_PARTITION_SIZE),
               limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
               local_buffer: UnsafeCell::new(local_buf_uninit),
               local_count: AtomicUsize::new(0),
         },
         cons: ConsumerFieldsB { 
               read: AtomicUsize::new(H_PARTITION_SIZE),
               clear: AtomicUsize::new(0),
         },
         capacity,
         mask: capacity - 1,
         h_mask: H_PARTITION_SIZE - 1,
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
      
      let local_buf_uninit: [MaybeUninit<T>; LOCAL_BATCH_SIZE] = MaybeUninit::uninit().assume_init();

      ptr::write(
         queue_ptr,
         Self {
               prod: ProducerFieldsB {
                  write: AtomicUsize::new(H_PARTITION_SIZE),
                  limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
                  local_buffer: UnsafeCell::new(local_buf_uninit),
                  local_count: AtomicUsize::new(0),
               },
               cons: ConsumerFieldsB {
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

   fn publish_batch_internal(&self) -> Result<usize, ()> {
      let local_count = self.prod.local_count.load(Ordering::Relaxed);
      if local_count == 0 {
         return Ok(0);
      }

      let local_buf_ptr = self.prod.local_buffer.get();
      let mut current_write = self.prod.write.load(Ordering::Relaxed);
      let mut current_limit = self.prod.limit.load(Ordering::Acquire);
      let mut published_count = 0;

      for i in 0..local_count {
         if current_write == current_limit {
               let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
               let slot_to_check_idx = next_limit_potential & self.mask;
               let slot_state = unsafe { (*self.get_slot(slot_to_check_idx).get()).assume_init_read() };

               if slot_state.is_some() { 
                  self.prod.write.store(current_write, Ordering::Release); 
                  unsafe {
                     let src = (*local_buf_ptr).as_ptr().add(i);
                     let dst = (*local_buf_ptr).as_mut_ptr(); 
                     ptr::copy(src, dst, local_count - i);
                  }
                  self.prod.local_count.store(local_count - i, Ordering::Release);
                  return if published_count > 0 { Ok(published_count) } else { Err(()) };
               }
               self.prod.limit.store(next_limit_potential, Ordering::Release);
               current_limit = next_limit_potential;
         }

         let item_to_write = unsafe { ptr::read(&(*local_buf_ptr)[i]).assume_init() }; 
         let shared_slot_ptr = self.get_slot(current_write).get();
         unsafe {
               ptr::write(shared_slot_ptr, MaybeUninit::new(Some(item_to_write)));
         }
         current_write = current_write.wrapping_add(1);
         published_count += 1;
      }

      self.prod.write.store(current_write, Ordering::Release);
      self.prod.local_count.store(0, Ordering::Release); 
      Ok(published_count)
   }
   
   fn dequeue_internal(&self) -> Result<T, BiffqPopError> {
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
         Err(BiffqPopError)
      }
   }

   pub fn flush_producer_buffer(&self) -> Result<usize, ()> {
      self.publish_batch_internal()
   }
} 

impl<T: Send + 'static> SpscQueue<T> for BiffqQueue<T> {
   type PushError = BiffqPushError<T>;
   type PopError = BiffqPopError;

   #[inline]
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      let current_local_count = self.prod.local_count.load(Ordering::Relaxed);

      if current_local_count < LOCAL_BATCH_SIZE {
         unsafe {
               let local_buf_slot_ptr = (*self.prod.local_buffer.get()).as_mut_ptr().add(current_local_count);
               ptr::write(local_buf_slot_ptr, MaybeUninit::new(item));
         }
         self.prod.local_count.store(current_local_count + 1, Ordering::Release); 
         
         if current_local_count + 1 == LOCAL_BATCH_SIZE {
               let _ = self.publish_batch_internal(); 
         }
         Ok(())
      } else {
         match self.publish_batch_internal() {
               Ok(_published_count) => { 
                  let new_local_count = self.prod.local_count.load(Ordering::Relaxed); 
                  if new_local_count < LOCAL_BATCH_SIZE {
                     unsafe {
                           let local_buf_slot_ptr = (*self.prod.local_buffer.get()).as_mut_ptr().add(new_local_count);
                           ptr::write(local_buf_slot_ptr, MaybeUninit::new(item));
                     }
                     self.prod.local_count.store(new_local_count + 1, Ordering::Release);
                     Ok(())
                  } else {
                     Err(BiffqPushError(item))
                  }
               }
               Err(_) => { 
                  Err(BiffqPushError(item))
               }
         }
      }
   }

   #[inline]
   fn pop(&self) -> Result<T, Self::PopError> {
      self.dequeue_internal()
   }
   
   #[inline]
   fn available(&self) -> bool {
      if self.prod.local_count.load(Ordering::Relaxed) < LOCAL_BATCH_SIZE {
         return true;
      }
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
      let local_empty = self.prod.local_count.load(Ordering::Relaxed) == 0;
      if !local_empty { return false; }

      let current_read = self.cons.read.load(Ordering::Acquire);
      let slot_state = unsafe { (*self.get_slot(current_read).get()).assume_init_read() };
      slot_state.is_none()
   }
} 

impl<T: Send + 'static> Drop for BiffqQueue<T> {
   fn drop(&mut self) {
      if self.owns_buffer || !(*self.prod.local_buffer.get_mut()).as_mut_ptr().is_null() { 
         let local_count_val = *self.prod.local_count.get_mut();
         if local_count_val > 0 {
               let _ = self.publish_batch_internal(); 
         }
      }

      if self.owns_buffer {
         if std::mem::needs_drop::<T>() {
               let local_count = *self.prod.local_count.get_mut(); 
               let local_buf_ptr_mut = (*self.prod.local_buffer.get_mut()).as_mut_ptr();
               for i in 0..local_count {
                  unsafe { 
                     // Corrected: item_mu needs to be mutable to call assume_init_drop(&mut self)
                     let mut item_mu = ptr::read(local_buf_ptr_mut.add(i));
                     item_mu.assume_init_drop(); 
                  }
               }
               *self.prod.local_count.get_mut() = 0;
         }

         if std::mem::needs_drop::<T>() {
               let mut current_read = *self.cons.read.get_mut();
               let current_write = *self.prod.write.get_mut(); 
               while current_read != current_write {
                  let slot_ptr = self.get_slot(current_read).get();
                  unsafe {
                     let mu_opt_t = ptr::read(slot_ptr); 
                     drop(mu_opt_t.assume_init());
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

impl<T: Send + fmt::Debug + 'static> fmt::Debug for BiffqQueue<T> {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("BiffqQueue")
         .field("capacity", &self.capacity)
         .field("local_count", &self.prod.local_count.load(Ordering::Relaxed))
         .field("write", &self.prod.write.load(Ordering::Relaxed))
         .field("limit", &self.prod.limit.load(Ordering::Relaxed))
         .field("read", &self.cons.read.load(Ordering::Relaxed))
         .field("clear", &self.cons.clear.load(Ordering::Relaxed))
         .field("owns_buffer", &self.owns_buffer)
         .finish()
   }
}
