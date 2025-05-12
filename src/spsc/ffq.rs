use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};

// An empty slot is represented by `None`; a full one by `Some(T)`.
type Slot<T> = Option<T>;

// The queue object itself sits in *shared* memory, but the two index
// variables are each written **only** by their owner, so they can be plain
// `usize` wrapped in `UnsafeCell`.
#[repr(C, align(64))]
pub struct FfqQueue<T: Send + 'static> {
   // Producer-local write cursor.
   head: UnsafeCell<usize>,
   // Consumer-local read cursor.
   tail: UnsafeCell<usize>,

   capacity: usize,
   mask: usize,
   buffer: *mut UnsafeCell<MaybeUninit<Slot<T>>>,
   owns_buffer: bool,
}

unsafe impl<T: Send> Send for FfqQueue<T> {}
unsafe impl<T: Send> Sync for FfqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct FfqPushError<T>(pub T);
#[derive(Debug, PartialEq, Eq)]
pub struct FfqPopError;

impl<T: Send + 'static> FfqQueue<T> {
   // Build a new queue in process-local memory.
   // The **capacity must be a power of two**.
   pub fn with_capacity(capacity: usize) -> Self {
      assert!(capacity.is_power_of_two() && capacity > 0);

      // Initialise buffer with `None` in every slot.
      let mut buf: Vec<UnsafeCell<MaybeUninit<Slot<T>>>> =
         (0..capacity).map(|_| UnsafeCell::new(MaybeUninit::new(None))).collect();
      let ptr = buf.as_mut_ptr();
      core::mem::forget(buf); // ownership transferred to struct

      Self {
         head: UnsafeCell::new(0),
         tail: UnsafeCell::new(0),
         capacity,
         mask: capacity - 1,
         buffer: ptr,
         owns_buffer: true,
      }
   }

   // Bytes required to place this queue in shared memory.
   pub fn shared_size(capacity: usize) -> usize {
      assert!(capacity.is_power_of_two() && capacity > 0);
      let self_layout = core::alloc::Layout::new::<Self>();
      let buf_layout =
         core::alloc::Layout::array::<UnsafeCell<MaybeUninit<Slot<T>>>>(capacity).unwrap();
      self_layout.extend(buf_layout).unwrap().0.size()
   }

   // Construct in user-provided shared memory region (e.g. `mmap`).
   // The caller must guarantee the memory lives for `'static`.
   pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
      assert!(capacity.is_power_of_two() && capacity > 0);

      let queue_ptr = mem as *mut Self;
      let buf_ptr = mem.add(core::mem::size_of::<Self>())
         as *mut UnsafeCell<MaybeUninit<Slot<T>>>;

      for i in 0..capacity {
         ptr::write(buf_ptr.add(i), UnsafeCell::new(MaybeUninit::new(None)));
      }

      ptr::write(
         queue_ptr,
         Self {
               head: UnsafeCell::new(0),
               tail: UnsafeCell::new(0),
               capacity,
               mask: capacity - 1,
               buffer: buf_ptr,
               owns_buffer: false,
         },
      );
      &mut *queue_ptr
   }

   #[inline]
   fn slot_ptr(&self, index: usize) -> *mut MaybeUninit<Slot<T>> {
      unsafe { (*self.buffer.add(index & self.mask)).get() }
   }
}

impl<T: Send + 'static> SpscQueue<T> for FfqQueue<T> {
   type PushError = FfqPushError<T>;
   type PopError = FfqPopError;

   #[inline]
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      let head = unsafe { *self.head.get() };
      let slot = self.slot_ptr(head);

      // SAFETY: only the producer writes to this slot while it observes `None`.
      if unsafe { (*slot).assume_init_ref().is_some() } {
         return Err(FfqPushError(item)); // queue full
      }

      // Optional: for very weak memory models (POWER, Itanium) uncomment
      // the next line to impose Release ordering for the payload store.
      // core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::Release);

      unsafe { ptr::write(slot, MaybeUninit::new(Some(item))); }
      unsafe { *self.head.get() = head.wrapping_add(1); }
      Ok(())
   }

   #[inline]
   fn pop(&self) -> Result<T, Self::PopError> {
      let tail = unsafe { *self.tail.get() };
      let slot = self.slot_ptr(tail);

      // Optional Acquire fence counterpart (see comment in `push`).
      // core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::Acquire);

      let maybe = unsafe { ptr::read(slot).assume_init() };
      match maybe {
         Some(val) => {
               // Mark slot empty again.
               unsafe { ptr::write(slot, MaybeUninit::new(None)); }
               unsafe { *self.tail.get() = tail.wrapping_add(1); }
               Ok(val)
         }
         None => {
               // Put the `None` back we just removed via `read`.
               unsafe { ptr::write(slot, MaybeUninit::new(None)); }
               Err(FfqPopError)
         }
      }
   }

   #[inline]
   fn available(&self) -> bool {
      // Called by producer – look only at producer cursor.
      let head = unsafe { *self.head.get() };
      unsafe { (*self.slot_ptr(head)).assume_init_ref().is_none() }
   }

   #[inline]
   fn empty(&self) -> bool {
      // Called by consumer – look only at consumer cursor.
      let tail = unsafe { *self.tail.get() };
      unsafe { (*self.slot_ptr(tail)).assume_init_ref().is_none() }
   }
}

impl<T: Send + 'static> Drop for FfqQueue<T> {
   fn drop(&mut self) {
      if self.owns_buffer {
         // Drop any still-present items.
         if core::mem::needs_drop::<T>() {
               for i in 0..self.capacity {
                  let slot = self.slot_ptr(i);
                  unsafe {
                     let opt = ptr::read(slot).assume_init();
                     if let Some(v) = opt { drop(v); }
                  }
               }
         }
         unsafe {
               // Reconstitute the Vec and let it deallocate.
               let slice = core::slice::from_raw_parts_mut(
                  self.buffer as *mut MaybeUninit<Slot<T>>,
                  self.capacity,
               );
               drop(Box::from_raw(slice));
         }
      }
   }
}

impl<T: fmt::Debug + Send + 'static> fmt::Debug for FfqQueue<T> {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("FfqQueue")
         .field("capacity", &self.capacity)
         .field("head", unsafe { &*self.head.get() })
         .field("tail", unsafe { &*self.tail.get() })
         .field("owns_buffer", &self.owns_buffer)
         .finish()
   }
}