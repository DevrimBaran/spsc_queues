// minimal single-process test for the uspsc queue

use nix::libc;
use spsc_queues::{SpscQueue, UnboundedQueue};

const ELEMENTS: usize = 10;

unsafe fn map_shared(bytes: usize) -> *mut u8 {
   let p = libc::mmap(
      std::ptr::null_mut(),
      bytes,
      libc::PROT_READ | libc::PROT_WRITE,
      libc::MAP_SHARED | libc::MAP_ANONYMOUS,
      -1,
      0,
   );
   assert_ne!(p, libc::MAP_FAILED, "mmap failed");
   p.cast()
}

fn main() {
   /* 1) allocate a tiny mapping ------------------------------------------------ */
   let bytes = UnboundedQueue::<usize>::shared_size();
   let shm   = unsafe { map_shared(bytes) };

   /* 2) place the queue inside ------------------------------------------------- */
   let q: &mut UnboundedQueue<usize> =
      unsafe { UnboundedQueue::init_in_shared(shm) };

   /* 3) simple round-trip ------------------------------------------------------ */
   for i in 0..ELEMENTS {
      assert!(q.push(i).is_ok());
   }
   for i in 0..ELEMENTS {
      assert_eq!(q.pop().unwrap(), i);
   }

   println!("smoke-test passed ✅");
}
