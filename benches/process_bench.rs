// Process-based SPSC benchmark for my queues
// This benchmark uses `fork` to create a child process that pushes
// items into the queue, while the parent process pops items from it
// TODO: add a benchmark for all spsc queues

#![allow(clippy::cast_possible_truncation)]

use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use nix::{
   libc,                       // raw mmap/munmap constants
   sys::wait::waitpid,
   unistd::{fork, ForkResult},
};

use spsc_queues::{LamportQueue, SpscQueue};

const RING_CAP: usize = 1024;         // power-of-two
const ITERS: usize = 1_000_000;

// create a shared memory block of `bytes` size for the used processes
unsafe fn map_shared(bytes: usize) -> *mut u8 {
   let ptr = libc::mmap(
      std::ptr::null_mut(),
      bytes,
      libc::PROT_READ | libc::PROT_WRITE,
      libc::MAP_SHARED | libc::MAP_ANONYMOUS,
      -1,
      0,
   );
   if ptr == libc::MAP_FAILED {
      panic!("mmap failed: {}", std::io::Error::last_os_error());
   }
   ptr.cast()
}

// unmap the shared memory block
unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
   let ret = libc::munmap(ptr.cast(), len);
   assert_eq!(ret, 0, "munmap failed: {}", std::io::Error::last_os_error());
}

// actual benchmark function

fn lamport_process_bench(c: &mut Criterion) {
   c.bench_function("lamport/forked push+pop", |b| {
      b.iter(|| {
         // allocate & build queue in shared memory
         let bytes   = LamportQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };

         let q = unsafe { LamportQueue::init_in_shared(shm_ptr, RING_CAP) };

         // fork – child pushes, parent pops
         // since we only want to test the performance of the queues, we can use fork to get a second process that automatically shares the same memory
         // child parent relationship irrelevant for the benchmark, for the kernel they are just two processes that share the same memory,
         // which would be the case for any other process that uses the queue.
         // after the fork, each process (reader and writer) will enter its own loop at the same time. So we mimic the behavior of two different processes that run side by side
         match unsafe { fork() }.expect("fork failed") {
            // producer process
            ForkResult::Child => {
               for i in 0..ITERS {
                  while q.push(i).is_err() {
                        std::hint::spin_loop();
                  }
               }
               unsafe { libc::_exit(0) };
            }
            // consumer process
            ForkResult::Parent { child } => {
               // start the timer (since the parent process starts immediately after we start the child process both processes will run at the same time). 
               // So we actually measure the time it takes to push and pop the items from the queue.
               let start = Instant::now(); 
               let mut got = 0usize;

               while got < ITERS {
                  if q.pop().is_ok() {
                        got += 1;
                  } else {
                        std::hint::spin_loop();
                  }
               }
               let elapsed = start.elapsed();

               // reap child and unmap shared memory
               let _ = waitpid(child, None);
               unsafe { unmap_shared(shm_ptr, bytes) };

               elapsed // Criterion uses this Duration for its statistics
            }
         }
      });
   });
}

criterion_group!(benches, lamport_process_bench);
criterion_main!(benches);
