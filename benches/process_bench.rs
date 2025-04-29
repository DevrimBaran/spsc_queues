#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion};
use std::time::Duration;
use std::ptr;
use nix::{
   libc,
   sys::wait::waitpid,
   unistd::{fork, ForkResult},
};

use spsc_queues::{
   BQueue, LamportQueue, MultiPushQueue, UnboundedQueue, SpscQueue, DynListQueue
};
use std::sync::atomic::{Ordering, AtomicU32};


const RING_CAP: usize = 1024;
const ITERS:     usize = 1_000_000;

//mmap / munmap helpers

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

unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
   let ret = libc::munmap(ptr.cast(), len);
   assert_eq!(ret, 0, "munmap failed: {}", std::io::Error::last_os_error());
}

// Lamport

fn bench_lamport(c: &mut Criterion) {
   c.bench_function("Lamport (process)", |b| {
      b.iter(|| {
         let bytes   = LamportQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q       = unsafe { LamportQueue::init_in_shared(shm_ptr, RING_CAP) };
         let dur     = fork_and_run(q);
         unsafe { unmap_shared(shm_ptr, bytes) };
         dur
      })
   });
}

// B-Queue
fn bench_bqueue(c: &mut Criterion) {
   c.bench_function("B-Queue (process)", |b| {
      b.iter(|| {
         let bytes   = BQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q = unsafe { BQueue::init_in_shared(shm_ptr, RING_CAP) };
         let dur = fork_and_run(q);
         unsafe { unmap_shared(shm_ptr, bytes) };
         dur
      })
   });
}

// Multi-Push (mspsc)

fn bench_mp(c: &mut Criterion) {
   c.bench_function("Multi-Push (process)", |b| {
       b.iter(|| {
           // 1) mmap & init in shared memory
           let bytes   = MultiPushQueue::<usize>::shared_size(RING_CAP);
           let shm_ptr = unsafe { map_shared(bytes) };
           let q       = unsafe { MultiPushQueue::init_in_shared(shm_ptr, RING_CAP) };

           // grab a raw pointer so we can run Drop in-place later
           let q_ptr: *mut MultiPushQueue<usize> = q;

           // 2) do the fork-and-run measurement
           let dur = fork_and_run(q);

           // 3) explicitly drop the queue in-place, then unmap
           unsafe {
               ptr::drop_in_place(q_ptr);
               unmap_shared(shm_ptr, bytes);
           }

           dur
       })
   });
}

// dspsc

fn bench_dspsc(c: &mut Criterion) {
   // allocate and initialize shared DynListQueue (no shared mapping needed)
   let queue: &'static DynListQueue<usize> = Box::leak(Box::new(DynListQueue::new(32)));

   c.bench_function("dSPSC (process)", move |b|{
       b.iter(|| fork_and_run(queue))
   });
}

// Unbounded spsc

fn bench_unbounded(c: &mut Criterion) {
   // allocate and initialize shared queue
   let size = UnboundedQueue::<usize>::shared_size();
   let shm = unsafe { map_shared(size) };
   let queue = unsafe { UnboundedQueue::init_in_shared(shm) };

   // let criterion drive a single fork-and-run per sample
   c.bench_function("Unbounded (process)", |b| {
       b.iter(|| {
           let dur = fork_and_run(queue);
           // cleanup between runs if you like:
           // unsafe { unmap_shared(shm, size) };
           dur
       })
   });
}


// generic fork-and-run helper

fn fork_and_run<Q>(q: &'static Q) -> std::time::Duration
where
   Q: SpscQueue<usize, PushError = (), PopError = ()> + Sync,
{
   // Simple auxiliary synchronization
   let page_size = 4096;
   let sync_shm = unsafe { 
      libc::mmap(
         std::ptr::null_mut(),
         page_size,
         libc::PROT_READ | libc::PROT_WRITE,
         libc::MAP_SHARED | libc::MAP_ANONYMOUS,
         -1,
         0,
      )
   };
   
   if sync_shm == libc::MAP_FAILED {
      panic!("Failed to create sync shared memory");
   }
   
   let sync_flag = sync_shm as *mut u32;
   unsafe { *sync_flag = 0 };
   
   match unsafe { fork() }.expect("fork failed") {
      ForkResult::Child => {
         // Signal child is ready and wait for parent signal
         unsafe { *sync_flag = 1 };
         while unsafe { *sync_flag } < 2 {
            std::hint::spin_loop();
         }
         
         // Producer code
         for i in 0..ITERS {
            // Keep trying until push succeeds
            while q.push(i).is_err() {
               std::hint::spin_loop();
            }
         }
         
         // Signal completion
         unsafe { *sync_flag = 3 };
         unsafe { libc::_exit(0) };
      }
      ForkResult::Parent { child } => {
         // Wait for child to signal readiness
         while unsafe { *sync_flag } < 1 {
            std::hint::spin_loop();
         }
         
         // Signal ready to start
         unsafe { *sync_flag = 2 };
         
         let start = std::time::Instant::now();
         let mut count = 0;
         
         // Continue until we've read all items or the child signals completion
         while count < ITERS && unsafe { *sync_flag } < 3 {
            if let Ok(_) = q.pop() {
               count += 1;
            } else {
               std::hint::spin_loop();
            }
         }
         
         let dur = start.elapsed();
         let _ = waitpid(child, None);
         
         // Clean up shared memory
         unsafe { 
            libc::munmap(sync_shm as *mut libc::c_void, page_size);
         }
         
         dur
      }
   }
}

fn custom_criterion() -> Criterion {
   Criterion::default()
      .warm_up_time(Duration::from_secs(5))
      .measurement_time(Duration::from_secs(10))
}

criterion_group!{
   name = benches;
   config = custom_criterion();
   targets =
      //bench_lamport,
      //bench_bqueue,
      bench_mp,
      //bench_unbounded,
      //bench_dspsc
}
criterion_main!(benches);