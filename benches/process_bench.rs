#![allow(clippy::cast_possible_truncation)] 

use criterion::{criterion_group, criterion_main, Criterion}; 
use std::time::Duration; 
use std::ptr; 
use nix::{ 
   libc, 
   sys::wait::waitpid, 
   unistd::{fork, ForkResult}, 
}; 

// Import all necessary queue types and the main SpscQueue trait 
use spsc_queues::{ 
   BQueue, LamportQueue, MultiPushQueue, UnboundedQueue, SpscQueue, DynListQueue, DehnaviQueue,
   IffqQueue, BiffqQueue, FfqQueue
}; 
use std::sync::atomic::{AtomicU32, Ordering};

const PERFORMANCE_TEST: bool = false; // Set to false for local testing

const RING_CAP: usize = 1024; 
const ITERS:     usize = 1_000_000; 

// Helper trait for benchmarking if underlying SpscQueue error types differ 
// from what fork_and_run expects (Result<(), ()> and Result<T, ()>). 
trait BenchSpscQueue<T: Send>: Send + Sync + 'static { 
   fn bench_push(&self, item: T) -> Result<(), ()>; 
   fn bench_pop(&self) -> Result<T, ()>; 
} 

// mmap / munmap helpers 
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

// Implement BenchSpscQueue for DehnaviQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for DehnaviQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item).map_err(|_| ()) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self).map_err(|_| ())
   } 
} 

// Implement BenchSpscQueue for LamportQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for LamportQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self)
   } 
} 

// Implement BenchSpscQueue for BQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for BQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item).map_err(|_| ()) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self) 
   } 
} 

// Implement BenchSpscQueue for MultiPushQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for MultiPushQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self).map_err(|_| ()) 
   } 
} 

// Implement BenchSpscQueue for UnboundedQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for UnboundedQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self)
   } 
} 

// Implement BenchSpscQueue for DynListQueue 
impl<T: Send + 'static> BenchSpscQueue<T> for DynListQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { 
      SpscQueue::push(self, item) 
   } 
   fn bench_pop(&self) -> Result<T, ()> { 
      SpscQueue::pop(self)
   } 
} 

// Implement BenchSpscQueue for IffqQueue
impl<T: Send + 'static> BenchSpscQueue<T> for IffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ()) 
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

// Implement BenchSpscQueue for BiffqQueue
impl<T: Send + 'static> BenchSpscQueue<T> for BiffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ()) 
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

// Implement BenchSpscQueue for FfqQueue
impl<T: Send + 'static> BenchSpscQueue<T> for FfqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ()) 
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}


// Dehnavi benchmark function 
fn bench_dehnavi(c: &mut Criterion) { 
   c.bench_function("Dehnavi (process)", |b| { 
      b.iter(|| { 
         let current_ring_cap = if RING_CAP <= 1 { 2 } else { RING_CAP };  
         let bytes = DehnaviQueue::<usize>::shared_size(current_ring_cap); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q = unsafe { DehnaviQueue::init_in_shared(shm_ptr, current_ring_cap) }; 
         
         let dur = fork_and_run(q); 
         unsafe { 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

// Lamport benchmark function 
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

// B-Queue benchmark function 
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

// Multi-Push (mspsc) benchmark function 
fn bench_mp(c: &mut Criterion) { 
   c.bench_function("Multi-Push (process)", |b| { 
      b.iter(|| { 
         let bytes   = MultiPushQueue::<usize>::shared_size(RING_CAP); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q       = unsafe { MultiPushQueue::init_in_shared(shm_ptr, RING_CAP) }; 
         let q_ptr: *mut MultiPushQueue<usize> = q; 

         let dur = fork_and_run(q); 

         unsafe { 
            ptr::drop_in_place(q_ptr); 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

// dSPSC benchmark function 
fn bench_dspsc(c: &mut Criterion) { 
   c.bench_function("dSPSC (process - shared)", |b| { 
      b.iter(|| { 
         let bytes = DynListQueue::<usize>::shared_size(); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q = unsafe { DynListQueue::init_in_shared(shm_ptr) }; 

         let dur = fork_and_run(q); 

         unsafe { 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

// Unbounded SPSC benchmark function 
fn bench_unbounded(c: &mut Criterion) { 
   c.bench_function("Unbounded (process)", |b| { 
      b.iter(|| { 
         let size = UnboundedQueue::<usize>::shared_size(); 
         let shm_ptr = unsafe { map_shared(size) }; 
         let q = unsafe { UnboundedQueue::init_in_shared(shm_ptr) }; 
         let dur = fork_and_run(q); 
         unsafe { unmap_shared(shm_ptr, size); } 
         dur 
      }) 
   }); 
} 

// IFFQ benchmark function
fn bench_iffq(c: &mut Criterion) {
    c.bench_function("Iffq (process - shared)", |b| { 
        b.iter(|| {
            assert!(RING_CAP.is_power_of_two());
            // H_PARTITION_SIZE is 32 in iffq.rs
            assert_eq!(RING_CAP % 32, 0, "RING_CAP must be a multiple of IFFQ H_PARTITION_SIZE (32)");
            assert!(RING_CAP >= 2 * 32, "RING_CAP must be >= 2 * IFFQ H_PARTITION_SIZE (64)");

            let bytes = IffqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { IffqQueue::init_in_shared(shm_ptr, RING_CAP) };
            
            let dur = fork_and_run(q);
            
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

// BIFFQ benchmark function
fn bench_biffq(c: &mut Criterion) {
    c.bench_function("Biffq (process - shared)", |b| { 
        b.iter(|| {
            assert!(RING_CAP.is_power_of_two());
            // H_PARTITION_SIZE is 32 in biffq.rs
            assert_eq!(RING_CAP % 32, 0, "RING_CAP must be a multiple of BIFFQ H_PARTITION_SIZE (32)");
            assert!(RING_CAP >= 2 * 32, "RING_CAP must be >= 2 * BIFFQ H_PARTITION_SIZE (64)");

            let bytes = BiffqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BiffqQueue::init_in_shared(shm_ptr, RING_CAP) };
            
            let dur = fork_and_run(q);
            
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

// FFQ benchmark function
fn bench_ffq(c: &mut Criterion) {
    c.bench_function("Ffq (process - shared)", |b| { // Naming convention for plot
        b.iter(|| {
            // FFQ does not have H_PARTITION_SIZE constraints, only power of two for capacity.
            assert!(RING_CAP.is_power_of_two() && RING_CAP > 0);

            let bytes = FfqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { FfqQueue::init_in_shared(shm_ptr, RING_CAP) };
            
            let dur = fork_and_run(q);
            
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}


// Generic fork-and-run helper 
fn fork_and_run<Q>(q: &'static Q) -> std::time::Duration 
where 
   Q: BenchSpscQueue<usize> + Sync, 
{ 
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
      panic!("mmap for sync_shm failed: {}", std::io::Error::last_os_error()); 
   } 
   
   let sync_atomic_flag = unsafe { &*(sync_shm as *const AtomicU32) }; 
   sync_atomic_flag.store(0, Ordering::Relaxed);  
   
   match unsafe { fork() }.expect("fork failed") { 
      ForkResult::Child => { 
         sync_atomic_flag.store(1, Ordering::Release); 
         while sync_atomic_flag.load(Ordering::Acquire) < 2 { 
            std::hint::spin_loop(); 
         } 
         
         for i in 0..ITERS { 
            while q.bench_push(i).is_err() {  
               std::hint::spin_loop(); 
            } 
         } 
         
         if let Some(biffq_queue) = (q as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>() {
            for _attempt in 0..1000 { 
                if biffq_queue.flush_producer_buffer().is_ok() {
                    // Ideally, check if local buffer is empty.
                    // For now, assume flush makes it empty or significantly reduces it.
                    // A more robust check would be `biffq_queue.prod.local_count.load(Ordering::Relaxed) == 0`
                    // but `prod` and `local_count` are private.
                    // If the queue provides a method like `is_local_buffer_empty()`, that would be better.
                    // For now, we assume a successful flush is good enough for the benchmark.
                    break; 
                }
                std::hint::spin_loop();
            }
         }
         
         sync_atomic_flag.store(3, Ordering::Release); 
         unsafe { libc::_exit(0) }; 
      } 
      ForkResult::Parent { child } => { 
         while sync_atomic_flag.load(Ordering::Acquire) < 1 { 
            std::hint::spin_loop(); 
         } 
         
         sync_atomic_flag.store(2, Ordering::Release); 
         
         let start_time = std::time::Instant::now(); 
         let mut consumed_count = 0; 
         
         while consumed_count < ITERS { 
            if sync_atomic_flag.load(Ordering::Acquire) == 3 { 
               if q.bench_pop().is_err() { 
                  break;  
               } else { 
                  consumed_count += 1; 
                  if consumed_count == ITERS { break; } 
               } 
            } 
            
            if let Ok(_item) = q.bench_pop() { 
               consumed_count += 1; 
            } else { 
               std::hint::spin_loop();  
            } 
         } 
         
         let duration = start_time.elapsed(); 
         
         while sync_atomic_flag.load(Ordering::Acquire) != 3 { 
            std::hint::spin_loop();  
         } 
         let _ = waitpid(child, None).expect("waitpid failed"); 
         
         unsafe {  
            libc::munmap(sync_shm as *mut libc::c_void, page_size); 
         } 
         
         if ((consumed_count < ITERS) && (PERFORMANCE_TEST == false)) { 
            eprintln!( 
               "Warning: Parent consumed {}/{} items. Queue type: {}",  
               consumed_count,  
               ITERS, 
               std::any::type_name::<Q>() 
            ); 
         } 
         duration 
      } 
   } 
} 

// Criterion setup 
fn custom_criterion() -> Criterion { 
   Criterion::default() 
      .warm_up_time(Duration::from_secs(5)) 
      .measurement_time(Duration::from_secs(20)) 
      .sample_size(150)
} 

criterion_group!{ 
   name = benches; 
   config = custom_criterion(); 
   targets = 
      //bench_lamport, 
      // bench_bqueue, 
      //bench_mp, 
      // bench_unbounded, 
      bench_dspsc, 
      // bench_dehnavi,
      // bench_iffq,  
      // bench_biffq,
      // bench_ffq
} 
criterion_main!(benches);

