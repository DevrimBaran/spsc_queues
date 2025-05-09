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
    BQueue, LamportQueue, MultiPushQueue, UnboundedQueue, SpscQueue, DynListQueue, DehnaviQueue 
 }; 
 // Removed unused AtomicU32 
 use std::sync::atomic::Ordering; 

 const PERFORMANCE_TEST: bool = true; // Set to false for local testing

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
       SpscQueue::push(self, item).map_err(|_| ()) // Convert Dehnavi's PushError<T> 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       SpscQueue::pop(self).map_err(|_| ())   // Convert Dehnavi's PopError 
    } 
 } 

 // Implement BenchSpscQueue for LamportQueue 
 impl<T: Send + 'static> BenchSpscQueue<T> for LamportQueue<T> { 
    fn bench_push(&self, item: T) -> Result<(), ()> { 
       SpscQueue::push(self, item) // Assuming LamportQueue's PushError is () 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       SpscQueue::pop(self)    // Assuming LamportQueue's PopError is () 
    } 
 } 

 // Implement BenchSpscQueue for BQueue 
 impl<T: Send + 'static> BenchSpscQueue<T> for BQueue<T> { 
    fn bench_push(&self, item: T) -> Result<(), ()> { 
       SpscQueue::push(self, item).map_err(|_| ()) // Assuming BQueue's PushError might not be () 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       SpscQueue::pop(self) // Assuming BQueue's PopError is () 
    } 
 } 

 // Implement BenchSpscQueue for MultiPushQueue 
 impl<T: Send + 'static> BenchSpscQueue<T> for MultiPushQueue<T> { 
    fn bench_push(&self, item: T) -> Result<(), ()> { 
        SpscQueue::push(self, item) // Assuming MultiPushQueue's PushError is () 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       // Assuming MultiPushQueue's PopError needs mapping if it's not () 
       SpscQueue::pop(self).map_err(|_| ()) 
    } 
 } 

 // Implement BenchSpscQueue for UnboundedQueue 
 impl<T: Send + 'static> BenchSpscQueue<T> for UnboundedQueue<T> { 
    fn bench_push(&self, item: T) -> Result<(), ()> { 
       SpscQueue::push(self, item) // Assuming UnboundedQueue's PushError is () 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       SpscQueue::pop(self)    // Assuming UnboundedQueue's PopError is () 
    } 
 } 

 // Implement BenchSpscQueue for DynListQueue 
 impl<T: Send + 'static> BenchSpscQueue<T> for DynListQueue<T> { 
    fn bench_push(&self, item: T) -> Result<(), ()> { 
       SpscQueue::push(self, item) // Assuming DynListQueue's PushError is () 
    } 
    fn bench_pop(&self) -> Result<T, ()> { 
       SpscQueue::pop(self)    // Assuming DynListQueue's PopError is () 
    } 
 } 


 // Dehnavi benchmark function 
 fn bench_dehnavi(c: &mut Criterion) { 
    c.bench_function("Dehnavi (process)", |b| { 
       b.iter(|| { 
          let current_ring_cap = if RING_CAP <= 1 { 2 } else { RING_CAP };  
          let bytes = DehnaviQueue::<usize>::shared_size(current_ring_cap); 
          let shm_ptr = unsafe { map_shared(bytes) }; 
          // Initialize the queue in shared memory 
          let q = unsafe { DehnaviQueue::init_in_shared(shm_ptr, current_ring_cap) }; 
           
          let dur = fork_and_run(q); // q is &'static mut DehnaviQueue<usize> 
                                     // fork_and_run expects &'static Q where Q: BenchSpscQueue 
          unsafe { 
             // Elements are usize, no explicit drop needed for elements themselves. 
             // The Drop impl of DehnaviQueue handles its internal Box structure if not from shm. 
             // For shm, the main thing is unmapping. 
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
          let q_ptr: *mut MultiPushQueue<usize> = q; // For explicit drop 

          let dur = fork_and_run(q); 

          unsafe { 
             ptr::drop_in_place(q_ptr); // Explicitly drop if needed for its Drop impl 
             unmap_shared(shm_ptr, bytes); 
          } 
          dur 
       }) 
    }); 
 } 

 // dSPSC benchmark function 
 fn bench_dspsc(c: &mut Criterion) { 
    c.bench_function("dSPSC (process - shared)", |b| { // Renamed for clarity 
       b.iter(|| { 
          let bytes = DynListQueue::<usize>::shared_size(); 
          let shm_ptr = unsafe { map_shared(bytes) }; 
          let q = unsafe { DynListQueue::init_in_shared(shm_ptr) }; 

          let dur = fork_and_run(q); // Assuming fork_and_run uses BenchSpscQueue 

          unsafe { 
             // Values are usize, no explicit drop needed for T. 
             // The Drop impl of DynListQueue should handle internal T if they were complex. 
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


 // Generic fork-and-run helper 
 fn fork_and_run<Q>(q: &'static Q) -> std::time::Duration 
 where 
    Q: BenchSpscQueue<usize> + Sync, // Use the BenchSpscQueue trait 
 { 
    let page_size = 4096; // Standard page size 
    let sync_shm = unsafe {  
       libc::mmap( 
          std::ptr::null_mut(), 
          page_size, 
          libc::PROT_READ | libc::PROT_WRITE, 
          libc::MAP_SHARED | libc::MAP_ANONYMOUS, // Anonymous shared memory 
          -1, // fd for anonymous mapping 
          0,  // offset 
       ) 
    }; 
     
    if sync_shm == libc::MAP_FAILED { 
       panic!("mmap for sync_shm failed: {}", std::io::Error::last_os_error()); 
    } 
     
    // Cast to a pointer to AtomicU32 for inter-process atomic operations 
    let sync_atomic_flag = unsafe { &*(sync_shm as *const std::sync::atomic::AtomicU32) }; 
    // Initialize the atomic flag to 0 from the parent before forking 
    sync_atomic_flag.store(0, Ordering::Relaxed);  
     
    match unsafe { fork() }.expect("fork failed") { 
       ForkResult::Child => { 
          // Child process 
          sync_atomic_flag.store(1, Ordering::Release); // 1: Child is ready 
          while sync_atomic_flag.load(Ordering::Acquire) < 2 { // Wait for parent to signal 2 (start) 
             std::hint::spin_loop(); 
          } 
           
          // Producer code 
          for i in 0..ITERS { 
             while q.bench_push(i).is_err() {  
                std::hint::spin_loop(); // Spin if push fails (e.g., queue full, though bench_push maps err) 
             } 
          } 
           
          sync_atomic_flag.store(3, Ordering::Release); // 3: Child has finished producing 
          unsafe { libc::_exit(0) }; // Exit child process 
       } 
       ForkResult::Parent { child } => { 
          // Parent process 
          while sync_atomic_flag.load(Ordering::Acquire) < 1 { // Wait for child to be ready (signal 1) 
             std::hint::spin_loop(); 
          } 
           
          sync_atomic_flag.store(2, Ordering::Release); // 2: Parent signals child to start 
           
          let start_time = std::time::Instant::now(); 
          let mut consumed_count = 0; 
           
          while consumed_count < ITERS { 
             // Check if child is done and queue is empty 
             if sync_atomic_flag.load(Ordering::Acquire) == 3 { // Child finished 
                if q.bench_pop().is_err() { // Try one last pop 
                   // If child is done and pop fails, assume queue is drained 
                   break;  
                } else { 
                   consumed_count += 1; // Count the item if successfully popped 
                   if consumed_count == ITERS { break; } // Check if all items consumed 
                } 
             } 
              
             if let Ok(_item) = q.bench_pop() { 
                consumed_count += 1; 
             } else { 
                // Yield if pop fails but child might still be producing or items are in-flight 
                std::hint::spin_loop();  
             } 
          } 
           
          let duration = start_time.elapsed(); 
           
          // Ensure child has signaled completion if loop broke for other reasons 
          while sync_atomic_flag.load(Ordering::Acquire) != 3 { 
             std::hint::spin_loop();  
          } 
          let _ = waitpid(child, None).expect("waitpid failed"); // Wait for child process to terminate 
           
          unsafe {  
             libc::munmap(sync_shm as *mut libc::c_void, page_size); 
          } 
           
          if ((consumed_count < ITERS) && (PERFORMANCE_TEST == false)) { 
             // This warning is important for queues that might lose items under contention 
             // or if the benchmark logic has issues. 
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
       .measurement_time(Duration::from_secs(10)) 
       .sample_size(10) // Reduce sample size for faster local testing if needed 
 } 

 criterion_group!{ 
    name = benches; 
    config = custom_criterion(); 
    targets = 
       bench_lamport, 
       bench_bqueue, 
       bench_mp, 
       bench_unbounded, 
       bench_dspsc, 
       bench_dehnavi,
 } 
 criterion_main!(benches);
