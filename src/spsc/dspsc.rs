// spsc_queues/src/spsc/dspsc.rs
use crate::SpscQueue;
use std::ptr::{self, null_mut};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::mem;

// Exactly like the paper tates is around 6 times slower then uspsc if producer and consumer on different core.
const PREALLOCATED_NODES: usize = 1_000_000; // Adjusted to match typical ITERS

struct NodeShared<T: Send + 'static> {
    val: Option<T>,
    next: AtomicPtr<NodeShared<T>>,
}

pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<NodeShared<T>>, 
    tail: AtomicPtr<NodeShared<T>>, 

    nodes_pool_ptr: *mut NodeShared<T>, 
    recycled_nodes_ptr: *mut AtomicPtr<NodeShared<T>>,

    next_free_node_idx: AtomicUsize, 
    recycled_count: AtomicUsize,    
    pool_capacity: usize,           
}

unsafe impl<T: Send + 'static> Send for DynListQueue<T> {}
unsafe impl<T: Send + 'static> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn shared_size() -> usize {
        let main_struct_size = mem::size_of::<Self>(); 
        let pool_nodes_data_size = (PREALLOCATED_NODES + 1) * mem::size_of::<NodeShared<T>>();
        let recycled_array_data_size = PREALLOCATED_NODES * mem::size_of::<AtomicPtr<NodeShared<T>>>();
        const CACHE_LINE_SIZE: usize = 64; 
        let total = main_struct_size + pool_nodes_data_size + recycled_array_data_size;
        (total + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1) 
    }

    pub fn new(_cache_sz: usize) -> Self {
        let dummy_node_ptr = Box::into_raw(Box::new(NodeShared {
            val: None,
            next: AtomicPtr::new(null_mut()),
        }));

        let mut nodes_vec: Vec<NodeShared<T>> = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            nodes_vec.push(NodeShared { val: None, next: AtomicPtr::new(null_mut()) });
        }
        let nodes_pool_ptr = Box::into_raw(nodes_vec.into_boxed_slice()) as *mut NodeShared<T>;

        let mut recycled_vec: Vec<AtomicPtr<NodeShared<T>>> = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            recycled_vec.push(AtomicPtr::new(null_mut()));
        }
        let recycled_nodes_ptr = Box::into_raw(recycled_vec.into_boxed_slice()) as *mut AtomicPtr<NodeShared<T>>;

        Self {
            head: AtomicPtr::new(dummy_node_ptr),
            tail: AtomicPtr::new(dummy_node_ptr),
            nodes_pool_ptr,
            recycled_nodes_ptr,
            next_free_node_idx: AtomicUsize::new(0),
            recycled_count: AtomicUsize::new(0),
            pool_capacity: PREALLOCATED_NODES,
        }
    }

    pub unsafe fn init_in_shared(mem: *mut u8) -> &'static mut Self {
        let queue_instance_ptr = mem as *mut Self;
        let mut current_offset = mem::size_of::<Self>(); 
        
        let alignment_node: usize = mem::align_of::<NodeShared<T>>();
        let alignment_atomicptr: usize = mem::align_of::<AtomicPtr<NodeShared<T>>>();

        current_offset = (current_offset + alignment_node - 1) & !(alignment_node - 1);
        let nodes_pool_and_dummy_start_ptr = mem.add(current_offset) as *mut NodeShared<T>;
        let dummy_node_ptr = nodes_pool_and_dummy_start_ptr; 
        let actual_nodes_pool_ptr = nodes_pool_and_dummy_start_ptr.add(1); 
        current_offset += (PREALLOCATED_NODES + 1) * mem::size_of::<NodeShared<T>>();

        current_offset = (current_offset + alignment_atomicptr - 1) & !(alignment_atomicptr - 1);
        let recycled_nodes_array_start_ptr = mem.add(current_offset) as *mut AtomicPtr<NodeShared<T>>;

        ptr::write(dummy_node_ptr, NodeShared { val: None, next: AtomicPtr::new(null_mut()) });
        for i in 0..PREALLOCATED_NODES {
            ptr::write(actual_nodes_pool_ptr.add(i), NodeShared { val: None, next: AtomicPtr::new(null_mut()) });
        }
        for i in 0..PREALLOCATED_NODES {
            ptr::write(recycled_nodes_array_start_ptr.add(i), AtomicPtr::new(null_mut()));
        }

        ptr::write(queue_instance_ptr, DynListQueue {
            head: AtomicPtr::new(dummy_node_ptr),
            tail: AtomicPtr::new(dummy_node_ptr),
            nodes_pool_ptr: actual_nodes_pool_ptr, 
            recycled_nodes_ptr: recycled_nodes_array_start_ptr, 
            next_free_node_idx: AtomicUsize::new(0), 
            recycled_count: AtomicUsize::new(0),    
            pool_capacity: PREALLOCATED_NODES,
        });
        
        &mut *queue_instance_ptr
    }
    
    fn alloc_node(&self, v: T) -> *mut NodeShared<T> {
        // Try to get from recycled LIFO stack first
        // Using strong CAS (compare_exchange) for potentially more robustness in contention
        let mut current_recycled_count = self.recycled_count.load(Ordering::Acquire);
        while current_recycled_count > 0 {
            match self.recycled_count.compare_exchange(
                current_recycled_count,
                current_recycled_count - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => { // Successfully decremented count
                    let node_ptr = unsafe { (*self.recycled_nodes_ptr.add(current_recycled_count - 1)).swap(null_mut(), Ordering::AcqRel) };
                    if !node_ptr.is_null() {
                        unsafe {
                            (*node_ptr).val = Some(v);
                            (*node_ptr).next.store(null_mut(), Ordering::Relaxed);
                        }
                        return node_ptr;
                    } else {
                        // This is an inconsistent state: count was >0, CAS succeeded, but got null.
                        // This implies a bug or severe race. Increment count back and try again or fail.
                        self.recycled_count.fetch_add(1, Ordering::Release); // Try to correct count
                        // Consider this a failure for this attempt from recycled list.
                        break; // Break from recycled attempts, try main pool
                    }
                }
                Err(observed_count) => { // CAS failed, retry with the new observed count
                    current_recycled_count = observed_count;
                    // continue; // Implicitly continues loop
                }
            }
            std::hint::spin_loop(); // Yield if CAS fails to avoid tight spin
        }

        // Try to get from initial pre-allocated pool
        let mut current_free_idx = self.next_free_node_idx.load(Ordering::Acquire);
        while current_free_idx < self.pool_capacity {
            match self.next_free_node_idx.compare_exchange(
                current_free_idx,
                current_free_idx + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => { // Successfully claimed an index
                    let node_ptr = unsafe { self.nodes_pool_ptr.add(current_free_idx) };
                    unsafe {
                        (*node_ptr).val = Some(v);
                        (*node_ptr).next.store(null_mut(), Ordering::Relaxed); 
                    }
                    return node_ptr;
                }
                Err(observed_idx) => { // CAS failed, retry
                    current_free_idx = observed_idx;
                    // continue;
                }
            }
            std::hint::spin_loop();
        }
        
        null_mut() // Both recycled and initial pool exhausted
    }
    
    fn recycle_node(&self, node_ptr: *mut NodeShared<T>) {
        if node_ptr.is_null() { return; }

        unsafe {
            if let Some(val_to_drop) = (*node_ptr).val.take() {
                drop(val_to_drop); 
            }
            (*node_ptr).next.store(null_mut(), Ordering::Relaxed); 
        }

        let mut current_recycled_count = self.recycled_count.load(Ordering::Acquire);
        loop { // Retry loop for CAS
            if current_recycled_count < self.pool_capacity { 
                 match self.recycled_count.compare_exchange( 
                    current_recycled_count,
                    current_recycled_count + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed
                ) {
                    Ok(_) => {
                        unsafe { (*self.recycled_nodes_ptr.add(current_recycled_count)).store(node_ptr, Ordering::Release); }
                        return;
                    }
                    Err(observed_count) => {
                        current_recycled_count = observed_count;
                        // continue;
                    }
                 }
            } else {
                // This means the recycled_nodes_ptr array is full.
                // All PREALLOCATED_NODES are currently considered "recycled" / free.
                // alloc_node should be able to pick one up.
                // If recycle_node is called when the cache is already at max capacity,
                // it means we are trying to recycle more nodes than exist in the pool,
                // or alloc_node is not consuming them. This is an anomaly.
                eprintln!(
                    "DynListQueue: Warning - recycle_node: recycled stack is full ({} of {}). Node not added to cache. This may lead to node loss from cache.", 
                    current_recycled_count, self.pool_capacity
                );
                // In this state, we cannot add the node to the cache.
                // The node remains part of the shared memory but is not in the LIFO.
                // This is effectively a "leak" from the cache perspective.
                return; 
            }
            std::hint::spin_loop();
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = (); 
    type PopError = ();
    
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let new_node_ptr = self.alloc_node(item);
        
        if new_node_ptr.is_null() {
            return Err(()); 
        }
        
        let tail_ptr = self.tail.load(Ordering::Relaxed); 
        unsafe { (*tail_ptr).next.store(new_node_ptr, Ordering::Release); } 
        self.tail.store(new_node_ptr, Ordering::Release); 
        
        Ok(())
    }
    
    fn pop(&self) -> Result<T, Self::PopError> {
        let head_ptr = self.head.load(Ordering::Acquire); 
        let next_node_ptr = unsafe { (*head_ptr).next.load(Ordering::Acquire) }; 
        
        if next_node_ptr.is_null() { 
            return Err(()); 
        }
        
        let value = unsafe { (*next_node_ptr).val.take() }; 
        
        self.head.store(next_node_ptr, Ordering::Release); 
        self.recycle_node(head_ptr);
        
        value.ok_or(()) 
    }
    
    fn available(&self) -> bool { 
        self.recycled_count.load(Ordering::Relaxed) > 0 || self.next_free_node_idx.load(Ordering::Relaxed) < self.pool_capacity
    }
    
    fn empty(&self) -> bool { 
        let head_ptr = self.head.load(Ordering::Relaxed); 
        unsafe { (*head_ptr).next.load(Ordering::Relaxed).is_null() } 
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        if !mem::needs_drop::<T>() {
            return;
        }

        let mut current_node_ptr = self.head.load(Ordering::Relaxed); 
        let mut next_to_process = unsafe { (*current_node_ptr).next.load(Ordering::Relaxed) };

        while !next_to_process.is_null() {
            unsafe {
                if let Some(val) = (*next_to_process).val.take() {
                    drop(val);
                }
                current_node_ptr = next_to_process; 
                next_to_process = (*current_node_ptr).next.load(Ordering::Relaxed);
            }
        }

        let recycled_count_val = *self.recycled_count.get_mut(); 
        for i in 0..recycled_count_val {
            let node_atomic_ptr_in_cache = unsafe { &*self.recycled_nodes_ptr.add(i) };
            let node_ptr = node_atomic_ptr_in_cache.load(Ordering::Relaxed); 
            if !node_ptr.is_null() {
                unsafe {
                    if let Some(val_to_drop) = (*node_ptr).val.take() { 
                        drop(val_to_drop);
                    }
                }
            }
        }
    }
}
