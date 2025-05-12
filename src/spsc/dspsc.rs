use crate::SpscQueue;
use std::{
    alloc::Layout, 
    // mem, // Removed as core::mem and std::alloc::Layout are used
    ptr::{self, null_mut},
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
    thread, 
};

// helpers
#[inline(always)]
const fn null_node<T: Send>() -> *mut Node<T> { null_mut() }

const PREALLOCATED_NODES: usize = 32;

// node
#[repr(C)]
struct Node<T: Send + 'static> {
    val:  Option<T>,
    next: AtomicPtr<Node<T>>,
}

// queue
#[repr(C, align(64))] // Align the whole struct to 64 bytes
pub struct DynListQueue<T: Send + 'static> {
    // Head and Tail are accessed by different processes in SPSC.
    // Consider explicit padding structs if false sharing becomes an issue,
    // but for now, struct-level alignment is applied.
    head: AtomicPtr<Node<T>>, // consumer
    tail: AtomicPtr<Node<T>>, // producer

    // Pool management fields
    next_free_node: AtomicUsize,
    recycled_count: AtomicUsize,

    // Pointers to memory regions (either heap or shared)
    base_ptr:       *mut Node<T>, // dummy node 
    nodes_pool_ptr: *mut Node<T>, // preallocated node pool
    recycled_ptr:   *mut AtomicPtr<Node<T>>, // preallocated recycle stack pointers

    pool_capacity: usize, // Same as PREALLOCATED_NODES
    owns_buffer: bool, // True if created by `new()`, false if `init_in_shared()`
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

// size helper

impl<T: Send + 'static> DynListQueue<T> {
    pub fn shared_size() -> usize {
        // Using fully qualified paths for size_of and align_of
        let layout_self = Layout::new::<Self>();
        let layout_dummy = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(PREALLOCATED_NODES).unwrap();
        let layout_recycle_array = Layout::array::<AtomicPtr<Node<T>>>(PREALLOCATED_NODES).unwrap();

        let (layout_after_dummy, _) = layout_self.extend(layout_dummy).unwrap();
        let (layout_after_pool, _) = layout_after_dummy.extend(layout_pool_array).unwrap();
        let (final_layout, _) = layout_after_pool.extend(layout_recycle_array).unwrap();
        
        final_layout.size()
    }
}

// constructors
impl<T: Send + 'static> DynListQueue<T> {
    pub fn new() -> Self {
        let dummy = Box::into_raw(Box::new(Node { val: None, next: AtomicPtr::new(null_node()) }));

        let mut pool_nodes_vec: Vec<Node<T>> = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            pool_nodes_vec.push(Node { val: None, next: AtomicPtr::new(null_node()) });
        }
        let pool_ptr = Box::into_raw(pool_nodes_vec.into_boxed_slice()) as *mut Node<T>;

        let mut recycle_slots_vec: Vec<AtomicPtr<Node<T>>> = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            recycle_slots_vec.push(AtomicPtr::new(null_node::<T>()));
        }
        let rec_ptr = Box::into_raw(recycle_slots_vec.into_boxed_slice()) as *mut AtomicPtr<Node<T>>;

        Self {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
            base_ptr: dummy,
            nodes_pool_ptr: pool_ptr,
            next_free_node: AtomicUsize::new(0),
            recycled_ptr: rec_ptr,
            recycled_count: AtomicUsize::new(0),
            pool_capacity: PREALLOCATED_NODES,
            owns_buffer: true, 
        }
    }

    pub unsafe fn init_in_shared(mem: *mut u8) -> &'static mut Self {
        let self_ptr = mem as *mut Self;

        let layout_self = Layout::new::<Self>();
        let layout_dummy_node = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(PREALLOCATED_NODES).unwrap();
        let layout_recycle_array = Layout::array::<AtomicPtr<Node<T>>>(PREALLOCATED_NODES).unwrap();

        let (layout_after_dummy, offset_dummy) = layout_self.extend(layout_dummy_node).unwrap();
        let (layout_after_pool, offset_pool_array) = layout_after_dummy.extend(layout_pool_array).unwrap();
        let (_final_layout, offset_recycle_array) = layout_after_pool.extend(layout_recycle_array).unwrap();
        
        let dummy_ptr = mem.add(offset_dummy) as *mut Node<T>;
        ptr::write(dummy_ptr, Node { val: None, next: AtomicPtr::new(null_node()) });

        let pool_nodes_ptr = mem.add(offset_pool_array) as *mut Node<T>;
        for i in 0..PREALLOCATED_NODES {
            ptr::write(
                pool_nodes_ptr.add(i),
                Node { val: None, next: AtomicPtr::new(null_node()) },
            );
        }

        let recycle_slots_ptr = mem.add(offset_recycle_array) as *mut AtomicPtr<Node<T>>;
        for i in 0..PREALLOCATED_NODES {
            ptr::write(recycle_slots_ptr.add(i), AtomicPtr::new(null_node::<T>()));
        }

        ptr::write(
            self_ptr,
            DynListQueue {
                head: AtomicPtr::new(dummy_ptr),
                tail: AtomicPtr::new(dummy_ptr),
                base_ptr: dummy_ptr,
                nodes_pool_ptr: pool_nodes_ptr,
                next_free_node: AtomicUsize::new(0),
                recycled_ptr: recycle_slots_ptr,
                recycled_count: AtomicUsize::new(0),
                pool_capacity: PREALLOCATED_NODES,
                owns_buffer: false,
            },
        );
        &mut *self_ptr
    }
}

// allocation / recycle
impl<T: Send + 'static> DynListQueue<T> {
    fn alloc_node(&self, v: T) -> *mut Node<T> {
        // recycled stack
        let mut n = self.recycled_count.load(Ordering::Acquire);
        while n != 0 { 
            if self.recycled_count
                .compare_exchange(n, n - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let slot_index = n - 1;
                let atomic_ptr_slot = unsafe { &*self.recycled_ptr.add(slot_index) };
                let mut node_ptr = atomic_ptr_slot.load(Ordering::Acquire);
                
                while node_ptr.is_null() {
                    // Important so os scheduler can switch threads 
                    // (processes for our case (scheduler makes no difference if threads or processes)) so no busy waiting happens (wait-free)
                    thread::yield_now(); 
                    node_ptr = atomic_ptr_slot.load(Ordering::Acquire);
                }
                atomic_ptr_slot.store(null_node::<T>(), Ordering::Release); 

                unsafe {
                    (*node_ptr).val  = Some(v);
                    (*node_ptr).next.store(null_node(), Ordering::Relaxed); 
                }
                return node_ptr;
            }
            n = self.recycled_count.load(Ordering::Acquire);
        }

        // pool
        let mut idx = self.next_free_node.load(Ordering::Acquire);
        while idx < self.pool_capacity {
            if self.next_free_node
                .compare_exchange(idx, idx + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let node = unsafe { self.nodes_pool_ptr.add(idx) };
                unsafe {
                    (*node).val  = Some(v);
                    (*node).next.store(null_node(), Ordering::Relaxed);
                }
                return node;
            }
            idx = self.next_free_node.load(Ordering::Acquire);
        }

        // heap fallback
        Box::into_raw(Box::new(Node { val: Some(v), next: AtomicPtr::new(null_node()) }))
    }

    #[inline]
    fn is_pool_node(&self, p: *mut Node<T>) -> bool {
        if p == self.base_ptr { // The initial dummy node is considered part of the "pool" for recycling logic
            return true;
        }
        let start = self.nodes_pool_ptr as usize;
        // Ensure nodes_pool_ptr is not null before doing arithmetic if it could be
        if self.nodes_pool_ptr.is_null() { return false; }
        let end   = unsafe { self.nodes_pool_ptr.add(self.pool_capacity) } as usize; 
        let addr  = p as usize;
        addr >= start && addr < end
    }

    fn recycle_node(&self, node_to_recycle: *mut Node<T>) {
        unsafe {
            (*node_to_recycle).val  = None; 
            (*node_to_recycle).next.store(null_node(), Ordering::Relaxed); 
        }

        // Try to recycle if it's a pool node (including base_ptr) or a heap node
        // and the recycle stack has capacity.
        if !self.is_pool_node(node_to_recycle) { // True for heap-allocated nodes
            let mut current_recycle_count = self.recycled_count.load(Ordering::Acquire);
            loop {
                if current_recycle_count >= self.pool_capacity { 
                    // Recycle stack is full, free the heap node
                    unsafe { drop(Box::from_raw(node_to_recycle)); }
                    return;
                }
                match self.recycled_count.compare_exchange(
                    current_recycle_count, 
                    current_recycle_count + 1, 
                    Ordering::AcqRel, 
                    Ordering::Acquire
                ) {
                    Ok(_) => {
                        unsafe { 
                            (*self.recycled_ptr.add(current_recycle_count)).store(node_to_recycle, Ordering::Release); 
                        }
                        return;
                    }
                    Err(prev_val) => { 
                        current_recycle_count = prev_val;
                    }
                }
            }
        } else { // Node is from the mmap-allocated pool (or is base_ptr)
            let mut current_recycle_count = self.recycled_count.load(Ordering::Acquire);
            loop {
                if current_recycle_count >= self.pool_capacity {
                    // Recycle stack is full. Pool nodes are not freed.
                    return; 
                }
                match self.recycled_count.compare_exchange(
                    current_recycle_count, 
                    current_recycle_count + 1, 
                    Ordering::AcqRel, 
                    Ordering::Acquire
                ) {
                    Ok(_) => {
                        unsafe { 
                            (*self.recycled_ptr.add(current_recycle_count)).store(node_to_recycle, Ordering::Release); 
                        }
                        return;
                    }
                    Err(prev_val) => {
                        current_recycle_count = prev_val;
                    }
                }
            }
        }
    }
}

// SpscQueue
impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = (); 
    type PopError  = (); 

    fn push(&self, item: T) -> Result<(), ()> {
        let new_node = self.alloc_node(item);
        if new_node.is_null() { return Err(()); } // Should ideally not happen with current alloc_node

        let current_tail_ptr = self.tail.load(Ordering::Relaxed); 
        
        unsafe { (*current_tail_ptr).next.store(new_node, Ordering::Release) };
        self.tail.store(new_node, Ordering::Release);
        Ok(())
    }

    fn pop(&self) -> Result<T, ()> {
        let current_head_ptr = self.head.load(Ordering::Acquire);
        let next_node_ptr = unsafe { (*current_head_ptr).next.load(Ordering::Acquire) };

        if next_node_ptr.is_null() { 
            return Err(()); 
        }
        
        let val = unsafe { (*next_node_ptr).val.take() }.ok_or(())?;
        self.head.store(next_node_ptr, Ordering::Release);
        self.recycle_node(current_head_ptr);
        Ok(val)
    }

    #[inline] fn available(&self) -> bool {
        // This queue is "unbounded" due to heap fallback.
        // For practical purposes of the preallocated pool:
        self.next_free_node.load(Ordering::Relaxed) < self.pool_capacity
            || self.recycled_count.load(Ordering::Relaxed) > 0
    }
    #[inline] fn empty(&self) -> bool {
        let h = self.head.load(Ordering::Relaxed); 
        unsafe { (*h).next.load(Ordering::Acquire).is_null() } 
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        if self.owns_buffer { // Only deallocate if created by new()
            // Drain the queue to drop remaining items and attempt to free heap-allocated nodes
            while let Ok(item) = SpscQueue::pop(self) { // Use trait method
                drop(item); 
            }
            
            unsafe {
                // Free any remaining heap-allocated nodes on the recycle stack
                let recycle_count = *self.recycled_count.get_mut(); // Safe in drop
                for i in 0..recycle_count {
                    let node_ptr = (*self.recycled_ptr.add(i)).load(Ordering::Relaxed);
                    if !node_ptr.is_null() && !self.is_pool_node(node_ptr) {
                        // It's a heap node if not a pool node and not the base_ptr
                        // is_pool_node already checks for base_ptr
                        let _ = Box::from_raw(node_ptr);
                    }
                }

                // Deallocate the pool of nodes
                if !self.nodes_pool_ptr.is_null() {
                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.nodes_pool_ptr, PREALLOCATED_NODES));
                }
                // Deallocate the recycle stack's backing array
                if !self.recycled_ptr.is_null() {
                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.recycled_ptr, PREALLOCATED_NODES));
                }
                // Deallocate the initial dummy node (base_ptr)
                if !self.base_ptr.is_null() {
                     let _ = Box::from_raw(self.base_ptr);
                }
            }
        }
    }
}
