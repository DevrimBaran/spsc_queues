// dspsc by torquati
// works almost 6 times slower then uspsc like torquati says in the paper
// looses items in around 1% of iterations
use crate::spsc::lamport::LamportQueue;
use crate::SpscQueue;
use std::{
    alloc::Layout,
    ptr::{self, null_mut},
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering, fence},
};

// helpers
#[inline(always)]
const fn null_node<T: Send>() -> *mut Node<T> { null_mut() }

// More reasonable constants
const PREALLOCATED_NODES: usize = 1024; 
const NODE_CACHE_CAPACITY: usize = 4096; 
const CACHE_LINE_SIZE: usize = 512;

// Simple logging (turned off for production)
fn log_msg(msg: &str) {
    //eprintln!("[dSPSC] {}", msg);
}

// node

// strict alignment and adequate size for Node
#[repr(C, align(128))]  // Increased alignment to cache line size
struct Node<T: Send + 'static> {
    val: Option<T>,
    next: AtomicPtr<Node<T>>,
    // Padding to fill a cache line for better memory sharing
    _padding: [u8; CACHE_LINE_SIZE - 16], // 16 bytes for Option<T> + AtomicPtr
}

// Wrapper for raw node pointers
#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
struct NodePtr<U: Send + 'static>(*mut Node<U>);

unsafe impl<U: Send + 'static> Send for NodePtr<U> {}
unsafe impl<U: Send + 'static> Sync for NodePtr<U> {}

// queue

#[repr(C, align(128))]
pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>, 
    tail: AtomicPtr<Node<T>>, 
    // Fixed size padding to avoid false sharing
    padding1: [u8; CACHE_LINE_SIZE - 16], // 16 = size of two AtomicPtr

    nodes_pool_ptr: *mut Node<T>,
    next_free_node: AtomicUsize, 
    // Fixed size padding
    padding2: [u8; CACHE_LINE_SIZE - 16], 

    // Cache for recycled nodes
    node_cache: LamportQueue<NodePtr<T>>, 

    base_ptr: *mut Node<T>, 
    pool_capacity: usize,      
    owns_all: bool,    
    
    // Debug counters
    heap_allocs: AtomicUsize,
    heap_frees: AtomicUsize,
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn shared_size() -> usize {
        // Calculate total size needed for all components
        let layout_self = Layout::new::<Self>();
        let lamport_cache_size = LamportQueue::<NodePtr<T>>::shared_size(NODE_CACHE_CAPACITY);
        let layout_dummy_node = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(PREALLOCATED_NODES).unwrap();

        // Align all components to 128-byte boundaries (cache line)
        let (layout1, _) = layout_self.extend(layout_dummy_node).unwrap();
        let (layout2, _) = layout1.extend(layout_pool_array).unwrap();
        
        let lamport_align = std::cmp::max(std::mem::align_of::<LamportQueue<NodePtr<T>>>(), 128);
        let (final_layout, _) = layout2.align_to(lamport_align).unwrap()
            .extend(Layout::from_size_align(lamport_cache_size, lamport_align).unwrap()).unwrap();
        
        log_msg(&format!("DynListQueue::shared_size() = {}", final_layout.size()));
        
        final_layout.size()
    }
}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn new() -> Self {
        log_msg("DynListQueue::new()");
        
        // Create dummy node - this is the first node in the queue
        // and doesn't hold a value, just points to the next node
        let dummy = Box::into_raw(Box::new(Node { 
            val: None, 
            next: AtomicPtr::new(null_node()),
            _padding: [0; CACHE_LINE_SIZE - 16],
        }));
        
        // Create preallocated node pool
        let mut pool_nodes_vec: Vec<Node<T>> = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            pool_nodes_vec.push(Node { 
                val: None, 
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 16],
            });
        }
        let pool_ptr = Box::into_raw(pool_nodes_vec.into_boxed_slice()) as *mut Node<T>;
        
        // Create node cache
        let node_cache = LamportQueue::<NodePtr<T>>::with_capacity(NODE_CACHE_CAPACITY);

        Self {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
            padding1: [0; CACHE_LINE_SIZE - 16],
            base_ptr: dummy,
            nodes_pool_ptr: pool_ptr,
            next_free_node: AtomicUsize::new(0),
            padding2: [0; CACHE_LINE_SIZE - 16],
            node_cache,
            pool_capacity: PREALLOCATED_NODES,
            owns_all: true, 
            heap_allocs: AtomicUsize::new(0),
            heap_frees: AtomicUsize::new(0),
        }
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8) -> &'static mut Self {
        log_msg(&format!("DynListQueue::init_in_shared({:p})", mem_ptr));
        
        let self_ptr = mem_ptr as *mut Self;

        // Calculate offsets for each component
        let layout_self = Layout::new::<Self>();
        let layout_dummy_node = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(PREALLOCATED_NODES).unwrap();
        
        let lamport_cache_size = LamportQueue::<NodePtr<T>>::shared_size(NODE_CACHE_CAPACITY);
        let lamport_align = std::cmp::max(std::mem::align_of::<LamportQueue<NodePtr<T>>>(), 128);

        let (layout1, offset_dummy) = layout_self.extend(layout_dummy_node).unwrap();
        let (layout2, offset_pool_array) = layout1.extend(layout_pool_array).unwrap();
        let (_, offset_node_cache) = layout2.align_to(lamport_align).unwrap()
            .extend(Layout::from_size_align(lamport_cache_size, lamport_align).unwrap()).unwrap();

        // Initialize dummy node
        let dummy_ptr_val = mem_ptr.add(offset_dummy) as *mut Node<T>;
        
        ptr::write(dummy_ptr_val, Node { 
            val: None, 
            next: AtomicPtr::new(null_node()),
            _padding: [0; CACHE_LINE_SIZE - 16],
        });

        // Initialize pool nodes
        let pool_nodes_ptr_val = mem_ptr.add(offset_pool_array) as *mut Node<T>;
        
        for i in 0..PREALLOCATED_NODES {
            ptr::write(
                pool_nodes_ptr_val.add(i),
                Node { 
                    val: None, 
                    next: AtomicPtr::new(null_node()),
                    _padding: [0; CACHE_LINE_SIZE - 16],
                },
            );
        }
        
        // Initialize LamportQueue for node cache in shared memory
        let node_cache_mem_start = mem_ptr.add(offset_node_cache);
        
        let initialized_node_cache_ref = LamportQueue::<NodePtr<T>>::init_in_shared(
            node_cache_mem_start, 
            NODE_CACHE_CAPACITY
        );

        // Initialize main queue structure
        ptr::write(
            self_ptr,
            DynListQueue {
                head: AtomicPtr::new(dummy_ptr_val),
                tail: AtomicPtr::new(dummy_ptr_val),
                padding1: [0; CACHE_LINE_SIZE - 16],
                base_ptr: dummy_ptr_val,
                nodes_pool_ptr: pool_nodes_ptr_val,
                next_free_node: AtomicUsize::new(0),
                padding2: [0; CACHE_LINE_SIZE - 16],
                node_cache: ptr::read(initialized_node_cache_ref as *const _),
                pool_capacity: PREALLOCATED_NODES,
                owns_all: false,
                heap_allocs: AtomicUsize::new(0),
                heap_frees: AtomicUsize::new(0),
            },
        );

        // Ensure all memory writes are visible before returning
        fence(Ordering::SeqCst);
        
        &mut *self_ptr
    }
}

impl<T: Send + 'static> DynListQueue<T> {
    // Allocate a new node with the given value
    fn alloc_node(&self, v: T) -> *mut Node<T> {
        // Try to reuse a cached node first
        for _ in 0..3 { // Try a few times
            if let Ok(node_ptr_wrapper) = self.node_cache.pop() {
                let node_ptr = node_ptr_wrapper.0;
                if !node_ptr.is_null() { 
                    unsafe {
                        // Clear any previous data and reinitialize
                        ptr::write(&mut (*node_ptr).val, Some(v));
                        (*node_ptr).next.store(null_node(), Ordering::SeqCst);
                    }
                    return node_ptr;
                }
            }
            // Spin a bit before retrying
            std::hint::spin_loop();
        }

        // Then try to get from preallocated pool
        let idx = self.next_free_node.fetch_add(1, Ordering::SeqCst);
        if idx < self.pool_capacity {
            let node = unsafe { self.nodes_pool_ptr.add(idx) };
            
            unsafe {
                // Initialize the node
                ptr::write(&mut (*node).val, Some(v));
                (*node).next.store(null_node(), Ordering::SeqCst);
            }
            return node;
        }

        // Last resort: allocate from heap
        let count = self.heap_allocs.fetch_add(1, Ordering::Relaxed) + 1;
        log_msg(&format!("Heap allocation #{}", count));
        
        // Allocate with alignment
        let layout = Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) as *mut Node<T> };
        
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        
        unsafe {
            ptr::write(ptr, Node {
                val: Some(v),
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 16],
            });
        }
        
        log_msg(&format!("Allocated heap node at {:p}", ptr));
        ptr
    }

    #[inline]
    fn is_pool_node(&self, p: *mut Node<T>) -> bool {
        if p == self.base_ptr { 
            return true;
        }
        
        if self.nodes_pool_ptr.is_null() { 
            return false; 
        }
        
        let start = self.nodes_pool_ptr as usize;
        let end = unsafe { self.nodes_pool_ptr.add(self.pool_capacity) } as usize; 
        let addr = p as usize;
        
        addr >= start && addr < end
    }

    // Consumer recycles a node
    fn recycle_node(&self, node_to_recycle: *mut Node<T>) {
        if node_to_recycle.is_null() {
            return;
        }
        
        unsafe {
            // Clear the node data
            if let Some(val) = ptr::replace(&mut (*node_to_recycle).val, None) {
                drop(val);
            }
            (*node_to_recycle).next.store(null_node(), Ordering::SeqCst);
        }

        // Simplify the recycling path to avoid issues
        if self.is_pool_node(node_to_recycle) {
            // Basic approach: try once to cache, if it fails that's OK
            let _ = self.node_cache.push(NodePtr(node_to_recycle));
            // No retry loops that could cause issues
        } else {
            // For heap nodes, always deallocate
            let count = self.heap_frees.fetch_add(1, Ordering::Relaxed) + 1;
            log_msg(&format!("Heap deallocation #{}", count));
            
            unsafe {
                let layout = Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                std::alloc::dealloc(node_to_recycle as *mut u8, layout);
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = (); 
    type PopError = (); 

    fn push(&self, item: T) -> Result<(), ()> {
        log_msg("push: entry");
        
        // Producer allocates a new node
        let new_node = self.alloc_node(item);
        
        // Ensure node is initialized before linking
        fence(Ordering::SeqCst);
        
        // Producer links the new node
        
        // Get the current tail (only producer modifies this)
        let current_tail_ptr = self.tail.load(Ordering::SeqCst);
        
        // Validate tail pointer before using it
        if current_tail_ptr.is_null() {
            // This shouldn't happen, but guard against it
            log_msg("ERROR: Null tail pointer in push!");
            return Err(());
        }
        
        // Link the new node from the current tail
        unsafe { 
            (*current_tail_ptr).next.store(new_node, Ordering::SeqCst);
        }
        
        // Memory barrier to ensure the link is visible before updating tail
        fence(Ordering::SeqCst);
        
        // Update the tail pointer to point to the new node
        self.tail.store(new_node, Ordering::SeqCst);
        
        Ok(())
    }

    fn pop(&self) -> Result<T, ()> {
        // Consumer checks if queue is empty
        
        // Get the current head (the dummy node)
        let current_dummy_ptr = self.head.load(Ordering::SeqCst);
        
        // Validate head pointer
        if current_dummy_ptr.is_null() {
            log_msg("ERROR: Null head pointer in pop!");
            return Err(());
        }
        
        // Memory barrier to ensure we see the latest next pointer
        fence(Ordering::SeqCst);
        
        // Check if queue is empty by looking at the dummy's next pointer
        let item_node_ptr = unsafe { 
            (*current_dummy_ptr).next.load(Ordering::SeqCst) 
        };
        
        if item_node_ptr.is_null() { 
            return Err(()); // Queue is empty
        }
        
        // Consumer extracts the value and updates head
        
        // Extract the value with additional validation
        let value = unsafe {
            if item_node_ptr.is_null() {
                // Double-check after the fence
                return Err(());
            }
            
            // Check if the node has a value
            if let Some(value) = ptr::replace(&mut (*item_node_ptr).val, None) {
                value
            } else {
                // No value found (shouldn't happen, but be defensive)
                log_msg("ERROR: Node without value in pop!");
                return Err(());
            }
        };
        
        // Memory barrier before updating head
        fence(Ordering::SeqCst);
        
        // Update head pointer to make the item node the new dummy
        self.head.store(item_node_ptr, Ordering::SeqCst);
        
        // Memory barrier before recycling
        fence(Ordering::SeqCst);
        
        // Recycle the old dummy node
        self.recycle_node(current_dummy_ptr);
        
        Ok(value)
    }

    #[inline] 
    fn available(&self) -> bool {
        // Dynamic queue is always available for push
        true
    }

    #[inline] 
    fn empty(&self) -> bool {
        // Queue is empty if head's next pointer is null
        let h = self.head.load(Ordering::SeqCst); 
        
        if h.is_null() {
            // This shouldn't happen, but be defensive
            return true;
        }
        
        unsafe { (*h).next.load(Ordering::SeqCst).is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        // Print diagnostics
        let allocs = self.heap_allocs.load(Ordering::Relaxed);
        let frees = self.heap_frees.load(Ordering::Relaxed);
        log_msg(&format!("Dropping queue - heap allocs: {}, heap frees: {}", allocs, frees));
        
        if self.owns_all {
            // Drain the queue
            while let Ok(item) = SpscQueue::pop(self) {
                drop(item);
            }
            
            // Handle the node_cache
            unsafe {
                // First, pop and free any nodes still in the cache
                while let Ok(node_ptr) = self.node_cache.pop() {
                    if !node_ptr.0.is_null() && !self.is_pool_node(node_ptr.0) {
                        // For heap nodes, free them properly
                        ptr::drop_in_place(&mut (*node_ptr.0).val);
                        let layout = Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                        std::alloc::dealloc(node_ptr.0 as *mut u8, layout);
                    }
                }
                
                // Now drop the internal buffer of the LamportQueue itself
                ptr::drop_in_place(&mut self.node_cache.buf);
            }

            // Deallocate the pool of nodes as a slice
            unsafe {
                if !self.nodes_pool_ptr.is_null() {
                    // First, make sure all nodes are properly dropped
                    for i in 0..self.pool_capacity {
                        let node = self.nodes_pool_ptr.add(i);
                        ptr::drop_in_place(&mut (*node).val);
                    }
                    
                    // Then free the entire slice
                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(
                        self.nodes_pool_ptr, 
                        PREALLOCATED_NODES
                    ));
                }
                
                // Deallocate the base/dummy node if it isn't already handled
                if !self.base_ptr.is_null() {
                    if self.head.load(Ordering::Relaxed) == self.base_ptr {
                        ptr::drop_in_place(&mut (*self.base_ptr).val);
                        let _ = Box::from_raw(self.base_ptr);
                    }
                }
            }
        }
    }
}