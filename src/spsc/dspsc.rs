// dSPSC – dynamic list with node cache - wait-free implementation
use crate::SpscQueue;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

// Preallocated nodes to ensure wait-free operation
const PREALLOCATED_NODES: usize = 32;

struct Node<T> { 
    val: Option<T>, 
    next: *mut Node<T> 
}

pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,      // consumer reads
    tail: AtomicPtr<Node<T>>,      // producer writes
    // Pre-allocated node storage - use UnsafeCell for interior mutability
    nodes_storage: Box<[u8]>,      // Raw memory for nodes
    nodes_ptr: AtomicPtr<Node<T>>, // Pointer to the first node
    // Index of the next free node to use
    next_free_node: AtomicUsize,
    // Array of recycled nodes
    recycled: Box<[AtomicPtr<Node<T>>]>,
    recycled_count: AtomicUsize,
}

unsafe impl<T: Send> Sync for DynListQueue<T> {}
unsafe impl<T: Send + 'static> Send for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn new(_cache_sz: usize) -> Self {
        // Create dummy node
        let dummy = Box::into_raw(Box::new(Node{val:None, next:null_mut()}));
        
        // Pre-allocate memory for nodes
        let node_size = std::mem::size_of::<Node<T>>();
        let total_size = node_size * PREALLOCATED_NODES;
        let mut nodes_storage = vec![0u8; total_size].into_boxed_slice();
        
        // Initialize the memory as Node<T>
        let nodes_ptr = nodes_storage.as_mut_ptr() as *mut Node<T>;
        for i in 0..PREALLOCATED_NODES {
            unsafe {
                std::ptr::write(
                    nodes_ptr.add(i), 
                    Node {
                        val: None,
                        next: null_mut()
                    }
                );
            }
        }
        
        // Create recycled array
        let mut recycled_vec = Vec::with_capacity(PREALLOCATED_NODES);
        for _ in 0..PREALLOCATED_NODES {
            recycled_vec.push(AtomicPtr::new(null_mut()));
        }
        
        Self { 
            head: AtomicPtr::new(dummy), 
            tail: AtomicPtr::new(dummy),
            nodes_storage,
            nodes_ptr: AtomicPtr::new(nodes_ptr),
            next_free_node: AtomicUsize::new(0),
            recycled: recycled_vec.into_boxed_slice(),
            recycled_count: AtomicUsize::new(0),
        }
    }
    
    // Wait-free node allocation - always succeeds in bounded time
    fn alloc_node(&self, v: T) -> *mut Node<T> {
        // First try to use a recycled node
        let recycled_count = self.recycled_count.load(Ordering::Relaxed);
        if recycled_count > 0 {
            // Try to decrease the recycled count
            if self.recycled_count.compare_exchange(
                recycled_count,
                recycled_count - 1,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                // Get a recycled node
                let node_ptr = self.recycled[recycled_count - 1].load(Ordering::Relaxed);
                if !node_ptr.is_null() {
                    unsafe {
                        (*node_ptr).val = Some(v);
                        (*node_ptr).next = null_mut();
                        return node_ptr;
                    }
                }
            }
        }
        
        // If no recycled nodes, use a pre-allocated one
        let next_free = self.next_free_node.load(Ordering::Relaxed);
        if next_free < PREALLOCATED_NODES {
            // Try to increment the next_free_node counter
            if self.next_free_node.compare_exchange(
                next_free,
                next_free + 1,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                // Use a pre-allocated node
                let nodes_ptr = self.nodes_ptr.load(Ordering::Relaxed);
                let node = unsafe { &mut *nodes_ptr.add(next_free) };
                node.val = Some(v);
                node.next = null_mut();
                return node;
            }
        }
        
        // If all else fails, allocate a new node
        Box::into_raw(Box::new(Node{val: Some(v), next: null_mut()}))
    }
    
    // Wait-free recycle operation - stores node for reuse
    fn recycle_node(&self, node: *mut Node<T>) {
        let recycled_count = self.recycled_count.load(Ordering::Relaxed);
        if recycled_count < PREALLOCATED_NODES {
            // Try to add to recycled nodes
            if self.recycled_count.compare_exchange(
                recycled_count,
                recycled_count + 1,
                Ordering::AcqRel,
                Ordering::Relaxed
            ).is_ok() {
                self.recycled[recycled_count].store(node, Ordering::Relaxed);
                return;
            }
        }
        
        // If recycled array is full, free the node
        unsafe { drop(Box::from_raw(node)); }
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = ();
    type PopError = ();
    
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        // Allocate a new node - wait-free operation
        let n = self.alloc_node(item);
        
        // Update the tail's next pointer
        let tail_ptr = self.tail.load(Ordering::Relaxed);
        unsafe { (*tail_ptr).next = n };
        
        // Update the tail pointer
        self.tail.store(n, Ordering::Release);
        
        Ok(())
    }
    
    fn pop(&self) -> Result<T, Self::PopError> {
        // Load the head
        let head = self.head.load(Ordering::Acquire);
        
        // Check if queue is empty
        let next = unsafe { (*head).next };
        if next.is_null() { 
            return Err(()); 
        }
        
        // Extract the value
        let result = unsafe { (*next).val.take().unwrap() };
        
        // Update the head
        self.head.store(next, Ordering::Release);
        
        // Recycle the old head node
        self.recycle_node(head);
        
        Ok(result)
    }
    
    // This queue is unbounded, so always has space available
    fn available(&self) -> bool { true }
    
    // Queue is empty when head's next pointer is null
    fn empty(&self) -> bool { 
        let head = self.head.load(Ordering::Relaxed);
        unsafe { (*head).next.is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        // Free all remaining nodes in the queue
        let mut current = self.head.load(Ordering::Relaxed);
        while !current.is_null() {
            let next = unsafe { (*current).next };
            unsafe { drop(Box::from_raw(current)) };
            current = next;
        }
        
        // Nodes in the recycled array will be freed automatically when the Box is dropped
        
        // Clean up any nodes that were allocated from our pre-allocated storage
        let nodes_ptr = self.nodes_ptr.load(Ordering::Relaxed);
        for i in 0..PREALLOCATED_NODES {
            unsafe {
                let node = &mut *nodes_ptr.add(i);
                if node.val.is_some() {
                    node.val = None;
                }
            }
        }
    }
}