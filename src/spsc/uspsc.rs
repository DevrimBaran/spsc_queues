// Unbounded spsc from Torquati
use crate::spsc::LamportQueue;
use crate::SpscQueue;
use nix::libc;
use std::{
    cell::UnsafeCell,
    mem::{align_of, size_of, MaybeUninit},
    ptr,
    sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
};

// constants
const BUF_CAP:  usize = 1024; // elements in one Lamport ring (must be 2^n)
const POOL_CAP: usize = 32; // spare rings cached inline

// Number of preallocated rings to ensure wait-free operation
const PREALLOCATED_RINGS: usize = 4;

const CHILD_READY: u32 = 1; // producer mapped – waiting for consumer
const BOTH_READY:  u32 = 2; // mapped in both processes, slot reusable


// metadata for one spare ring
#[repr(C)]
struct RingSlot<T: Send + 'static> {
    prod_ptr: UnsafeCell<*mut LamportQueue<T>>, // address in producer
    cons_ptr: UnsafeCell<*mut LamportQueue<T>>, // address in consumer
    fd:   AtomicU32,
    pid:  AtomicU32,
    len:  AtomicUsize,
    flag: AtomicU32, // CHILD_READY | BOTH_READY
    initialized: AtomicBool, // Whether this slot is ready to use
}

// queue header
pub struct UnboundedQueue<T: Send + 'static> {
    // active rings
    write: UnsafeCell<*mut LamportQueue<T>>, // current producer ring
    read:  UnsafeCell<*mut LamportQueue<T>>, // current consumer ring

    // metadata for lazy re-mapping after fork()
    fixed_fd:  [AtomicU32; 2],
    fixed_pid: [AtomicU32; 2],
    fixed_len: AtomicUsize,

    // pool of spare rings
    pool: UnsafeCell<[MaybeUninit<RingSlot<T>>; POOL_CAP]>,
    head: AtomicUsize,
    tail: AtomicUsize,
    
    // preallocated rings to ensure wait-free operation
    preallocated_rings: [UnsafeCell<*mut LamportQueue<T>>; PREALLOCATED_RINGS],
    next_free_ring: AtomicUsize,
    
    // status flags
    initialized: AtomicBool,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

// internal helpers
impl<T: Send + 'static> UnboundedQueue<T> {
    #[inline]
    fn ensure_fixed(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire) {
            return false;
        }
        
        let len = self.fixed_len.load(Ordering::Acquire);
        if len == 0 || len > 1024 * 1024 * 1024 { // 1GB arbitrary safety limit
            return false;
        }
    
        // For anonymous shared memory, we don't need to check FDs
        // We just need to make sure the pointers are valid
        unsafe {
            let write_ptr = *self.write.get();
            let read_ptr = *self.read.get();
            
            // Verify pointers are not null
            if write_ptr.is_null() || read_ptr.is_null() {
                return false;
            }
            
            // Verify pointers are in valid range (simple sanity check)
            if (write_ptr as usize) < 0x1000 || (read_ptr as usize) < 0x1000 {
                return false;
            }
            
            true
        }
    }
    
    // Get a new ring buffer - wait-free operation
    fn get_new_ring(&self) -> Option<*mut LamportQueue<T>> {
        // First try to get a preallocated ring
        let next_free = self.next_free_ring.load(Ordering::Relaxed);
        if next_free < PREALLOCATED_RINGS {
            if self.next_free_ring.compare_exchange(
                next_free, 
                next_free + 1, 
                Ordering::AcqRel, 
                Ordering::Relaxed
            ).is_ok() {
                return Some(unsafe { *self.preallocated_rings[next_free].get() });
            }
        }
        
        // Then try to get one from the pool (non-blocking)
        if let Some((ring, _, _, _)) = self.pool_pop_prod_nonblocking() {
            return Some(ring);
        }
        
        // As a fallback for truly exceptional cases (not wait-free but rarely needed)
        // Just return none to indicate no ring is available
        None
    }
}

impl<T: Send + 'static> UnboundedQueue<T> {
    // This method will prepare the queue with an initial allocation
    pub fn prepare_for_use(&self) -> bool {
        // Ensure fixed mappings are valid
        if !self.ensure_fixed() {
            return false;
        }
        
        // Validate pool state
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        
        if tail.wrapping_sub(head) > POOL_CAP {
            return false;
        }
        
        // All checks passed, queue is ready to use
        true
    }
}

// SpscQueue implementation
impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError  = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        // 0) Ensure shared‐mapping pointers are sane
        if !self.ensure_fixed() {
            return Err(());
        }
    
        // 1) Fast‐path: try the current producer ring
        let cur = unsafe { *self.write.get() };
        if cur.is_null() {
            return Err(());
        }
        if unsafe { (&*cur).available() } {
            // room! this cannot fail
            return unsafe { (&*cur).push(item) };
        }
    
        // 2) Slow‐path: ring is full → grab a fresh one (wait-free)
        let new_ring = self
            .get_new_ring()
            .ok_or(())?;
    
        // 3) Publish it for future pushes
        unsafe { *self.write.get() = new_ring; }
    
        // 4) Push into the fresh, empty ring (must succeed)
        unsafe { (&*new_ring).push(item) }.map_err(|_| ())
    }
    
    // Wait-free pop implementation
    fn pop(&self) -> Result<T, Self::PopError> {
        // 0) Sanity check
        if !self.ensure_fixed() {
            return Err(());
        }
    
        // 1) Fast‐path: pop from the current consumer ring
        let mut cur = unsafe { *self.read.get() };
        if cur.is_null() {
            return Err(());
        }
        if let Ok(v) = unsafe { (&*cur).pop() } {
            return Ok(v);
        }
    
        // 2) If ring is empty but producer has moved on, we must switch rings
        let write_ptr = unsafe { *self.write.get() };
        if cur == write_ptr {
            // truly empty
            return Err(());
        }
    
        // 3) Double-check emptiness (avoid races), then recycle old ring
        if unsafe { (&*cur).empty() } {
            // Return this now-empty ring to the shared pool
            //
            //   fd = u32::MAX (we don’t use FDs here),
            //   len = the ring’s shared_size (fixed_len),
            //   is_static = false
            let len = self.fixed_len.load(Ordering::Acquire);
            self.pool_push_prod(cur, u32::MAX, len, false);
    
            // Grab the next available ring (must succeed, since write_ptr != cur)
            let next = self
                .get_new_ring()
                .ok_or(())?;
    
            // Publish it as our new read ring
            unsafe { *self.read.get() = next; }
            cur = next;
        }
    
        // 4) Finally, pop from that new ring
        unsafe { (&*cur).pop() }
    }
    
    fn available(&self) -> bool {
        // Check if the queue has been initialized
        if !self.initialized.load(Ordering::Acquire) {
            return false;
        }
        
        // Check if the write buffer is valid
        let write_ptr = unsafe { *self.write.get() };
        if write_ptr.is_null() {
            return false;
        }
        
        // Check if current ring has space
        unsafe {
            (&*write_ptr).available()
        }
    }

    fn empty(&self) -> bool {
        // Check if the queue has been initialized
        if !self.initialized.load(Ordering::Acquire) {
            return true;
        }
        
        // Check if the read buffer is valid
        let read_ptr = unsafe { *self.read.get() };
        if read_ptr.is_null() {
            return true;
        }
        
        // Check if current ring is empty
        unsafe {
            (&*read_ptr).empty()
        }
    }
}

// shared-memory construction
impl<T: Send + 'static> UnboundedQueue<T> {
    pub const fn shared_size() -> usize {
        let hdr = size_of::<Self>();
        let a   = align_of::<LamportQueue<T>>();
        let pad = (a - (hdr % a)) % a;
        // Additional space for preallocated rings
        hdr + pad + (2 + POOL_CAP + PREALLOCATED_RINGS) * LamportQueue::<T>::shared_size(BUF_CAP)
    }

    // Safety: mem must be a writable, process-shared mapping of shared_size() bytes.
    pub unsafe fn init_in_shared(mem: *mut u8) -> &'static mut Self {
        // Calculate memory layout
        let hdr = size_of::<Self>();
        let a = align_of::<LamportQueue<T>>();
        let pad = (a - (hdr % a)) % a;
        let ring_sz = LamportQueue::<T>::shared_size(BUF_CAP);
    
        // Initialize the queue structure with null pointers
        let this = mem.cast::<MaybeUninit<Self>>();
        
        // Create array of null pointers for preallocated rings
        let preallocated = [
            UnsafeCell::new(ptr::null_mut()),
            UnsafeCell::new(ptr::null_mut()),
            UnsafeCell::new(ptr::null_mut()),
            UnsafeCell::new(ptr::null_mut()),
        ];
        
        ptr::write(
            this,
            MaybeUninit::new(Self {
                write: UnsafeCell::new(ptr::null_mut()),
                read: UnsafeCell::new(ptr::null_mut()),
                fixed_fd: [AtomicU32::new(u32::MAX), AtomicU32::new(u32::MAX)],
                fixed_pid: [AtomicU32::new(0), AtomicU32::new(0)],
                fixed_len: AtomicUsize::new(0),
                pool: UnsafeCell::new(
                    MaybeUninit::<[MaybeUninit<RingSlot<T>>; POOL_CAP]>::uninit().assume_init(),
                ),
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                preallocated_rings: preallocated,
                next_free_ring: AtomicUsize::new(0),
                initialized: AtomicBool::new(false),
            }),
        );
    
        let me: &mut Self = &mut *(*this).as_mut_ptr();
    
        // Calculate the start address for buffers (after queue structure)
        let mut cur = mem.add(hdr + pad);
        
        // Initialize primary buffer
        let initial_ring = LamportQueue::init_in_shared(cur, BUF_CAP);
        *me.write.get() = initial_ring;
        *me.read.get() = initial_ring;  // Both point to the same buffer initially
        cur = cur.add(ring_sz);
        
        // Skip the second buffer position but still advance the pointer
        cur = cur.add(ring_sz);
    
        // Store metadata about the ring size and our PID
        me.fixed_len.store(ring_sz, Ordering::Release);
        let pid = libc::getpid() as u32;
        me.fixed_pid[0].store(pid, Ordering::Release);
        me.fixed_pid[1].store(pid, Ordering::Release);
    
        // Initialize the pool of additional buffers
        let pool = &mut *me.pool.get();
        for slot in pool.iter_mut() {
            let ring = LamportQueue::init_in_shared(cur, BUF_CAP);
            cur = cur.add(ring_sz);
            
            // Set up each slot with proper initial values
            slot.write(RingSlot {
                prod_ptr: UnsafeCell::new(ring),
                cons_ptr: UnsafeCell::new(ring),
                fd: AtomicU32::new(u32::MAX),
                pid: AtomicU32::new(pid),
                len: AtomicUsize::new(ring_sz),
                flag: AtomicU32::new(BOTH_READY),
                initialized: AtomicBool::new(true),
            });
        }

        me.tail.store(POOL_CAP, Ordering::Release);
        
        // Initialize preallocated rings
        for i in 0..PREALLOCATED_RINGS {
            let ring = LamportQueue::init_in_shared(cur, BUF_CAP);
            cur = cur.add(ring_sz);
            *me.preallocated_rings[i].get() = ring;
        }
        
        // Mark as initialized
        me.initialized.store(true, Ordering::Release);
    
        me
    }
}

// pool helpers – producer (non-blocking)
impl<T: Send + 'static> UnboundedQueue<T> {
   // Attempts to get a buffer from the pool without blocking
    fn pool_pop_prod_nonblocking(&self) -> Option<(*mut LamportQueue<T>, u32, usize, bool)> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if head == tail { return None; }
        
        // Safety check for pool size
        if tail.wrapping_sub(head) > POOL_CAP {
            return None;
        }
        
        let slot_idx = head % POOL_CAP;
        let slot = unsafe { &mut *(*self.pool.get())[slot_idx].as_mut_ptr() };
        
        // Check if slot is initialized and ready
        if !slot.initialized.load(Ordering::Acquire) || 
            slot.flag.load(Ordering::Acquire) != BOTH_READY {
            return None;
        }
        
        // Try to advance the head - if someone else has already taken this spot, return None
        if self.head.compare_exchange(
            head, 
            head + 1, 
            Ordering::AcqRel, 
            Ordering::Relaxed
        ).is_err() {
            return None;
        }
        
        let ring_ptr = unsafe { *slot.prod_ptr.get() };
        
        // Don't return null pointers
        if ring_ptr.is_null() {
            return None;
        }
        
        let fd = slot.fd.load(Ordering::Acquire);
        let len = slot.len.load(Ordering::Acquire);
        let is_static = slot.pid.load(Ordering::Acquire) == 0;
        
        Some((ring_ptr, fd, len, is_static))
    }

    fn pool_push_prod(&self, ring: *mut LamportQueue<T>, fd: u32, len: usize, is_static: bool) {
        if ring.is_null() {
            return;
        }
    
        let tail = self.tail.load(Ordering::Relaxed);
        let idx  = tail % POOL_CAP;
        if tail.wrapping_sub(self.head.load(Ordering::Relaxed)) >= POOL_CAP {
            return;
        }
    
        let slot = unsafe { &mut *(*self.pool.get())[idx].as_mut_ptr() };
        unsafe { *slot.prod_ptr.get() = ring };
    
        // Always mark BOTH_READY, and do the cast inside the unsafe block:
        slot.fd.store(
            if is_static { u32::MAX } else { fd },
            Ordering::Relaxed,
        );
        slot.len.store(
            if is_static { 0 } else { len },
            Ordering::Relaxed,
        );
        slot.pid.store(
            if is_static {
                0
            } else {
                // Here we cast inside the unsafe, so no dangling `as` at top level
                unsafe { libc::getpid() as u32 }
            },
            Ordering::Relaxed,
        );
        unsafe { *slot.cons_ptr.get() = ring };
        slot.initialized.store(true, Ordering::Release);
        slot.flag.store(BOTH_READY, Ordering::Release);
    
        self.tail.fetch_add(1, Ordering::AcqRel);
    }
}