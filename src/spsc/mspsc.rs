use crate::spsc::LamportQueue;
use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// compile-time size of the producer’s scratch buffer (paper uses 16)
const LOCAL_BUF: usize = 16;

pub struct MultiPushQueue<T: Send + 'static> {
    // underlying classic Lamport ring allocated in shared or private memory
    inner: *mut LamportQueue<T>,

    // producer-private scratch buffer that accumulates up to 16 items
    local_buf: UnsafeCell<[MaybeUninit<T>; LOCAL_BUF]>,
    local_count: AtomicUsize, // number of valid items currently in `local_buf`

    // set to `true` if `inner` lives in caller-provided shared memory and must not be dropped.
    shared: AtomicBool,
}

unsafe impl<T: Send> Send for MultiPushQueue<T> {}
unsafe impl<T: Send> Sync for MultiPushQueue<T> {}

// constructors
impl<T: Send + 'static> MultiPushQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let boxed = Box::new(LamportQueue::with_capacity(capacity));
        Self::from_raw(Box::into_raw(boxed), false)
    }

    // Safety
    // `mem` must be a contiguous writable region of at least
    // `shared_size(capacity)` bytes that lives for `'static` and is visible to
    // both producer and consumer (e.g. `mmap` with `MAP_SHARED`).
    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        let head = mem as *mut MaybeUninit<Self>;
        let ring_ptr = mem.add(core::mem::size_of::<Self>()) as *mut u8;
        let ring = LamportQueue::init_in_shared(ring_ptr, capacity) as *mut _;
        ptr::write(head, MaybeUninit::new(Self::from_raw(ring, true)));
        &mut *(*head).as_mut_ptr()
    }

    pub const fn shared_size(capacity: usize) -> usize {
        core::mem::size_of::<Self>() + LamportQueue::<T>::shared_size(capacity)
    }

    #[inline(always)]
    fn from_raw(ring: *mut LamportQueue<T>, shared: bool) -> Self {
        Self {
            inner: ring,
            local_buf: UnsafeCell::new(unsafe { MaybeUninit::uninit().assume_init() }),
            local_count: AtomicUsize::new(0),
            shared: AtomicBool::new(shared),
        }
    }

    #[inline(always)]
    fn ring(&self) -> &LamportQueue<T> {
        unsafe { &*self.inner }
    }

    #[inline(always)]
    fn ring_mut(&self) -> &mut LamportQueue<T> {
        unsafe { &mut *self.inner }
    }

    // contiguous free slots starting at producer cursor (“distance to the next wrap”)
    #[inline(always)]
    fn contiguous_free(&self) -> usize {
        let cap = self.ring().capacity();
        let head = self.ring().head_relaxed();  // producer-private load
        let tail = self.ring().tail_relaxed();   // consumer cursor (may change)
        let used = head.wrapping_sub(tail) & (cap - 1);
        let free_total = cap - used - 1;                     // Lamport invariant
        let room_till_wrap = cap - (head & (cap - 1));       // contiguous part
        free_total.min(room_till_wrap)
    }

    // Trying to flush `local_buf[0..local_count]` into the ring atomically.
    // Returns `true` on success; on failure nothing is copied and the scratch
    // buffer remains intact.
    #[inline]
    fn multipush(&self) -> bool {
        let cnt = self.local_count.load(Ordering::Relaxed);
        if cnt == 0 { return true; }

        // Paper: proceed only if the whole batch fits contiguously.
        if self.contiguous_free() < cnt {
            return false; // leave scratch untouched
        }

        // Commit the batch in reverse order (improves distance head-tail).
        let buf = unsafe { &mut *self.local_buf.get() };
        for idx in (0..cnt).rev() {
            let val = unsafe { ptr::read(buf[idx].as_ptr()) };
            // safe because we just proved space exists
            unsafe { self.ring_mut().push_unchecked(val) };
        }

        self.local_count.store(0, Ordering::Relaxed);
        true
    }
}

// Drop
impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {
        // try flushing remaining items (best effort)
        let _ = self.multipush();

        // drop any leftovers still in the scratch buffer
        let cnt = self.local_count.load(Ordering::Relaxed);
        if cnt > 0 {
            let buf = unsafe { &mut *self.local_buf.get() };
            for i in 0..cnt {
                unsafe { ptr::drop_in_place(buf[i].as_mut_ptr()) };
            }
        }

        if !self.shared.load(Ordering::Relaxed) {
            unsafe { drop(Box::from_raw(self.inner)); }
        }
    }
}

// SpscQueue impl
impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
    type PushError = ();
    type PopError  = <LamportQueue<T> as SpscQueue<T>>::PopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        //append to scratch buffer
        let idx = self.local_count.load(Ordering::Relaxed);
        if idx < LOCAL_BUF {
            unsafe { (*self.local_buf.get())[idx].as_mut_ptr().write(item); }
            self.local_count.store(idx + 1, Ordering::Relaxed);
            // if the scratch just became full ⇒ try to flush
            if idx + 1 == LOCAL_BUF {
                let _ = self.multipush(); // ignore fail: scratch stays valid
            }
            return Ok(());
        }

        // if scratch buffer full – attempt flush then retry
        if self.multipush() {
            // now we definitely have < LOCAL_BUF free in scratch
            return self.push(item);
        }

        // fallback: try direct insertion into the ring
        self.ring_mut()
            .push(item)
            .map_err(|_| ())
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ring().pop()
    }

    #[inline]
    fn available(&self) -> bool {
        self.local_count.load(Ordering::Relaxed) < LOCAL_BUF || self.ring().available()
    }

    #[inline]
    fn empty(&self) -> bool {
        self.local_count.load(Ordering::Relaxed) == 0 && self.ring().empty()
    }
}

// Debug
impl<T: Send> fmt::Debug for MultiPushQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiPushQueue")
            .field("local_count", &self.local_count.load(Ordering::Relaxed))
            .field("shared", &self.shared.load(Ordering::Relaxed))
            .finish()
    }
}
