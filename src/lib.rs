pub mod spsc;

pub use spsc::LamportQueue;
pub use spsc::DynListQueue;
pub use spsc::UnboundedQueue;
pub use spsc::MultiPushQueue;
pub use spsc::BQueue;

/// Common interface for all queues.
pub trait SpscQueue<T: Send>: Send + 'static {
    /// Error on push when the queue is full.
    type PushError;
    /// Error on pop when the queue is empty.
    type PopError;

    fn push(&self, item: T) -> Result<(), Self::PushError>;
    fn pop(&self) -> Result<T, Self::PopError>;

    /// True when a subsequent `push` *may* succeed without blocking.
    fn available(&self) -> bool;
    /// True when a subsequent `pop` will fail.
    fn empty(&self) -> bool;
}