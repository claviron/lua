//! One script's run, as its extensions see it: whether it has been told to
//! stop, and a way to wake whatever it is parked on when it is.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

type Waker = Box<dyn Fn() + Send + Sync>;

/// Shared by the engine and the extensions of one execution.
#[derive(Default)]
pub struct Execution {
    interrupted: AtomicBool,
    stopping: CancellationToken,
    parked: Mutex<HashMap<u64, Waker>>,
    next: AtomicU64,
}

impl Execution {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The word a parked wait watches besides its own: set when the execution
    /// is told to stop.
    pub fn interrupted(&self) -> &AtomicBool {
        &self.interrupted
    }

    /// Cancelled when the execution is told to stop: what an async wait
    /// selects on beside what it waits for.
    pub fn stopping(&self) -> &CancellationToken {
        &self.stopping
    }

    /// Announce a wait before it parks, with what wakes it; the guard takes the
    /// announcement back when the wait is over.
    pub fn park(
        self: &Arc<Self>,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Parked {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.parked.lock().unwrap().insert(id, Box::new(wake));
        Parked { execution: Arc::clone(self), id }
    }

    /// End every parked wait: the word first, then the wakes, so a wait already
    /// parked is released and one about to park reads the word.
    pub(crate) fn interrupt(&self) {
        self.interrupted.store(true, Ordering::SeqCst);
        self.stopping.cancel();
        for wake in self.parked.lock().unwrap().values() {
            wake();
        }
    }
}

/// A wait's announcement, taken back when it is dropped.
pub struct Parked {
    execution: Arc<Execution>,
    id: u64,
}

impl Drop for Parked {
    fn drop(&mut self) {
        self.execution.parked.lock().unwrap().remove(&self.id);
    }
}
