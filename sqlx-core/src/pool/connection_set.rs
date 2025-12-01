use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use event_listener::{Event, IntoNotification};
use crate::sync::{AsyncMutex, AsyncMutexGuardArc};

pub struct ConnectionSet<C> {
    global: Arc<Global<C>>,
    slots: Box<[Arc<Slot<C>>]>
}

pub struct ConnectedSlot<C>(SlotGuard<C>);

pub struct DisconnectedSlot<C>(SlotGuard<C>);

struct Global<C> {
    unlock_event: Event<SlotGuard<C>>,
    disconnect_event: Event<SlotGuard<C>>,
    connected_set: Box<[AtomicBool]>,
}

struct SlotGuard<C> {
    slot: Option<Arc<Slot<C>>>,
    // `Option` allows us to take the guard in the drop handler.
    locked: Option<AsyncMutexGuardArc<Option<C>>>,
}

struct Slot<C> {
    global: Arc<Global<C>>,
    index: usize,
    connection: Arc<AsyncMutex<Option<C>>>,
}

impl<C> ConnectionSet<C> {
    pub fn new(size: usize) -> Self {
        let global = Arc::new(Global {
            unlock_event: Event::with_tag(),
            disconnect_event: Event::with_tag(),
            // `vec![<expr>; size].into()` clones `<expr>` instead of repeating it
            connected_set: (0 .. size).map(|_i| AtomicBool::new(false)).collect(),
        });

        ConnectionSet {
            slots: (0 .. size).map(|index| Arc::new(Slot {
                global: global.clone(),
                index,
                connection: Arc::new(AsyncMutex::new(None)),
            }))
                .collect(),
            global,
        }
    }

    pub async fn acquire_connected(&self) -> ConnectedSlot<C> {
        self.acquire::<true>().await.assert_connected()
    }

    pub async fn acquire_disconnected(&self) -> DisconnectedSlot<C> {
        self.acquire::<false>().await.assert_disconnected()
    }

    async fn acquire<const CONNECTED: bool>(&self) -> SlotGuard<C> {

    }
}

impl<C> SlotGuard<C> {
    #[inline(always)]
    fn get(&self) -> &Option<C> {
        self.locked.as_ref().expect(EXPECT_LOCKED)
    }

    #[inline(always)]
    fn get_mut(&mut self) -> &mut Option<C> {
        self.locked.as_mut().expect(EXPECT_LOCKED)
    }

    fn assert_connected(self) -> ConnectedSlot<C> {
        assert!(self.get().is_some());

        ConnectedSlot(self)
    }

    fn assert_disconnected(self) -> DisconnectedSlot<C> {
        assert!(self.get().is_none());

        DisconnectedSlot(self)
    }
}

const EXPECT_LOCKED: &str = "BUG: `SlotGuard::locked` should not be `None` in normal operation";
const EXPECT_CONNECTED: &str = "BUG: `ConnectedSlot` expects `Slot::connection` to be `Some`";

impl<C> Deref for ConnectedSlot<C> {
    type Target = C;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.0.get().as_ref().expect(EXPECT_CONNECTED)
    }
}

impl<C> DerefMut for ConnectedSlot<C> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_mut().as_mut().expect(EXPECT_CONNECTED)
    }
}

impl<C> Drop for SlotGuard<C> {
    fn drop(&mut self) {
        // Will be `None` if this drop handler was already run
        // and this is being dropped from within `Event`.
        let Some(slot) = self.slot.take() else {
            return;
        };

        if self.locked.is_none() {
            return;
        }

        let connected = if let Some(locked) = &self.locked {
            locked.is_some()
        } else {
            return;
        };

        // This is a code smell, but it's necessary because `event-listener` has no way to specify
        // that a message should *only* be sent once. This means tags either need to be `Clone`
        // or provided by a `FnMut()` closure.
        //
        // Note that there's no guarantee that this closure won't be called more than once by the
        // implementation, but the code as of writing should not.
        let self_as_tag = || {
            let locked = self.locked
                .take()
                .expect("BUG: notification sent more than once");

            SlotGuard {
                // To avoid infinite recursion or deadlock, don't send another notification
                // if this guard was already dropped once: just unlock it.
                slot: None,
                locked: Some(locked),
            }
        };

        let event = if connected {
            &slot.global.unlock_event
        } else {
            &slot.global.disconnect_event
        };

        event.notify(1.tag_with(self_as_tag));
    }
}

fn current_thread_id() -> usize {
    // FIXME: this can be replaced when this is stabilized:
    // https://doc.rust-lang.org/stable/std/thread/struct.ThreadId.html#method.as_u64
    static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        static CURRENT_THREAD_ID: usize = THREAD_ID.fetch_add(1, Ordering::SeqCst);
    }

    CURRENT_THREAD_ID.with(|i| *i)
}
