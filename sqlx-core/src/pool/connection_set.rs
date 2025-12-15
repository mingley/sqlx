use crate::ext::future::race;
use crate::rt;
use crate::sync::{AsyncMutex, AsyncMutexGuardArc};
use event_listener::{listener, Event, IntoNotification};
use futures_core::Stream;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use std::cmp;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

pub struct ConnectionSet<C> {
    global: Arc<Global>,
    slots: Box<[Arc<Slot<C>>]>,
}

pub struct ConnectedSlot<C>(SlotGuard<C>);

pub struct DisconnectedSlot<C>(SlotGuard<C>);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AcquirePreference {
    Connected,
    Disconnected,
    Either,
}

struct Global {
    unlock_event: Event<usize>,
    disconnect_event: Event<usize>,
    num_connected: AtomicUsize,
}

struct SlotGuard<C> {
    slot: Arc<Slot<C>>,
    // `Option` allows us to take the guard in the drop handler.
    locked: Option<AsyncMutexGuardArc<Option<C>>>,
}

struct Slot<C> {
    // By having each `Slot` hold its own reference to `Global`, we can avoid extra contended clones
    // which would sap performance
    global: Arc<Global>,
    index: usize,
    // I'd love to eliminate this redundant `Arc` but it's likely not possible without `unsafe`
    connection: Arc<AsyncMutex<Option<C>>>,
    unlock_event: Event,
    disconnect_event: Event,
    connected: AtomicBool,
    locked: AtomicBool,
}

impl<C> ConnectionSet<C> {
    pub fn new(size: usize) -> Self {
        let global = Arc::new(Global {
            unlock_event: Event::with_tag(),
            disconnect_event: Event::with_tag(),
            num_connected: AtomicUsize::new(0),
        });

        ConnectionSet {
            // `vec![<expr>; size].into()` clones `<expr>` instead of repeating it,
            // which is *no bueno* when wrapping something in `Arc`
            slots: (0..size)
                .map(|index| {
                    Arc::new(Slot {
                        global: global.clone(),
                        index,
                        connection: Arc::new(AsyncMutex::new(None)),
                        unlock_event: Event::with_tag(),
                        disconnect_event: Event::with_tag(),
                        connected: AtomicBool::new(false),
                        locked: AtomicBool::new(false),
                    })
                })
                .collect(),
            global,
        }
    }

    #[inline(always)]
    pub fn num_connected(&self) -> usize {
        self.global.num_connected.load(Ordering::Relaxed)
    }

    pub fn count_idle(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_locked()).count()
    }

    pub async fn acquire_connected(&self) -> ConnectedSlot<C> {
        self.acquire_inner(AcquirePreference::Connected)
            .await
            .assert_connected()
    }

    pub async fn acquire_disconnected(&self) -> DisconnectedSlot<C> {
        self.acquire_inner(AcquirePreference::Disconnected)
            .await
            .assert_disconnected()
    }

    /// Attempt to acquire the connection associated with the current thread.
    pub async fn acquire_any(
        &self,
        pref: AcquirePreference,
    ) -> Result<ConnectedSlot<C>, DisconnectedSlot<C>> {
        self.acquire_inner(pref).await.try_connected()
    }

    async fn acquire_inner(&self, pref: AcquirePreference) -> SlotGuard<C> {
        const LARGE_PRIME: usize = 547;

        /// Smallest time-step supported by [`tokio::time::sleep()`].
        ///
        /// `async-io` doesn't document a minimum time-step, instead deferring to the platform.
        const STEP_INTERVAL: Duration = Duration::from_millis(1);

        const SEARCH_LIMIT: usize = 5;

        let preferred_slot = current_thread_id() % self.slots.len();

        // Always try to lock the connection associated with our thread ID
        let mut acquire_preferred = pin!(self.slots[preferred_slot].acquire(pref));

        let mut step_interval = pin!(rt::interval_after(STEP_INTERVAL));

        let mut intervals_elapsed = 0usize;

        let mut search_slots = FuturesUnordered::new();

        let mut listen_global = pin!(self.global.listen(pref));

        // By adding a large number that is coprime to `slots.len()` before taking the modulo,
        // we can visit each slot in a pseudo-random order.
        let mut next_slot = (preferred_slot + LARGE_PRIME) % self.slots.len();

        std::future::poll_fn(|cx| loop {
            if let Poll::Ready(locked) = acquire_preferred.as_mut().poll(cx) {
                return Poll::Ready(locked);
            }

            // Don't push redundant futures for small sets.
            let search_limit = cmp::min(SEARCH_LIMIT, self.slots.len());

            if search_slots.len() < search_limit && step_interval.as_mut().poll_tick(cx).is_ready()
            {
                intervals_elapsed = intervals_elapsed.saturating_add(1);

                if next_slot != preferred_slot && self.slots[next_slot].matches_pref(pref) {
                    search_slots.push(self.slots[next_slot].lock());
                }

                next_slot = (next_slot + LARGE_PRIME) % self.slots.len();
            }

            if let Poll::Ready(Some(locked)) = Pin::new(&mut search_slots).poll_next(cx) {
                if locked.matches_pref(pref) {
                    return Poll::Ready(locked);
                }

                continue;
            }

            if intervals_elapsed > search_limit && search_slots.len() < search_limit {
                if let Poll::Ready(slot) = listen_global.as_mut().poll(cx) {
                    if self.slots[slot].matches_pref(pref) {
                        search_slots.push(self.slots[slot].lock());
                    }

                    listen_global.as_mut().set(self.global.listen(pref));
                }

                continue;
            }

            return Poll::Pending;
        })
        .await
    }

    pub async fn drain(&self, ref close: impl AsyncFn(ConnectedSlot<C>) -> DisconnectedSlot<C>) {
        let mut closing = FuturesUnordered::new();

        // We could try to be more efficient by only populating the `FuturesUnordered` for
        // connected slots, but then we'd have to handle a disconnected slot becoming connected,
        // which could happen concurrently.
        //
        // However, we don't *need* to be efficient when shutting down the pool.
        for slot in &self.slots {
            closing.push(async {
                let locked = slot.lock().await;

                let DisconnectedSlot(mut guard) = match locked.try_connected() {
                    Ok(connected) => close(connected).await,
                    Err(disconnected) => disconnected,
                };

                // The pool is shutting down; don't wake any tasks that might have been interested
                guard.drop_without_notify();
            });
        }

        while closing.next().await.is_some() {}
    }
}

impl AcquirePreference {
    #[inline(always)]
    fn wants_connected(&self) -> bool {
        matches!(self, Self::Connected | Self::Either)
    }
}

impl<C> Slot<C> {
    #[inline(always)]
    fn matches_pref(&self, pref: AcquirePreference) -> bool {
        self.is_connected() == pref.wants_connected()
    }

    #[inline(always)]
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_is_connected(&self, connected: bool) {
        let was_connected = self.connected.swap(connected, Ordering::Acquire);

        match (connected, was_connected) {
            (false, true) => {
                // Ensure this is synchronized with `connected`
                self.global.num_connected.fetch_add(1, Ordering::Release);
            }
            (true, false) => {
                self.global.num_connected.fetch_sub(1, Ordering::Release);
            }
            _ => (),
        }
    }

    async fn acquire(self: &Arc<Self>, pref: AcquirePreference) -> SlotGuard<C> {
        loop {
            if self.matches_pref(pref) {
                let locked = self.lock().await;

                if locked.matches_pref(pref) {
                    return locked;
                }
            }

            let event = if pref.wants_connected() {
                &self.unlock_event
            } else {
                &self.disconnect_event
            };

            listener!(event => listener);
            listener.await;
        }
    }

    async fn lock(self: &Arc<Self>) -> SlotGuard<C> {
        let locked = crate::sync::lock_arc(&self.connection).await;

        self.locked.store(true, Ordering::Relaxed);

        SlotGuard {
            slot: self.clone(),
            locked: Some(locked),
        }
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

    #[inline(always)]
    fn matches_pref(&self, pref: AcquirePreference) -> bool {
        self.is_connected() == pref.wants_connected()
    }

    #[inline(always)]
    fn is_connected(&self) -> bool {
        self.get().is_some()
    }

    fn try_connected(self) -> Result<ConnectedSlot<C>, DisconnectedSlot<C>> {
        if self.is_connected() {
            Ok(ConnectedSlot(self))
        } else {
            Err(DisconnectedSlot(self))
        }
    }

    fn assert_connected(self) -> ConnectedSlot<C> {
        assert!(self.is_connected());
        ConnectedSlot(self)
    }

    fn assert_disconnected(self) -> DisconnectedSlot<C> {
        assert!(!self.is_connected());

        DisconnectedSlot(self)
    }

    /// Updates `Slot::connected` without notifying the `ConnectionSet`.
    ///
    /// Returns `Some(connected)` or `None` if this guard was already dropped.
    fn drop_without_notify(&mut self) -> Option<bool> {
        self.locked.take().map(|locked| {
            let connected = locked.is_some();
            self.slot.set_is_connected(connected);
            self.slot.locked.store(false, Ordering::Release);
            connected
        })
    }
}

const EXPECT_LOCKED: &str = "BUG: `SlotGuard::locked` should not be `None` in normal operation";
const EXPECT_CONNECTED: &str = "BUG: `ConnectedSlot` expects `Slot::connection` to be `Some`";

impl<C> ConnectedSlot<C> {
    pub fn take(mut self) -> (C, DisconnectedSlot<C>) {
        let conn = self.0.get_mut().take().expect(EXPECT_CONNECTED);
        (conn, self.0.assert_disconnected())
    }
}

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

impl<C> DisconnectedSlot<C> {
    pub fn put(mut self, conn: C) -> ConnectedSlot<C> {
        *self.0.get_mut() = Some(conn);
        ConnectedSlot(self.0)
    }
}

impl<C> Drop for SlotGuard<C> {
    fn drop(&mut self) {
        let Some(connected) = self.drop_without_notify() else {
            return;
        };

        let event = if connected {
            &self.slot.global.unlock_event
        } else {
            &self.slot.global.disconnect_event
        };

        if event.notify(1.tag(self.slot.index).additional()) != 0 {
            return;
        }

        let event = if connected {
            &self.slot.unlock_event
        } else {
            &self.slot.disconnect_event
        };

        event.notify(1);
    }
}

impl Global {
    async fn listen(&self, pref: AcquirePreference) -> usize {
        match pref {
            AcquirePreference::Either => race(self.listen_unlocked(), self.listen_disconnected())
                .await
                .unwrap_or_else(|slot| slot),
            AcquirePreference::Connected => self.listen_unlocked().await,
            AcquirePreference::Disconnected => self.listen_disconnected().await,
        }
    }

    async fn listen_unlocked(&self) -> usize {
        listener!(self.unlock_event => listener);
        listener.await
    }

    async fn listen_disconnected(&self) -> usize {
        listener!(self.disconnect_event => listener);
        listener.await
    }
}

fn current_thread_id() -> usize {
    // FIXME: this can be replaced when this is stabilized:
    // https://doc.rust-lang.org/stable/std/thread/struct.ThreadId.html#method.as_u64
    static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

    thread_local! {
        // `SeqCst` is possibly too strong since we don't need synchronization with
        // any other variable. I'm not confident enough in my understanding of atomics to be certain,
        // especially with regards to weakly ordered architectures.
        //
        // However, this is literally only done once on each thread, so it doesn't really matter.
        static CURRENT_THREAD_ID: usize = THREAD_ID.fetch_add(1, Ordering::SeqCst);
    }

    CURRENT_THREAD_ID.with(|i| *i)
}
