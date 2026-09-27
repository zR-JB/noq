use alloc::{collections::VecDeque, sync::Arc};
use core::mem;

use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, trace};

use super::{
    Connection,
    buffer_budget::{Allocation, AllocationError, BufferBudget, OwnedBacking},
};
use crate::{
    FrameStats, TransportError,
    connection::PacketBuilder,
    frame::{Datagram, FrameStruct},
};

/// API to control datagram traffic
pub struct Datagrams<'a> {
    pub(super) conn: &'a mut Connection,
}

impl Datagrams<'_> {
    /// Queue an unreliable, unordered datagram for immediate transmission
    ///
    /// If `drop` is true, previously queued datagrams which are still unsent may be discarded to
    /// make space for this datagram, in order of oldest to newest. If `drop` is false, and there
    /// isn't enough space due to previously queued datagrams, this function will return
    /// `SendDatagramError::Blocked`. `Event::DatagramsUnblocked` will be emitted once datagrams
    /// have been sent.
    ///
    /// Returns `Err` iff a `len`-byte datagram cannot currently be sent.
    pub fn send(&mut self, data: Bytes, drop: bool) -> Result<(), SendDatagramError> {
        if self.conn.config.datagram_receive_buffer_size.is_none() {
            return Err(SendDatagramError::Disabled);
        }
        let max = self
            .max_size()
            .ok_or(SendDatagramError::UnsupportedByPeer)?;
        let send_buffer_size = self.conn.config.datagram_send_buffer_size;
        if data.len() > max
            || data
                .len()
                .checked_add(size_of::<Datagram>())
                .is_none_or(|size| size > send_buffer_size)
        {
            return Err(SendDatagramError::TooLarge);
        }
        if self
            .conn
            .datagrams
            .queue_send(&data, drop, send_buffer_size)
            .is_err()
        {
            self.conn.datagrams.block_send(data.len());
            return Err(SendDatagramError::Blocked(data));
        }
        Ok(())
    }

    /// Queue many unreliable, unordered datagrams for transmission in a single call.
    ///
    /// This is the batch analogue of [`Self::send`], avoiding repeated connection checks.
    ///
    /// The batch is rejected atomically with the [`TooLarge`] error if any datagram
    /// in the batch is too large.
    ///
    /// `drop` selects the backpressure behaviour, matching [`Self::send`]:
    ///
    /// - `drop = true` drops the oldest queued datagrams to make room, so every element is queued
    ///   and `Ok(datagrams.len())` is returned.
    /// - `drop = false` queues elements until the send buffer is full, then stops and returns
    ///   `Ok(n)` for the `n` elements queued. The remaining elements are the caller's to retry once
    ///   space frees up.
    ///
    /// Returns `Err` if datagrams are unsupported by the peer or disabled locally.
    ///
    /// [`TooLarge`]: SendDatagramError::TooLarge
    pub fn send_many(
        &mut self,
        datagrams: &[Bytes],
        drop: bool,
    ) -> Result<usize, SendDatagramError> {
        if self.conn.config.datagram_receive_buffer_size.is_none() {
            return Err(SendDatagramError::Disabled);
        }
        let max = self
            .max_size()
            .ok_or(SendDatagramError::UnsupportedByPeer)?;
        let send_buffer_size = self.conn.config.datagram_send_buffer_size;
        if datagrams.iter().any(|data| {
            data.len() > max
                || data
                    .len()
                    .checked_add(size_of::<Datagram>())
                    .is_none_or(|size| size > send_buffer_size)
        }) {
            return Err(SendDatagramError::TooLarge);
        }

        let mut queued = 0usize;
        for data in datagrams {
            if self
                .conn
                .datagrams
                .queue_send(data, drop, send_buffer_size)
                .is_err()
            {
                self.conn.datagrams.block_send(data.len());
                break;
            }
            queued += 1;
        }

        Ok(queued)
    }

    /// Compute the maximum size of datagrams that may be passed to `send_datagram`
    ///
    /// Returns `None` if datagrams are unsupported by the peer or disabled locally.
    ///
    /// This may change over the lifetime of a connection according to variation in the path MTU
    /// estimate. The peer can also enforce an arbitrarily small fixed limit, but if the peer's
    /// limit is large this is guaranteed to be a little over a kilobyte at minimum.
    ///
    /// Not necessarily the maximum size of received datagrams.
    ///
    /// When multipath is enabled, this is calculated using the smallest MTU across all
    /// available paths.
    pub fn max_size(&self) -> Option<usize> {
        // We use the conservative overhead bound for any packet number, reducing the budget by at
        // most 3 bytes, so that PN size fluctuations don't cause users sending maximum-size
        // datagrams to suffer avoidable packet loss.
        let max_size = self.conn.current_mtu() as usize
            - self.conn.predict_1rtt_overhead_no_pn()
            - Datagram::SIZE_BOUND;
        let limit = self
            .conn
            .peer_params
            .max_datagram_frame_size?
            .into_inner()
            .saturating_sub(Datagram::SIZE_BOUND as u64);
        Some(limit.min(max_size as u64) as usize)
    }

    /// Receive an unreliable, unordered datagram
    pub fn recv(&mut self) -> Option<Bytes> {
        self.conn.datagrams.recv()
    }

    /// Drain up to `out.len()` buffered datagrams into `out`, in arrival order.
    ///
    /// This is the batch analogue of [`Self::recv`]: a single call takes many
    /// datagrams at once. `out` is filled from the front and overwritten in place;
    /// pass a slice of empty `Bytes` sized to the batch you want. Returns the number
    /// of datagrams written, which may be less than `out.len()` if fewer are buffered
    /// (0 if none). Any remaining datagrams stay queued for the next call.
    pub fn recv_many(&mut self, out: &mut [Bytes]) -> usize {
        self.conn.datagrams.recv_many(out)
    }

    /// Bytes available in the outgoing datagram buffer
    ///
    /// When greater than zero, [`send`](Self::send)ing a datagram of at most this size is
    /// guaranteed not to cause older datagrams to be dropped.
    pub fn send_buffer_space(&self) -> usize {
        self.conn
            .config
            .datagram_send_buffer_size
            .saturating_sub(self.conn.datagrams.outgoing.memory_used())
            .saturating_sub(size_of::<Datagram>())
            .min(
                self.conn
                    .datagrams
                    .outgoing
                    .allocation
                    .budget
                    .available()
                    .saturating_sub(self.conn.datagrams.outgoing.growth_bytes())
                    .saturating_sub(OwnedBacking::OVERHEAD_BYTES),
            )
    }
}

pub(super) struct DatagramState {
    pub(super) incoming: DatagramBuffer,
    pub(super) outgoing: DatagramBuffer,
    blocked_send_bytes: Option<usize>,
}

impl DatagramState {
    pub(super) fn new(receive: Arc<BufferBudget>, send: Arc<BufferBudget>) -> Self {
        Self {
            incoming: DatagramBuffer::new(receive),
            outgoing: DatagramBuffer::new(send),
            blocked_send_bytes: None,
        }
    }

    fn block_send(&mut self, length: usize) {
        self.blocked_send_bytes = Some(
            self.blocked_send_bytes
                .map_or(length, |old| old.min(length)),
        );
    }

    pub(super) fn poll_unblocked(&mut self, window: usize) -> bool {
        let Some(length) = self.blocked_send_bytes else {
            return false;
        };
        let required = length
            .saturating_add(if length == 0 {
                0
            } else {
                OwnedBacking::OVERHEAD_BYTES
            })
            .saturating_add(self.outgoing.growth_bytes());
        if !self.has_send_buffer_space(length, window)
            || self.outgoing.allocation.budget.available() < required
        {
            return false;
        }
        self.blocked_send_bytes = None;
        true
    }

    fn queue_send(
        &mut self,
        data: &[u8],
        drop: bool,
        window: usize,
    ) -> Result<(), AllocationError> {
        if drop {
            self.make_space_for(data.len(), window);
        } else if !self.has_send_buffer_space(data.len(), window) {
            return Err(AllocationError);
        }
        loop {
            let Err(error) = self.outgoing.push_copy(data) else {
                return Ok(());
            };
            self.outgoing.release_if_empty();
            if !drop {
                return Err(error);
            }
            let Some(old) = self.outgoing.pop_front() else {
                return Err(error);
            };
            mem::drop(old);
            self.outgoing.release_if_empty();
        }
    }

    pub(super) fn received(
        &mut self,
        datagram: Datagram,
        window: &Option<usize>,
    ) -> Result<bool, TransportError> {
        let window = match window {
            None => {
                return Err(TransportError::PROTOCOL_VIOLATION(
                    "unexpected DATAGRAM frame",
                ));
            }
            Some(x) => *x,
        };

        let size_with_overhead = datagram.data.len() + size_of::<Datagram>();

        if size_with_overhead > window {
            return Err(TransportError::PROTOCOL_VIOLATION("oversized datagram"));
        }

        let was_empty = self.incoming.is_empty();
        while self.incoming.memory_used() + size_with_overhead > window {
            debug!("dropping stale datagram");
            self.recv();
        }

        if self.incoming.push_copy(&datagram.data).is_err() {
            self.incoming.release_if_empty();
            return Ok(false);
        }
        Ok(was_empty)
    }

    fn make_space_for(&mut self, datagram_len: usize, send_buffer_size: usize) {
        while !self.has_send_buffer_space(datagram_len, send_buffer_size) {
            let Some(prev) = self.outgoing.pop_front() else {
                break;
            };
            trace!(len = prev.data.len(), "dropping outgoing datagram");
        }
        self.outgoing.release_if_empty();
    }

    fn has_send_buffer_space(&self, datagram_len: usize, send_buffer_size: usize) -> bool {
        // The new datagram's queue entry counts too
        let Some(total) = self
            .outgoing
            .memory_used()
            .checked_add(datagram_len)
            .and_then(|size| size.checked_add(size_of::<Datagram>()))
        else {
            return false;
        };

        total <= send_buffer_size
    }

    /// Discard outgoing datagrams with a payload larger than `max_payload` bytes
    ///
    /// Returns whether any datagrams were dropped.
    ///
    /// Used to ensure that reductions in MTU don't get us stuck in a state where we have a datagram
    /// queued but can't send it.
    pub(super) fn drop_oversized(&mut self, max_payload: usize) -> bool {
        let mut dropped_any = false;
        self.outgoing.queue.retain(|datagram| {
            let result = datagram.data.len() < max_payload;
            if !result {
                trace!(
                    "dropping {} byte datagram violating {} byte limit",
                    datagram.data.len(),
                    max_payload
                );
                self.outgoing.payload_bytes -= datagram.data.len();
                dropped_any = true;
            }
            result
        });
        self.outgoing.release_if_empty();
        dropped_any
    }

    /// Attempt to write a datagram frame into `buf`, consuming it from `self.outgoing`
    ///
    /// Returns whether a frame was written. At most `max_size` bytes will be written, including
    /// framing.
    pub(super) fn write<'a, 'b>(
        &mut self,
        buf: &mut PacketBuilder<'a, 'b>,
        stat: &mut FrameStats,
    ) -> bool {
        let Some(datagram) = self.outgoing.pop_front() else {
            return false;
        };

        if buf.frame_space_remaining() < datagram.size(true) {
            // Future work: we could be more clever about cramming small datagrams into
            // mostly-full packets when a larger one is queued first
            self.outgoing.push_front(datagram);
            return false;
        }

        self.outgoing.release_if_empty();
        buf.write_frame(datagram, stat);
        true
    }

    pub(super) fn recv(&mut self) -> Option<Bytes> {
        let x = self.incoming.pop_front()?.data;
        self.incoming.release_if_empty();
        Some(x)
    }

    /// Drain up to `out.len()` buffered datagrams into `out`, in arrival order.
    ///
    /// Returns the number of datagrams written into `out` (which may be less than
    /// `out.len()` if fewer are buffered). Remaining datagrams stay queued.
    pub(super) fn recv_many(&mut self, out: &mut [Bytes]) -> usize {
        let n = out.len().min(self.incoming.len());
        for (i, d) in self.incoming.drain_front(n).enumerate() {
            out[i] = d.data;
        }
        self.incoming.release_if_empty();
        n
    }
}

pub(super) struct DatagramBuffer {
    queue: VecDeque<Datagram>,
    payload_bytes: usize,
    /// Charges the queue's capacity; each payload's backing stays charged until its last owner
    /// drops it
    allocation: Allocation,
}

impl DatagramBuffer {
    fn new(budget: Arc<BufferBudget>) -> Self {
        Self {
            queue: VecDeque::new(),
            payload_bytes: 0,
            allocation: Allocation { budget, bytes: 0 },
        }
    }

    /// Queues a charged copy of `data`, so it cannot pin a packet buffer
    fn push_copy(&mut self, data: &[u8]) -> Result<(), AllocationError> {
        let growth = self.growth_bytes();
        if growth != 0 {
            let mut replacement = self.allocation.budget.acquire(growth)?;
            let mut entries = VecDeque::new();
            entries
                .try_reserve_exact(growth / size_of::<Datagram>())
                .map_err(|_| AllocationError)?;
            replacement.resize(entries.capacity() * size_of::<Datagram>())?;
            entries.extend(mem::take(&mut self.queue));
            self.queue = entries;
            self.allocation = replacement;
        }
        let data = if data.is_empty() {
            Bytes::new()
        } else {
            let mut backing = OwnedBacking::new(data.len(), &self.allocation.budget)?;
            backing.bytes.extend_from_slice(data);
            backing.finish()
        };
        self.payload_bytes += data.len();
        self.queue.push_back(Datagram { data });
        Ok(())
    }

    /// Bytes a push must charge to grow the queue
    fn growth_bytes(&self) -> usize {
        if self.queue.len() < self.queue.capacity() {
            return 0;
        }
        self.queue
            .len()
            .saturating_add(1)
            .max(self.queue.capacity().saturating_mul(2))
            .max(4)
            .saturating_mul(size_of::<Datagram>())
    }

    fn release_if_empty(&mut self) {
        if self.queue.is_empty() {
            self.queue = VecDeque::new();
            self.allocation.resize(0).expect("releasing datagram queue");
        }
    }

    fn pop_front(&mut self) -> Option<Datagram> {
        let datagram = self.queue.pop_front()?;
        self.payload_bytes -= datagram.data.len();
        Some(datagram)
    }

    fn push_front(&mut self, datagram: Datagram) {
        self.payload_bytes += datagram.data.len();
        self.queue.push_front(datagram);
    }

    fn memory_used(&self) -> usize {
        self.payload_bytes
            .saturating_add(self.queue.len() * size_of::<Datagram>())
    }

    pub(super) fn can_send_1rtt(&self, max_size: usize) -> bool {
        self.queue.front().is_some_and(|x| x.size(true) <= max_size)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    /// Removes and returns the first `n` datagrams, in arrival order.
    fn drain_front(&mut self, n: usize) -> impl Iterator<Item = Datagram> + '_ {
        let payload_bytes = &mut self.payload_bytes;
        self.queue.drain(..n).inspect(|datagram| {
            *payload_bytes -= datagram.data.len();
        })
    }
}

#[cfg(test)]
impl Default for DatagramState {
    fn default() -> Self {
        Self::new(
            BufferBudget::new(u64::MAX, None),
            BufferBudget::new(u64::MAX, None),
        )
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use alloc::vec;

    use super::*;

    #[test]
    fn delivered_datagrams_keep_aggregate_backing_credit() {
        let budget = BufferBudget::new(64 * 1024, None);
        let mut state = DatagramState::new(budget.clone(), BufferBudget::new(64 * 1024, None));
        let source = Bytes::from(vec![7; 1024 * 1024]);
        let mut retained = Vec::new();
        for _ in 0..100 {
            let accepted = state
                .received(
                    Datagram {
                        data: source.slice(..1024),
                    },
                    &Some(128 * 1024),
                )
                .unwrap();
            if !accepted {
                break;
            }
            retained.push(state.recv().unwrap());
            assert!(budget.used() <= 64 * 1024);
        }
        assert!(!retained.is_empty() && retained.len() < 100);
        assert!(state.incoming.is_empty());
        assert!(budget.used() > 0);
        drop(retained);
        assert_eq!(budget.used(), 0);
        assert!(
            state
                .received(
                    Datagram {
                        data: source.slice(..1)
                    },
                    &Some(128 * 1024)
                )
                .unwrap()
        );
        let tiny = state.recv().unwrap();
        assert!(budget.used() < 512);
        drop(tiny);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn make_space_for_accounts_for_new_datagram() {
        let mut state = DatagramState::default();
        state.outgoing.push_copy(&[0; 7]).unwrap();
        state.outgoing.push_copy(&[0; 2]).unwrap();

        state.make_space_for(4, 10 + 2 * size_of::<Datagram>());

        assert_eq!(state.outgoing.queue.len(), 1);
        assert_eq!(state.outgoing.queue[0].data.len(), 2);
        assert_eq!(state.outgoing.payload_bytes, 2);
    }

    #[test]
    fn make_space_for_handles_overflowing_capacity_check() {
        let mut state = DatagramState::default();
        state.outgoing.queue.push_back(Datagram {
            data: Bytes::from_static(&[0]),
        });
        state.outgoing.payload_bytes = usize::MAX - 1;

        state.make_space_for(2, usize::MAX);

        assert!(state.outgoing.is_empty());
        assert_eq!(state.outgoing.payload_bytes, usize::MAX - 2);
    }

    #[test]
    fn empty_frame_flood_limit() {
        let mut state = DatagramState::default();
        let datagram = Datagram { data: Bytes::new() };
        let window = 100;
        loop {
            let initial_count = state.incoming.queue.len();
            state.received(datagram.clone(), &Some(window)).unwrap();
            assert!(state.incoming.queue.len() * size_of::<Datagram>() <= window);
            if state.incoming.queue.len() == initial_count {
                // Datagrams are getting dropped
                break;
            }
        }
    }
}

/// Errors that can arise when sending a datagram
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SendDatagramError {
    /// The peer does not support receiving datagram frames
    #[error("datagrams not supported by peer")]
    UnsupportedByPeer,
    /// Datagram support is disabled locally
    #[error("datagram support disabled")]
    Disabled,
    /// The datagram is larger than the connection can currently accommodate
    ///
    /// Indicates that the path MTU minus overhead or the limit advertised by the peer has been
    /// exceeded.
    #[error("datagram too large")]
    TooLarge,
    /// Send would block
    #[error("datagram send blocked")]
    Blocked(Bytes),
}
