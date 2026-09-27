use crate::{TransportError, range_set::ArrayRangeSet};
use bytes::Bytes;
use std::{
    fmt, mem,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
    },
};

pub(super) const COPY_BLOCK_BYTES: usize = 16 * 1024;

pub(super) const MIN_BUFFER_BYTES: usize = 64 * 1024;

/// Byte budget shared by connections; a refused charge closes only the requesting connection
pub trait SharedBudget: Send + Sync + fmt::Debug {
    /// Charges `bytes` if they fit
    fn try_charge(&self, bytes: usize) -> bool;
    /// Returns previously charged bytes
    fn refund(&self, bytes: usize);
}

#[derive(Debug)]
pub(super) struct BufferBudget {
    limit: AtomicUsize,
    used: AtomicUsize,
    shared: Option<Arc<dyn SharedBudget>>,
    refused: AtomicBool,
}

impl BufferBudget {
    pub(super) fn new(limit: u64, shared: Option<&Arc<dyn SharedBudget>>) -> Arc<Self> {
        Arc::new(Self {
            limit: AtomicUsize::new(
                usize::try_from(limit)
                    .unwrap_or(usize::MAX)
                    .max(MIN_BUFFER_BYTES),
            ),
            used: AtomicUsize::new(0),
            shared: shared.cloned(),
            refused: AtomicBool::new(false),
        })
    }

    pub(super) fn for_receive(window: u64, shared: Option<&Arc<dyn SharedBudget>>) -> Arc<Self> {
        Self::new(window.saturating_mul(3), shared)
    }

    pub(super) fn set_limit(&self, limit: u64) {
        self.limit.store(
            usize::try_from(limit)
                .unwrap_or(usize::MAX)
                .max(MIN_BUFFER_BYTES),
            AtomicOrdering::Relaxed,
        );
    }

    pub(super) fn available(&self) -> usize {
        self.limit
            .load(AtomicOrdering::Relaxed)
            .saturating_sub(self.used())
    }

    pub(super) fn refused(&self) -> bool {
        self.refused.load(AtomicOrdering::Relaxed)
    }

    pub(super) fn used(&self) -> usize {
        self.used.load(AtomicOrdering::Relaxed)
    }

    pub(super) fn acquire(self: &Arc<Self>, bytes: usize) -> Result<Allocation, AllocationError> {
        self.used
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|&next| next <= self.limit.load(AtomicOrdering::Relaxed))
            })
            .map_err(|_| AllocationError)?;
        if let Some(shared) = &self.shared
            && bytes != 0
            && !shared.try_charge(bytes)
        {
            self.used.fetch_sub(bytes, AtomicOrdering::Relaxed);
            self.refused.store(true, AtomicOrdering::Relaxed);
            return Err(AllocationError);
        }
        Ok(Allocation {
            budget: self.clone(),
            bytes,
        })
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, AtomicOrdering::Relaxed);
        if let Some(shared) = &self.shared
            && bytes != 0
        {
            shared.refund(bytes);
        }
    }
}

#[derive(Debug)]
pub(super) struct Allocation {
    pub(super) budget: Arc<BufferBudget>,
    pub(super) bytes: usize,
}

impl Allocation {
    pub(super) fn absorb(&mut self, mut other: Self) {
        assert!(Arc::ptr_eq(&self.budget, &other.budget));
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .expect("budget bounds allocation sum");
        other.bytes = 0;
    }

    pub(super) fn resize(&mut self, bytes: usize) -> Result<(), AllocationError> {
        if bytes > self.bytes {
            let mut extra = self.budget.acquire(bytes - self.bytes)?;
            self.bytes = bytes;
            extra.bytes = 0;
        } else {
            self.budget.release(self.bytes - bytes);
            self.bytes = bytes;
        }
        Ok(())
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[derive(Debug)]
pub(super) struct OwnedBacking {
    pub(super) bytes: Vec<u8>,
    _allocation: Allocation,
}

impl OwnedBacking {
    pub(super) const OVERHEAD_BYTES: usize = mem::size_of::<Self>() + mem::size_of::<AtomicUsize>();
    pub(super) fn new(length: usize, budget: &Arc<BufferBudget>) -> Result<Self, AllocationError> {
        let overhead = if length == 0 { 0 } else { Self::OVERHEAD_BYTES };
        let mut allocation =
            budget.acquire(length.checked_add(overhead).ok_or(AllocationError)?)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| AllocationError)?;
        allocation.resize(
            bytes
                .capacity()
                .checked_add(overhead)
                .ok_or(AllocationError)?,
        )?;
        Ok(Self {
            bytes,
            _allocation: allocation,
        })
    }

    pub(super) fn from_owned(
        bytes: Vec<u8>,
        budget: &Arc<BufferBudget>,
    ) -> Result<Self, AllocationError> {
        let allocation = budget.acquire(
            bytes
                .capacity()
                .checked_add(Self::OVERHEAD_BYTES)
                .ok_or(AllocationError)?,
        )?;
        Ok(Self {
            bytes,
            _allocation: allocation,
        })
    }

    pub(super) fn from_reserved(data: &[u8], capacity: usize, mut allocation: Allocation) -> Self {
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(data);
        allocation
            .resize(bytes.capacity() + Self::OVERHEAD_BYTES)
            .expect("send backing was reserved");
        Self {
            bytes,
            _allocation: allocation,
        }
    }

    pub(super) fn finish(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for OwnedBacking {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
pub(super) struct AllocationError;

impl From<AllocationError> for TransportError {
    fn from(_: AllocationError) -> Self {
        Self::INTERNAL_ERROR("buffer allocation limit")
    }
}

#[derive(Debug)]
pub(super) struct ChargedRanges {
    pub(super) ranges: ArrayRangeSet,
    allocation: Allocation,
}

impl ChargedRanges {
    pub(super) fn new(budget: &Arc<BufferBudget>) -> Self {
        Self {
            ranges: ArrayRangeSet::new(),
            allocation: Allocation {
                budget: budget.clone(),
                bytes: 0,
            },
        }
    }

    pub(super) fn reserve(&mut self, count: usize) -> Result<(), AllocationError> {
        if count <= self.ranges.capacity() {
            return Ok(());
        }
        let capacity = count.max(self.ranges.capacity().saturating_mul(2));
        let mut allocation = self.allocation.budget.acquire(
            capacity
                .checked_mul(mem::size_of::<Range<u64>>())
                .ok_or(AllocationError)?,
        )?;
        let mut ranges = ArrayRangeSet::with_capacity(capacity).map_err(|_| AllocationError)?;
        allocation.resize(ranges.heap_capacity() * mem::size_of::<Range<u64>>())?;
        for range in self.ranges.iter() {
            ranges.insert(range);
        }
        self.ranges = ranges;
        self.allocation = allocation;
        Ok(())
    }

    pub(super) fn try_insert(&mut self, range: Range<u64>) -> Result<(), AllocationError> {
        if !range.is_empty()
            && !self
                .ranges
                .iter()
                .any(|old| old.start <= range.end && old.end >= range.start)
        {
            self.reserve(self.ranges.range_count() + 1)?;
        }
        self.ranges.insert(range);
        Ok(())
    }

    pub(super) fn insert(&mut self, range: Range<u64>) {
        self.ranges.insert(range);
    }
}
