use std::{collections::BTreeMap, mem, ops::RangeBounds, sync::Arc};

use super::buffer_budget::{Allocation, AllocationError, BufferBudget};

#[derive(Debug)]
pub(super) struct PacketMap<T> {
    entries: BTreeMap<u64, T>,
    allocation: Allocation,
}

impl<T> PacketMap<T> {
    fn entry_lease() -> usize {
        // Rust 1.98.1 B-tree nodes have 11 entries, five leaf fields, and 12 edges.
        let alignment = mem::align_of::<T>().max(mem::align_of::<u64>());
        mem::size_of::<usize>()
            + 2 * mem::size_of::<u16>()
            + 11 * (mem::size_of::<u64>() + mem::size_of::<T>())
            + 5 * (alignment - 1)
            + 12 * mem::size_of::<usize>()
    }

    pub(super) fn new(budget: Arc<BufferBudget>) -> Self {
        Self {
            entries: BTreeMap::new(),
            allocation: Allocation { budget, bytes: 0 },
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(super) fn admission_blocked(&self) -> bool {
        self.allocation
            .budget
            .available()
            .saturating_add(self.allocation.bytes)
            < (self.entries.len() + 1) * Self::entry_lease()
    }

    pub(super) fn reserve_entry(&mut self) -> Result<(), AllocationError> {
        self.allocation.resize(
            (self.entries.len() + 1)
                .checked_mul(Self::entry_lease())
                .ok_or(AllocationError)?,
        )
    }

    pub(super) fn insert(&mut self, key: u64, value: T) -> Result<(), AllocationError> {
        if !self.entries.contains_key(&key) {
            self.reserve_entry()?;
        }
        self.entries.insert(key, value);
        Ok(())
    }

    fn refund(&mut self) {
        if self.entries.is_empty() {
            self.entries = BTreeMap::new();
        }
        self.allocation
            .resize(self.entries.len() * Self::entry_lease())
            .expect("releasing packet nodes");
    }

    pub(super) fn remove(&mut self, key: u64) -> Option<T> {
        let value = self.entries.remove(&key);
        self.refund();
        value
    }

    pub(super) fn get(&self, key: u64) -> Option<&T> {
        self.entries.get(&key)
    }

    pub(super) fn iter_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> impl DoubleEndedIterator<Item = (u64, &T)> {
        self.entries.range(range).map(|(&key, value)| (key, value))
    }

    pub(super) fn keys_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> impl DoubleEndedIterator<Item = u64> {
        self.entries.range(range).map(|(&key, _)| key)
    }

    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = (u64, &T)> {
        self.entries.iter().map(|(&key, value)| (key, value))
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.values()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries.values_mut()
    }

    pub(super) fn retain(&mut self, mut retain: impl FnMut(u64, &mut T) -> bool) {
        self.entries.retain(|&key, value| retain(key, value));
        self.refund();
    }

    pub(super) fn take(&mut self) -> Self {
        mem::replace(self, Self::new(self.allocation.budget.clone()))
    }

    pub(super) fn into_iter(self) -> impl Iterator<Item = (u64, T)> {
        let allocation = self.allocation;
        self.entries.into_iter().inspect(move |_| {
            let _ = &allocation;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_packet_numbers_and_consuming_owner() {
        let budget = BufferBudget::new(64 * 1024);
        let mut packets = PacketMap::new(budget.clone());
        for number in 0..40 {
            packets.insert(number * 1_000_000, [7_u8; 32]).unwrap();
        }
        let leased = budget.used();
        let mut remaining = packets.take().into_iter();
        assert!(packets.is_empty());
        assert_eq!(remaining.next().unwrap().0, 0);
        assert_eq!(budget.used(), leased);
        drop(remaining);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn node_admission_survives_split_and_empty_root() {
        let budget = BufferBudget::new(64 * 1024);
        let mut packets = PacketMap::new(budget.clone());
        let mut end = 0;
        while packets.insert(end, [0_u8; 256]).is_ok() {
            end += 1;
        }
        assert!(end > 11);
        assert!(budget.used() <= 64 * 1024);
        for number in (0..end).rev() {
            assert!(packets.remove(number).is_some());
        }
        assert_eq!(budget.used(), 0);
        packets.insert(u64::MAX, [1_u8; 256]).unwrap();
        assert!(packets.remove(u64::MAX).is_some());
        assert_eq!(budget.used(), 0);
    }
}
