use std::{
    cmp::Ordering,
    collections::{BinaryHeap, binary_heap::PeekMut},
    mem,
    sync::Arc,
};

use bytes::{Buf, Bytes};

#[cfg(test)]
use super::buffer_budget::MIN_BUFFER_BYTES;
use super::buffer_budget::{
    Allocation, AllocationError, BufferBudget, ChargedRanges, OwnedBacking,
};

use super::buffer_budget::COPY_BLOCK_BYTES;

/// Keep only the minimum, fully charged heap between reads on a live stream.
const REUSABLE_BUFFER_CAPACITY: usize = 4;
/// Helper to assemble unordered stream frames into an ordered stream
#[derive(Debug)]
pub(super) struct Assembler {
    state: State,
    data: BinaryHeap<Buffer>,
    allocation: Allocation,
    /// Total number of buffered bytes, including duplicates in ordered mode.
    buffered: usize,
    /// Estimated number of allocated bytes, will never be less than `buffered`.
    allocated: usize,
    /// Number of bytes read by the application. When only ordered reads have been used, this is
    /// the length of the contiguous prefix of the stream which has been consumed by the
    /// application, aka the stream offset.
    bytes_read: u64,
    end: u64,
}

impl Assembler {
    pub(super) fn new(budget: Arc<BufferBudget>) -> Self {
        Self {
            state: State::Ordered,
            data: BinaryHeap::new(),
            allocation: Allocation { budget, bytes: 0 },
            buffered: 0,
            allocated: 0,
            bytes_read: 0,
            end: 0,
        }
    }

    fn reserve(&mut self, count: usize) -> Result<(), AllocationError> {
        if count <= self.data.capacity() {
            return Ok(());
        }
        let capacity = count
            .max(self.data.capacity().saturating_mul(2))
            .max(REUSABLE_BUFFER_CAPACITY);
        let mut allocation = self.allocation.budget.acquire(
            capacity
                .checked_mul(mem::size_of::<Buffer>())
                .ok_or(AllocationError)?,
        )?;
        let mut data = BinaryHeap::new();
        data.try_reserve_exact(capacity)
            .map_err(|_| AllocationError)?;
        allocation.resize(data.capacity() * mem::size_of::<Buffer>())?;
        data.extend(mem::take(&mut self.data));
        self.data = data;
        self.allocation = allocation;
        Ok(())
    }

    pub(super) fn is_ordered(&self) -> bool {
        self.state.is_ordered()
    }

    pub(super) fn ensure_ordering(&mut self, ordered: bool) -> Result<(), OrderingError> {
        if ordered && !self.state.is_ordered() {
            return Err(OrderingError::IllegalOrderedRead);
        } else if !ordered && self.state.is_ordered() {
            // Enter unordered mode
            if !self.data.is_empty() {
                // Get rid of possible duplicates
                self.defragment()?;
            }
            let mut recvd = ChargedRanges::new(&self.allocation.budget);
            recvd.reserve(self.data.len() + 1)?;
            recvd.insert(0..self.bytes_read);
            for chunk in &self.data {
                recvd.insert(chunk.offset..chunk.offset + chunk.bytes.len() as u64);
            }
            let mut delivered = ChargedRanges::new(&self.allocation.budget);
            delivered.reserve(self.data.len() + 1)?;
            delivered.insert(0..self.bytes_read);
            self.state = State::Unordered { recvd, delivered };
        }
        Ok(())
    }

    /// Get the the next chunk
    pub(super) fn read(&mut self, max_length: usize, ordered: bool) -> Option<Chunk> {
        self.read_capped(max_length, ordered, u64::MAX)
    }

    /// Like [`Self::read`], but never returns data at or beyond `offset_limit`.
    ///
    /// Used to deliver only the reliable prefix of a stream subject to a RESET_STREAM_AT. The limit
    /// is applied by stream offset rather than by the read cursor, because in unordered mode the
    /// cursor is a running total of bytes handed out, not the contiguous prefix offset.
    pub(super) fn read_capped(
        &mut self,
        max_length: usize,
        ordered: bool,
        offset_limit: u64,
    ) -> Option<Chunk> {
        loop {
            let mut chunk = self.data.peek_mut()?;

            if ordered {
                if chunk.offset > self.bytes_read {
                    // Next chunk is after current read index
                    return None;
                } else if (chunk.offset + chunk.bytes.len() as u64) <= self.bytes_read {
                    // Next chunk is useless as the read index is beyond its end
                    self.buffered -= chunk.bytes.len();
                    self.allocated -= chunk.allocation_size;
                    PeekMut::pop(chunk);
                    continue;
                }

                // Determine `start` and `len` of the slice of useful data in chunk
                let start = (self.bytes_read - chunk.offset) as usize;
                if start > 0 {
                    chunk.bytes.advance(start);
                    chunk.offset += start as u64;
                    self.buffered -= start;
                }
            }

            // Never hand out data at or beyond the offset limit, and never let a single chunk
            // straddle it.
            if chunk.offset >= offset_limit {
                return None;
            }
            let max_length = max_length.min((offset_limit - chunk.offset) as usize);

            let chunk = if max_length < chunk.bytes.len() {
                self.bytes_read += max_length as u64;
                let offset = chunk.offset;
                chunk.offset += max_length as u64;
                self.buffered -= max_length;
                let bytes = chunk.bytes.split_to(max_length);
                drop(chunk);
                Chunk::new(offset, bytes)
            } else {
                self.bytes_read += chunk.bytes.len() as u64;
                self.buffered -= chunk.bytes.len();
                self.allocated -= chunk.allocation_size;
                let chunk = PeekMut::pop(chunk);
                Chunk::new(chunk.offset, chunk.bytes)
            };
            if let State::Unordered { delivered, .. } = &mut self.state {
                delivered.insert(chunk.offset..chunk.offset + chunk.bytes.len() as u64);
            }
            // A small stream commonly drains between packets. Reuse its charged metadata,
            // but release larger heaps rather than retaining their high-water capacity.
            if self.data.is_empty() && self.data.capacity() > REUSABLE_BUFFER_CAPACITY {
                self.data = BinaryHeap::new();
                self.allocation
                    .resize(0)
                    .expect("releasing reassembly allocation");
            }
            return Some(chunk);
        }
    }

    fn defragment(&mut self) -> Result<(), AllocationError> {
        let mut buffers = mem::take(&mut self.data).into_vec();
        buffers.sort_unstable_by(|left, right| right.cmp(left));
        self.buffered = 0;
        let mut fragmented_buffered = 0;
        let mut offset = if self.state.is_ordered() {
            self.bytes_read
        } else {
            0
        };
        for chunk in &mut buffers {
            chunk.try_mark_defragment(offset);
            let size = chunk.bytes.len();
            offset = chunk.offset + size as u64;
            self.buffered += size;
            if !chunk.defragmented {
                fragmented_buffered += size;
            }
        }
        buffers.retain(|chunk| !chunk.bytes.is_empty());
        self.allocated = self.buffered;
        let original = buffers.len();
        let capacity = original * 2 + fragmented_buffered.div_ceil(COPY_BLOCK_BYTES);
        if capacity > buffers.capacity() {
            let mut allocation = self.allocation.budget.acquire(
                capacity
                    .checked_mul(mem::size_of::<Buffer>())
                    .ok_or(AllocationError)?,
            )?;
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(capacity)
                .map_err(|_| AllocationError)?;
            allocation.resize(replacement.capacity() * mem::size_of::<Buffer>())?;
            replacement.extend(buffers);
            buffers = replacement;
            self.allocation = allocation;
        }
        let mut index = 0;
        while index < original {
            if buffers[index].defragmented {
                let chunk = mem::replace(
                    &mut buffers[index],
                    Buffer::new_defragmented(0, Bytes::new()),
                );
                buffers.push(chunk);
                index += 1;
                continue;
            }
            let mut offset = buffers[index].offset;
            let mut remaining = buffers[index].bytes.len();
            let mut end = index + 1;
            while end < original
                && !buffers[end].defragmented
                && buffers[end].offset == offset + remaining as u64
            {
                remaining += buffers[end].bytes.len();
                end += 1;
            }
            let mut length = remaining.min(COPY_BLOCK_BYTES);
            let mut buffer = OwnedBacking::new(length, &self.allocation.budget)?;
            for source in index..end {
                let mut bytes = mem::take(&mut buffers[source].bytes);
                while !bytes.is_empty() {
                    let take = bytes.len().min(length - buffer.bytes.len());
                    buffer.bytes.extend_from_slice(&bytes[..take]);
                    bytes.advance(take);
                    if buffer.bytes.len() == length {
                        buffers.push(Buffer::new_defragmented(offset, buffer.finish()));
                        offset += length as u64;
                        remaining -= length;
                        length = remaining.min(COPY_BLOCK_BYTES);
                        buffer = OwnedBacking::new(length, &self.allocation.budget)?;
                    }
                }
            }
            index = end;
        }
        buffers.drain(..original);
        self.data = BinaryHeap::from(buffers);
        Ok(())
    }

    // The incoming packet size bounds the frame, but each inserted frame gets its own charged
    // backing. Slices of that backing can still conservatively count its footprint more than once.
    pub(super) fn insert(
        &mut self,
        mut offset: u64,
        mut bytes: Bytes,
        allocation_size: usize,
    ) -> Result<(), AllocationError> {
        debug_assert!(
            bytes.len() <= allocation_size,
            "allocation_size less than bytes.len(): {:?} < {:?}",
            allocation_size,
            bytes.len()
        );
        if bytes.is_empty()
            || offset + bytes.len() as u64 <= self.bytes_read && self.state.is_ordered()
        {
            return Ok(());
        }
        let fragments = match &self.state {
            State::Ordered => 1,
            State::Unordered { recvd, .. } => {
                recvd
                    .ranges
                    .iter_range(offset..offset + bytes.len() as u64)
                    .count()
                    + 1
            }
        };
        if self.reserve(self.data.len() + fragments).is_err() {
            self.defragment()?;
            self.reserve(self.data.len() + fragments)?;
        }
        if let State::Unordered { recvd, delivered } = &mut self.state {
            recvd.reserve(recvd.ranges.range_count() + 1)?;
            delivered.reserve(delivered.ranges.range_count() + self.data.len() + fragments)?;
        }
        let mut backing = OwnedBacking::new(bytes.len(), &self.allocation.budget)?;
        backing.bytes.extend_from_slice(&bytes);
        let allocation_size = backing.allocation_size();
        bytes = backing.finish();
        self.end = self.end.max(offset + bytes.len() as u64);
        if let State::Unordered { ref mut recvd, .. } = self.state {
            // Discard duplicate data
            let range = offset..offset + bytes.len() as u64;
            for duplicate in recvd.ranges.iter_range(range.clone()) {
                if duplicate.start > offset {
                    let buffer = Buffer::new(
                        offset,
                        bytes.split_to((duplicate.start - offset) as usize),
                        allocation_size,
                    );
                    self.buffered += buffer.bytes.len();
                    self.allocated += buffer.allocation_size;
                    self.data.push(buffer);
                    offset = duplicate.start;
                }
                bytes.advance((duplicate.end - offset) as usize);
                offset = duplicate.end;
            }
            recvd.insert(range);
        } else if offset < self.bytes_read {
            if (offset + bytes.len() as u64) <= self.bytes_read {
                return Ok(());
            } else {
                let diff = self.bytes_read - offset;
                offset += diff;
                bytes.advance(diff as usize);
            }
        }

        if bytes.is_empty() {
            return Ok(());
        }
        let buffer = Buffer::new(offset, bytes, allocation_size);
        self.buffered += buffer.bytes.len();
        self.allocated += buffer.allocation_size;
        self.data.push(buffer);
        // `self.buffered` also counts duplicate bytes, therefore we use
        // `self.end - self.bytes_read` as an upper bound of buffered unique
        // bytes. This will cause a defragmentation if the amount of duplicate
        // bytes exceedes a proportion of the receive window size.
        let buffered = self.buffered.min((self.end - self.bytes_read) as usize);
        let over_allocation = self.allocated - buffered;
        // Rationale: on the one hand, we want to defragment rarely, ideally never
        // in non-pathological scenarios. However, a pathological or malicious
        // peer could send us one-byte frames, and since we use reference-counted
        // buffers in order to prevent copying, this could result in keeping a lot
        // of memory allocated. This limits over-allocation in proportion to the
        // buffered data. The constants are chosen somewhat arbitrarily and try to
        // balance between defragmentation overhead and over-allocation.
        let threshold = 32768.max(buffered * 3 / 2);
        if over_allocation > threshold {
            self.defragment()?;
        }
        Ok(())
    }

    /// Number of bytes consumed by the application
    pub(super) fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Contiguous bytes delivered from offset zero. Unordered reads can consume a
    /// later chunk without making the reliable prefix complete.
    pub(super) fn delivered_prefix(&self) -> u64 {
        match &self.state {
            State::Ordered => self.bytes_read,
            State::Unordered { delivered, .. } => delivered
                .ranges
                .iter()
                .next()
                .filter(|range| range.start == 0)
                .map_or(0, |range| range.end),
        }
    }

    /// Number of bytes already handed to the application within an offset range.
    pub(super) fn delivered_within(&self, start: u64, end: u64) -> u64 {
        if start >= end {
            return 0;
        }
        match &self.state {
            State::Ordered => self.bytes_read.min(end).saturating_sub(start),
            State::Unordered { delivered, .. } => delivered
                .ranges
                .iter_range(start..end)
                .map(|range| range.end - range.start)
                .sum(),
        }
    }

    /// Discard all buffered data
    pub(super) fn clear(&mut self) {
        self.data = BinaryHeap::new();
        self.allocation
            .resize(0)
            .expect("releasing reassembly allocation");
        self.buffered = 0;
        self.allocated = 0;
    }
}

/// A chunk of data from the receive stream
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    /// The offset in the stream
    pub offset: u64,
    /// The contents of the chunk
    pub bytes: Bytes,
}

impl Chunk {
    fn new(offset: u64, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }
}

#[derive(Debug, Eq)]
struct Buffer {
    offset: u64,
    bytes: Bytes,
    /// Charged backing footprint, including its owner, if `defragmented == false`.
    /// Otherwise this will be set to `bytes.len()` by `try_mark_defragment`.
    /// Will never be less than `bytes.len()`.
    allocation_size: usize,
    defragmented: bool,
}

impl Buffer {
    /// Constructs a new fragmented Buffer
    fn new(offset: u64, bytes: Bytes, allocation_size: usize) -> Self {
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: false,
        }
    }

    /// Constructs a new defragmented Buffer
    fn new_defragmented(offset: u64, bytes: Bytes) -> Self {
        let allocation_size = bytes.len();
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: true,
        }
    }

    /// Discards data before `offset` and flags `self` as defragmented if it has good utilization
    fn try_mark_defragment(&mut self, offset: u64) {
        let duplicate = offset.saturating_sub(self.offset) as usize;
        self.offset = self.offset.max(offset);
        if duplicate >= self.bytes.len() {
            // All bytes are duplicate
            self.bytes = Bytes::new();
            self.defragmented = true;
            self.allocation_size = 0;
            return;
        }
        self.bytes.advance(duplicate);
        // Make sure that fragmented buffers with high utilization become defragmented and
        // defragmented buffers remain defragmented
        self.defragmented = (self.defragmented || self.bytes.len() * 6 / 5 >= self.allocation_size)
            && self.allocation_size <= COPY_BLOCK_BYTES;
        if self.defragmented {
            // Make sure that defragmented buffers do not contribute to over-allocation
            self.allocation_size = self.bytes.len();
        }
    }
}

impl Ord for Buffer {
    // Invert ordering based on offset (max-heap, min offset first),
    // prioritize longer chunks at the same offset.
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset
            .cmp(&other.offset)
            .reverse()
            .then(self.bytes.len().cmp(&other.bytes.len()))
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        (self.offset, self.bytes.len()) == (other.offset, other.bytes.len())
    }
}

#[derive(Debug)]
enum State {
    Ordered,
    Unordered {
        /// The set of offsets that have been received from the peer, including portions not yet
        /// read by the application.
        recvd: ChargedRanges,
        /// Data already handed to the application, tracked by stream offset.
        delivered: ChargedRanges,
    },
}

impl State {
    fn is_ordered(&self) -> bool {
        matches!(self, Self::Ordered)
    }
}

/// Error indicating that an ordered read was performed on a stream after an unordered read
#[derive(Debug)]
pub(super) enum OrderingError {
    IllegalOrderedRead,
    Allocation(AllocationError),
}

impl From<AllocationError> for OrderingError {
    fn from(error: AllocationError) -> Self {
        Self::Allocation(error)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn copied_backing_estimate_excludes_discarded_packet_storage() {
        let budget = BufferBudget::for_receive(1024 * 1024, None);
        let mut assembler = Assembler::new(budget.clone());
        // Several frames can share a much larger packet before each gets copied.
        for frame in 0..40 {
            assembler
                .insert(frame * 1024, Bytes::from(vec![7; 1024]), 65535)
                .unwrap();
        }
        // No artificial fragmentation or compaction from the discarded packet allocation.
        assert_eq!(assembler.data.len(), 40);
        assert_eq!(assembler.allocated, 40 * (1024 + OwnedBacking::OVERHEAD_BYTES));
        for frame in 0..40 {
            let chunk = assembler.read(usize::MAX, true).unwrap();
            assert_eq!(chunk.offset, frame * 1024);
            assert_eq!(chunk.bytes.as_ref(), &[7; 1024]);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn tiny_owned_backings_still_trigger_compaction() {
        let budget = BufferBudget::for_receive(1024 * 1024, None);
        let mut assembler = Assembler::new(budget.clone());
        for offset in 0..700 {
            assembler.insert(offset, Bytes::from_static(b"a"), 1).unwrap();
        }
        // Capacity alone would miss the owner overhead of hundreds of one-byte frames.
        assert!(assembler.data.len() < 700);
        assert!(assembler.data.iter().any(|buffer| buffer.bytes.len() > 1));
        let mut delivered = 0;
        while let Some(chunk) = assembler.read(usize::MAX, true) {
            assert_eq!(chunk.offset, delivered);
            assert!(chunk.bytes.iter().all(|&byte| byte == b'a'));
            delivered += chunk.bytes.len() as u64;
        }
        assert_eq!(delivered, 700);
        assembler.clear();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn small_heap_reuses_charged_storage_and_clear_refunds_it() {
        let budget = BufferBudget::for_receive(1, None);
        let mut assembler = Assembler::new(budget.clone());
        assembler.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        let capacity = assembler.data.capacity();
        let storage = assembler.data.as_slice().as_ptr();
        assert!(capacity <= REUSABLE_BUFFER_CAPACITY);
        let retained = capacity * mem::size_of::<Buffer>();

        for offset in 0..3 {
            if offset != 0 {
                assembler.insert(offset, Bytes::from_static(b"a"), 1).unwrap();
            }
            let chunk = assembler.read(usize::MAX, true).unwrap();
            assert_eq!(chunk.offset, offset);
            assert_eq!(chunk.bytes.as_ref(), b"a");
            drop(chunk);
            assert!(assembler.data.is_empty());
            assert_eq!(assembler.data.as_slice().as_ptr(), storage);
            assert_eq!(budget.used(), retained);
        }

        // Retained metadata remains charged: exhaustion still refuses new backing.
        let remaining = budget.acquire(budget.available()).unwrap();
        assert!(assembler.insert(3, Bytes::from_static(b"b"), 1).is_err());
        assert!(assembler.data.is_empty());
        assert_eq!(assembler.data.as_slice().as_ptr(), storage);
        drop(remaining);
        assembler.insert(3, Bytes::from_static(b"b"), 1).unwrap();
        let delivered = assembler.read(usize::MAX, true).unwrap();
        assembler.clear();
        assert_eq!(assembler.data.capacity(), 0);
        assert_eq!(budget.used(), 1 + OwnedBacking::OVERHEAD_BYTES);
        drop(delivered);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn drained_large_heap_releases_storage() {
        let budget = BufferBudget::for_receive(1, None);
        let mut assembler = Assembler::new(budget.clone());
        for offset in 0..5 {
            assembler.insert(offset, Bytes::from_static(b"a"), 1).unwrap();
        }
        assert!(assembler.data.capacity() > REUSABLE_BUFFER_CAPACITY);
        for offset in 0..5 {
            let chunk = assembler.read(usize::MAX, true).unwrap();
            assert_eq!(chunk.offset, offset);
            assert_eq!(chunk.bytes.as_ref(), b"a");
        }
        assert_eq!(assembler.data.capacity(), 0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn stream_drop_refunds_reused_metadata_but_not_delivered_backing() {
        let budget = BufferBudget::for_receive(1, None);
        let mut assembler = Assembler::new(budget.clone());
        assembler.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        let delivered = assembler.read(usize::MAX, true).unwrap();
        assert!(assembler.allocation.bytes != 0);
        drop(assembler);
        assert_eq!(budget.used(), 1 + OwnedBacking::OVERHEAD_BYTES);
        drop(delivered);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn defragment_releases_large_backing_before_partial_reads() {
        struct Owner {
            bytes: Vec<u8>,
            alive: Arc<AtomicUsize>,
        }
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                self.alive.fetch_sub(1, AtomicOrdering::Relaxed);
            }
        }
        let alive = Arc::new(AtomicUsize::new(1));
        let length = COPY_BLOCK_BYTES * 2 + 1;
        let bytes = Bytes::from_owner(Owner {
            bytes: vec![7; length],
            alive: alive.clone(),
        });
        let budget = BufferBudget::for_receive(1024 * 1024, None);
        let mut assembler = Assembler::new(budget.clone());
        assembler.insert(0, bytes, length).unwrap();
        assembler.defragment().unwrap();
        assert_eq!(alive.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(assembler.data.len(), 3);
        assert!(
            assembler
                .data
                .iter()
                .all(|chunk| chunk.allocation_size <= COPY_BLOCK_BYTES)
        );
        let first = assembler.read(COPY_BLOCK_BYTES - 1, true).unwrap();
        assert_eq!(first.offset, 0);
        assert_eq!(
            assembler.read(1, true).unwrap().offset,
            (COPY_BLOCK_BYTES - 1) as u64
        );
        assert_eq!(
            assembler.read(usize::MAX, true).unwrap().bytes.len(),
            COPY_BLOCK_BYTES
        );
        let tail = assembler.read(usize::MAX, true).unwrap();
        assert_eq!(tail.offset, (COPY_BLOCK_BYTES * 2) as u64);
        assert_eq!(tail.bytes.as_ref(), &[7]);
        assembler.clear();
        assert!(budget.used() > 0);
        assert_eq!(first.bytes.len(), COPY_BLOCK_BYTES - 1);
        let retained = first.bytes.slice(first.bytes.len() - 1..);
        drop(first);
        drop(tail);
        assert_eq!(
            budget.used(),
            COPY_BLOCK_BYTES + mem::size_of::<OwnedBacking>() + mem::size_of::<AtomicUsize>()
        );
        drop(retained);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn delivered_backing_keeps_shared_budget_until_last_clone() {
        let budget = BufferBudget::for_receive(1, None);
        let length = MIN_BUFFER_BYTES * 3 / 4;
        let mut assembler = Assembler::new(budget.clone());
        assembler
            .insert(0, Bytes::from(vec![7; length]), length)
            .unwrap();
        let delivered = assembler.read(usize::MAX, true).unwrap().bytes;
        let tail = delivered.slice(length - 1..);
        drop(delivered);
        assembler = Assembler::new(assembler.allocation.budget.clone());
        assert_eq!(
            budget.used(),
            length + mem::size_of::<OwnedBacking>() + mem::size_of::<AtomicUsize>()
        );
        assert!(
            assembler
                .insert(0, Bytes::from(vec![8; length]), length)
                .is_err()
        );
        drop(tail);
        assembler
            .insert(0, Bytes::from(vec![8; length]), length)
            .unwrap();
        assert_eq!(
            assembler.read(usize::MAX, true).unwrap().bytes.as_ref(),
            vec![8; length]
        );
        assembler.clear();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn defragment_charges_old_and_new_backing_together() {
        let budget = BufferBudget::for_receive(1, None);
        let length = MIN_BUFFER_BYTES / 2 + 1;
        let mut assembler = Assembler::new(budget.clone());
        assembler
            .insert(0, Bytes::from(vec![7; length]), length * 2)
            .unwrap();
        assert!(assembler.defragment().is_err());
        assert!(budget.used() <= MIN_BUFFER_BYTES);
        assembler.clear();
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn assemble_ordered() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 1), Some(ref y) if &y[..] == b"1");
        assert_matches!(next(&mut x, 3), Some(ref y) if &y[..] == b"23");
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        x.insert(6, Bytes::from_static(b"789"), 3).unwrap();
        x.insert(9, Bytes::from_static(b"10"), 2).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"789");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"10");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_unordered() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate_compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.defragment().unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained_compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.defragment().unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains_compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
        x.defragment().unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"4");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping_compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"123"), 4).unwrap();
        x.insert(1, Bytes::from_static(b"234"), 4).unwrap();
        x.defragment().unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex_compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
        x.defragment().unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_old() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn compact() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(9, Bytes::from_static(b"jkl"), 4).unwrap();
        x.insert(12, Bytes::from_static(b"mno"), 4).unwrap();
        x.defragment().unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abcdef"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jklmno"))
        );
    }

    #[test]
    fn defrag_with_missing_prefix() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        x.defragment().unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
    }

    #[test]
    fn defrag_read_chunk() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
        x.insert(7, Bytes::from_static(b"hij"), 4).unwrap();
        x.insert(11, Bytes::from_static(b"lmn"), 4).unwrap();
        x.defragment().unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"abcdef");
        x.insert(5, Bytes::from_static(b"fghijklmn"), 9).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"ghijklmn");
        x.insert(13, Bytes::from_static(b"nopq"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"opq");
        x.insert(15, Bytes::from_static(b"pqrs"), 4).unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"rs");
        assert_matches!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn unordered_happy_path() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.ensure_ordering(false).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abc"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn unordered_dedup() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"abcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"a"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(1, Bytes::from_static(b"bc"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(6, Bytes::from_static(b"ghi"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jkl"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(12, Bytes::from_static(b"mno"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn chunks_dedup() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(1, Bytes::from_static(b"bcdefghi"), 9).unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abcd")))
        );
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(4, Bytes::from_static(b"efghi")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(9, Bytes::from_static(b"jkl")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(12, Bytes::from_static(b"mno")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn ordered_eager_discard() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        assert_eq!(x.data.len(), 1);
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        x.insert(0, Bytes::from_static(b"ab"), 2).unwrap();
        assert_eq!(x.data.len(), 0);
        x.insert(2, Bytes::from_static(b"cd"), 2).unwrap();
        assert_eq!(
            x.data.peek(),
            Some(&Buffer::new(3, Bytes::from_static(b"d"), 2))
        );
    }

    #[test]
    fn ordered_insert_unordered_read() {
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
        x.ensure_ordering(false).unwrap();
        assert_eq!(
            x.read(3, false),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        assert_eq!(x.read(3, false), None);
    }

    #[test]
    fn no_duplicate_after_mode_switch() {
        // Regression test: bytes read in ordered mode should not be returned again in unordered
        // mode
        let mut x = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
        x.insert(0, Bytes::from_static(b"a"), 1).unwrap(); // duplicate
        assert_eq!(
            x.read(1, true),
            Some(Chunk::new(0, Bytes::from_static(b"a")))
        );
        x.ensure_ordering(false).unwrap();
        assert_eq!(x.read(1, false), None); // should be None, byte 0 already returned
    }

    fn next_unordered(x: &mut Assembler) -> Chunk {
        x.read(usize::MAX, false).unwrap()
    }

    fn next(x: &mut Assembler, size: usize) -> Option<Bytes> {
        x.read(size, true).map(|chunk| chunk.bytes)
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod proptests {
    use proptest::prelude::*;
    use rand::RngExt;
    use test_strategy::{Arbitrary, proptest};

    use super::*;

    const MAX_OFFSET: u64 = 512;
    const MAX_LEN: usize = 64;

    #[derive(Debug, Clone, Arbitrary)]
    enum Op {
        #[weight(10)]
        Insert {
            #[strategy(0..MAX_OFFSET)]
            offset: u64,
            #[strategy(1..MAX_LEN)]
            len: usize,
        },
        #[weight(10)]
        Read {
            #[strategy(1..MAX_LEN)]
            max_len: usize,
        },
        #[weight(1)]
        EnsureOrdering { ordered: bool },
        #[weight(1)]
        Defragment,
    }

    /// Tracks the state of the assembler for verification
    struct RefState {
        received: Vec<bool>,
        returned: Vec<bool>,
        ordered: bool,
    }

    fn set_range(bits: &mut [bool], start: u64, len: usize) {
        for i in start..(start + len as u64).min(bits.len() as u64) {
            bits[i as usize] = true;
        }
    }

    impl RefState {
        fn new() -> Self {
            Self {
                received: vec![false; MAX_OFFSET as usize],
                returned: vec![false; MAX_OFFSET as usize],
                ordered: true,
            }
        }

        fn insert(&mut self, offset: u64, len: usize) {
            set_range(&mut self.received, offset, len);
        }

        fn ensure_ordering(&mut self, ordered: bool) -> bool {
            if ordered && !self.ordered {
                return false;
            }
            self.ordered = ordered;
            true
        }

        fn bytes_read(&self) -> u64 {
            self.returned.iter().filter(|&&x| x).count() as u64
        }
    }

    fn make_data() -> Vec<u8> {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xDEADBEEF);
        let mut data = vec![0u8; MAX_OFFSET as usize];
        rng.fill(data.as_mut_slice());
        data
    }

    fn get_slice(data: &[u8], offset: u64, len: usize) -> Bytes {
        let start = offset as usize;
        let end = (start + len).min(data.len());
        Bytes::copy_from_slice(&data[start..end])
    }

    fn verify_chunk(data: &[u8], chunk: &Chunk) -> bool {
        let start = chunk.offset as usize;
        chunk.bytes[..] == data[start..start + chunk.bytes.len()]
    }

    #[proptest]
    fn assembler_matches_reference(
        #[strategy(proptest::collection::vec(any::<Op>(), 1..100))] ops: Vec<Op>,
    ) {
        let data = make_data();
        let mut asm = Assembler::new(BufferBudget::for_receive(1024 * 1024, None));
        let mut reference = RefState::new();

        for op in ops {
            match op {
                Op::Insert { offset, len } => {
                    let bytes = get_slice(&data, offset, len);
                    asm.insert(offset, bytes, len).unwrap();
                    reference.insert(offset, len);
                }
                Op::Read { max_len } => {
                    let ordered = reference.ordered;
                    let actual = asm.read(max_len, ordered);

                    match actual {
                        None => {
                            // Should only be None if no unreturned received bytes available
                            let has_available = if ordered {
                                // In ordered mode, check if the first unreturned byte is received
                                reference
                                    .returned
                                    .iter()
                                    .position(|&x| !x)
                                    .is_some_and(|pos| reference.received[pos])
                            } else {
                                // In unordered mode, check if any unreturned received byte exists
                                reference
                                    .received
                                    .iter()
                                    .zip(&reference.returned)
                                    .any(|(&r, &ret)| r && !ret)
                            };
                            prop_assert!(
                                !has_available,
                                "read returned None but data was available"
                            );
                        }
                        Some(chunk) => {
                            prop_assert!(chunk.bytes.len() <= max_len, "chunk exceeds max_len");
                            prop_assert!(verify_chunk(&data, &chunk), "data corruption");
                            // Mark as returned, check for duplicates
                            for i in 0..chunk.bytes.len() {
                                let offset = chunk.offset as usize + i;
                                prop_assert!(
                                    reference.received[offset],
                                    "returned unreceived byte at {offset}"
                                );
                                prop_assert!(
                                    !reference.returned[offset],
                                    "duplicate byte at {offset}"
                                );
                                reference.returned[offset] = true;
                            }
                        }
                    }
                }
                Op::EnsureOrdering { ordered } => {
                    let actual = asm.ensure_ordering(ordered).is_ok();
                    let expected = reference.ensure_ordering(ordered);
                    prop_assert_eq!(actual, expected, "ensure_ordering result mismatch");
                }
                Op::Defragment => {
                    if asm.state.is_ordered() {
                        asm.defragment().unwrap();
                    }
                }
            }
        }

        // Invariant: bytes_read matches
        prop_assert_eq!(
            asm.bytes_read(),
            reference.bytes_read(),
            "bytes_read mismatch"
        );
    }
}
