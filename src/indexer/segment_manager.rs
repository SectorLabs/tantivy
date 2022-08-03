use std::array::IntoIter;
use std::collections::hash_set::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::segment_register::SegmentRegister;
use crate::core::{SegmentId, SegmentMeta};
use crate::error::TantivyError;
use crate::indexer::delete_queue::DeleteCursor;
use crate::indexer::SegmentEntry;

#[derive(Default)]
struct SegmentRegisters {
    uncommitted: SegmentRegister,
    soft_committed: SegmentRegister,
    committed: SegmentRegister,
}

#[derive(PartialEq, Eq)]
pub(crate) enum SegmentsStatus {
    Committed,
    SoftCommitted,
    Uncommitted,
}

impl SegmentRegisters {
    /// Check if all the segments are committed or uncommited.
    ///
    /// If some segment is missing or segments are in a different state (this should not happen
    /// if tantivy is used correctly), returns `None`.
    fn segments_status(&self, segment_ids: &[SegmentId]) -> Option<SegmentsStatus> {
        if self.uncommitted.contains_all(segment_ids) {
            Some(SegmentsStatus::Uncommitted)
        } else if self.soft_committed.contains_all(segment_ids) {
            Some(SegmentsStatus::SoftCommitted)
        } else if self.committed.contains_all(segment_ids) {
            Some(SegmentsStatus::Committed)
        } else {
            warn!(
                "segment_ids: {:?}, committed_ids: {:?}, uncommitted_ids {:?}",
                segment_ids,
                self.committed.segment_ids(),
                self.uncommitted.segment_ids()
            );
            None
        }
    }

    pub fn iter(&self) -> IntoIter<&SegmentRegister, 3> {
        [&self.committed, &self.soft_committed, &self.uncommitted].into_iter()
    }
}

/// The segment manager stores the list of segments
/// as well as their state.
///
/// It guarantees the atomicity of the
/// changes (merges especially)
#[derive(Default)]
pub struct SegmentManager {
    registers: RwLock<SegmentRegisters>,
}

impl Debug for SegmentManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let lock = self.read();
        write!(
            f,
            "{{ uncommitted: {:?}, committed: {:?} }}",
            lock.uncommitted, lock.committed
        )
    }
}

impl SegmentManager {
    pub fn from_segments(
        segment_metas: Vec<SegmentMeta>,
        delete_cursor: &DeleteCursor,
    ) -> SegmentManager {
        SegmentManager {
            registers: RwLock::new(SegmentRegisters {
                uncommitted: SegmentRegister::default(),
                soft_committed: SegmentRegister::default(),
                committed: SegmentRegister::new(segment_metas, delete_cursor),
            }),
        }
    }

    pub fn get_mergeable_segments(
        &self,
        in_merge_segment_ids: &HashSet<SegmentId>,
    ) -> (Vec<SegmentMeta>, Vec<SegmentMeta>, Vec<SegmentMeta>) {
        let registers_lock = self.read();
        (
            registers_lock
                .committed
                .get_mergeable_segments(in_merge_segment_ids),
            registers_lock
                .soft_committed
                .get_mergeable_segments(in_merge_segment_ids),
            registers_lock
                .uncommitted
                .get_mergeable_segments(in_merge_segment_ids),
        )
    }

    /// Returns all of the segment entries (committed or uncommitted)
    pub fn segment_entries(&self) -> Vec<SegmentEntry> {
        let registers_lock = self.read();
        let mut segment_entries = registers_lock.uncommitted.segment_entries();
        segment_entries.extend(registers_lock.soft_committed.segment_entries());
        segment_entries.extend(registers_lock.committed.segment_entries());
        segment_entries
    }

    pub fn grouped_segment_entries(&self) -> (Vec<SegmentEntry>, Vec<SegmentEntry>, Vec<SegmentEntry>) {
        let registers_lock = self.read();
        (
            registers_lock.committed.segment_entries(),
            registers_lock.soft_committed.segment_entries(),
            registers_lock.uncommitted.segment_entries(),
        )
    }

    // Lock poisoning should never happen :
    // The lock is acquired and released within this class,
    // and the operations cannot panic.
    fn read(&self) -> RwLockReadGuard<'_, SegmentRegisters> {
        self.registers
            .read()
            .expect("Failed to acquire read lock on SegmentManager.")
    }

    fn write(&self) -> RwLockWriteGuard<'_, SegmentRegisters> {
        self.registers
            .write()
            .expect("Failed to acquire write lock on SegmentManager.")
    }

    /// Deletes all empty segments
    fn remove_empty_segments(&self) {
        let mut registers_lock = self.write();
        registers_lock
            .committed
            .segment_entries()
            .iter()
            .filter(|segment| segment.meta().num_docs() == 0)
            .for_each(|segment| {
                registers_lock
                    .committed
                    .remove_segment(&segment.segment_id())
            });
    }

    pub(crate) fn remove_all_segments(&self) {
        let mut registers_lock = self.write();
        registers_lock.committed.clear();
        registers_lock.soft_committed.clear();
        registers_lock.uncommitted.clear();
    }

    pub fn commit(&self, segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock = self.write();
        registers_lock.committed.clear();
        registers_lock.soft_committed.clear();
        registers_lock.uncommitted.clear();
        for segment_entry in segment_entries {
            registers_lock.committed.add_segment_entry(segment_entry);
        }
    }

    pub fn soft_commit(&self, committed_segment_entries: Vec<SegmentEntry>, soft_committed_segment_entries: Vec<SegmentEntry>) {
        let mut registers_lock = self.write();
        registers_lock.soft_committed.clear();
        registers_lock.committed.clear();
        registers_lock.uncommitted.clear();
        for segment_entries in [committed_segment_entries, soft_committed_segment_entries] {
            for segment_entry in segment_entries {
                registers_lock.soft_committed.add_segment_entry(segment_entry);
            }
        }
    }

    /// Marks a list of segments as in merge.
    ///
    /// Returns an error if some segments are missing, or if the `segment_ids`
    /// are not either all committed, all soft committed or all uncommitted.
    pub fn start_merge(&self, segment_ids: &[SegmentId]) -> crate::Result<Vec<SegmentEntry>> {
        let registers_lock = self.read();
        for register in registers_lock.iter() {
            if register.contains_all(segment_ids) {
                let mut segment_entries = vec![];
                for segment_id in segment_ids {
                    let segment_entry = register.get(segment_id).expect(
                        "Segment id not found {}. Should never happen because of the contains all \
                         if-block.",
                    );
                    segment_entries.push(segment_entry);
                }
                return Ok(segment_entries)
            }
        }
        let error_msg = "Merge operation sent for segments that are not all uncommited, \
                         soft committed or commited."
            .to_string();
        return Err(TantivyError::InvalidArgument(error_msg));
    }

    pub fn add_segment(&self, segment_entry: SegmentEntry) {
        let mut registers_lock = self.write();
        registers_lock.uncommitted.add_segment_entry(segment_entry);
    }
    // Replace a list of segments for their equivalent merged segment.
    //
    // Returns true if these segments are committed, false if the merge segments are uncommited.
    pub(crate) fn end_merge(
        &self,
        before_merge_segment_ids: &[SegmentId],
        after_merge_segment_entry: SegmentEntry,
    ) -> crate::Result<SegmentsStatus> {
        let mut registers_lock = self.write();
        let segments_status = registers_lock
            .segments_status(before_merge_segment_ids)
            .ok_or_else(|| {
                warn!("couldn't find segment in SegmentManager");
                crate::TantivyError::InvalidArgument(
                    "The segments that were merged could not be found in the SegmentManager. This \
                     is not necessarily a bug, and can happen after a rollback for instance."
                        .to_string(),
                )
            })?;

        let target_register: &mut SegmentRegister = match segments_status {
            SegmentsStatus::Uncommitted => &mut registers_lock.uncommitted,
            SegmentsStatus::SoftCommitted => &mut registers_lock.soft_committed,
            SegmentsStatus::Committed => &mut registers_lock.committed,
        };
        for segment_id in before_merge_segment_ids {
            target_register.remove_segment(segment_id);
        }
        target_register.add_segment_entry(after_merge_segment_entry);
        Ok(segments_status)
    }

    pub fn committed_segment_metas(&self) -> Vec<SegmentMeta> {
        self.remove_empty_segments();
        let registers_lock = self.read();
        let mut committed = registers_lock.committed.segment_metas();
        committed.append(&mut registers_lock.soft_committed.segment_metas());
        committed
    }
}
