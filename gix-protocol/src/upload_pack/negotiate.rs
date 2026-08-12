//! Fetch negotiation state and logic.
//!
//! This module separates negotiation state tracking from response serialization,
//! providing a clear decision path for acknowledgement generation and readiness.

use std::collections::BTreeSet;

use bstr::ByteSlice;
use gix_ref::file::ReferenceExt as _;

use crate::fetch::response::{Acknowledgement, WantedRef};
use super::{Fetch, FetchNegotiation, FetchNegotiationError};

/// Accumulated negotiation state for a single fetch round.
///
/// Tracks acknowledged haves (deduplicated) and protocol flags to determine
/// the correct acknowledgement response and readiness for pack transfer.
#[allow(dead_code)] // `wait_for_done` reserved for V1 multi-round and future state machine use.
pub(crate) struct NegotiationState {
    /// Deduplicated set of acknowledged have IDs.
    common_haves: BTreeSet<gix_hash::ObjectId>,
    /// Ordered list for response (preserves first-seen order).
    common_haves_ordered: Vec<gix_hash::ObjectId>,
    /// Whether the client sent `done`.
    done: bool,
    /// Whether `wait-for-done` is active.
    wait_for_done: bool,
}

impl NegotiationState {
    /// Create a new negotiation state initialized from the request's flags.
    pub fn new(request: &Fetch) -> Self {
        Self {
            common_haves: BTreeSet::new(),
            common_haves_ordered: Vec::new(),
            done: request.done,
            wait_for_done: request.wait_for_done,
        }
    }

    /// Record a have that exists in the repository. Deduplicates automatically.
    pub fn acknowledge_have(&mut self, id: gix_hash::ObjectId) {
        if self.common_haves.insert(id) {
            self.common_haves_ordered.push(id);
        }
    }

    /// Generate the acknowledgement list based on current state.
    ///
    /// Truth table:
    /// | done  | common_haves.is_empty() | Result                            |
    /// |-------|-------------------------|-----------------------------------|
    /// | true  | true                    | [] (omit ack section entirely)    |
    /// | true  | false                   | [Common(...), ..., Ready]         |
    /// | done  | common_haves.is_empty() | Result                            |
    /// |-------|-------------------------|-----------------------------------|
    /// | true  | any                     | [] (MUST omit per spec)           |
    /// | false | true                    | [NAK]                             |
    /// | false | false                   | [Common(...), ...]  (no Ready)    |
    ///
    /// Per gitprotocol-v2: "If the client determines that it is finished with
    /// negotiations by sending a 'done' line [...], the acknowledgments section
    /// MUST be omitted from the server's response."
    pub fn acknowledgements(&self) -> Vec<Acknowledgement> {
        if self.done {
            // Spec requires omitting the acknowledgments section entirely when done=true.
            // The server proceeds directly to packfile (or shallow-info/wanted-refs).
            Vec::new()
        } else if self.common_haves_ordered.is_empty() {
            vec![Acknowledgement::Nak]
        } else {
            self.common_haves_ordered
                .iter()
                .copied()
                .map(Acknowledgement::Common)
                .collect()
        }
    }

    /// Returns true iff `done` is true — the server should send a pack.
    ///
    /// In V2, readiness always requires an explicit `done` from the client.
    /// The `wait_for_done` flag reinforces this (it prevents early readiness
    /// in potential future multi-round scenarios), but in current V2 semantics
    /// readiness is simply `done == true`.
    #[allow(dead_code)] // Used by state machine (task 5.x) and property tests (task 4.4).
    pub fn is_ready(&self) -> bool {
        self.done
    }

    /// Evaluate the full negotiation against repository state.
    ///
    /// This processes haves, wants, and want-refs from the request, using
    /// the provided ref store and object existence predicate.
    pub fn evaluate(
        mut self,
        request: &Fetch,
        refs: &gix_ref::file::Store,
        mut object_exists: impl FnMut(&gix_hash::oid) -> bool,
    ) -> Result<FetchNegotiation, FetchNegotiationError> {
        // Process haves: acknowledge those that exist in the repository.
        for have in &request.haves {
            if object_exists(have) {
                self.acknowledge_have(*have);
            }
        }

        // Process wants: partition into known (exist) and missing.
        let mut known_wants = Vec::new();
        let mut missing_wants = Vec::new();
        let mut seen_known_wants = BTreeSet::new();
        let mut seen_missing_wants = BTreeSet::new();
        for want in &request.wants {
            if object_exists(want) {
                if seen_known_wants.insert(*want) {
                    known_wants.push(*want);
                }
            } else if seen_missing_wants.insert(*want) {
                missing_wants.push(*want);
            }
        }

        // Process want-refs: resolve ref names to object IDs.
        let packed = refs.cached_packed_buffer()?;
        let packed = packed.as_ref().map(|buffer| &***buffer);

        let mut wanted_refs = Vec::new();
        let mut unresolved_want_refs = Vec::new();
        let mut seen_resolved_wants = BTreeSet::new();
        let mut seen_unresolved_wants = BTreeSet::new();
        for requested_ref in &request.want_refs {
            if seen_resolved_wants.contains(requested_ref) || seen_unresolved_wants.contains(requested_ref) {
                continue;
            }

            let partial_name: &gix_ref::PartialNameRef = match requested_ref.as_bstr().try_into() {
                Ok(name) => name,
                Err(_) => {
                    if seen_unresolved_wants.insert(requested_ref.clone()) {
                        unresolved_want_refs.push(requested_ref.clone());
                    }
                    continue;
                }
            };

            match refs.find_packed(partial_name, packed) {
                Ok(mut reference) => {
                    let id = reference.follow_to_object_packed(refs, packed).map_err(|source| {
                        FetchNegotiationError::ResolveWantedRef {
                            ref_name: requested_ref.clone(),
                            source,
                        }
                    })?;
                    if seen_resolved_wants.insert(requested_ref.clone()) {
                        wanted_refs.push(WantedRef {
                            id,
                            path: requested_ref.clone(),
                        });
                    }
                }
                Err(gix_ref::file::find::existing::Error::NotFound { .. }) => {
                    if seen_unresolved_wants.insert(requested_ref.clone()) {
                        unresolved_want_refs.push(requested_ref.clone());
                    }
                }
                Err(source) => {
                    return Err(FetchNegotiationError::FindWantedRef {
                        ref_name: requested_ref.clone(),
                        source,
                    });
                }
            }
        }

        let acknowledgements = self.acknowledgements();
        let common_haves = self.common_haves_ordered;

        Ok(FetchNegotiation {
            acknowledgements,
            wanted_refs,
            known_wants,
            missing_wants,
            common_haves,
            unresolved_want_refs,
        })
    }
}
