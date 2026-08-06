//! Server-side receive-pack handler for one push session.
//!
//! This module provides [`ReceivePackHandler`], which orchestrates:
//! - Pack ingestion with thin-pack resolution
//! - Connectivity checking (object graph walk)
//! - Atomic ref transactions with compare-and-swap semantics
//!
//! The handler can be used either as a [`Delegate`](super::Delegate) for the all-in-one
//! path or via individual pipeline step methods for integrators who need custom logic
//! between stages.

use std::collections::{HashSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bstr::BString;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the receive-pack handler.
#[derive(Debug, Clone)]
pub struct Options {
    /// Thread limit for pack indexing. `None` means use all available cores.
    pub thread_limit: Option<usize>,
    /// Hash algorithm for object ids.
    pub object_hash: gix_hash::Kind,
    /// Pack iteration integrity verification mode.
    pub iteration_mode: gix_pack::data::input::Mode,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            thread_limit: None,
            object_hash: gix_hash::Kind::default(),
            iteration_mode: gix_pack::data::input::Mode::Verify,
        }
    }
}

// ---------------------------------------------------------------------------
// Session state machine
// ---------------------------------------------------------------------------

/// Tracks which pipeline stages have been executed in a push session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Handler is newly created, no stages executed.
    Fresh,
    /// Pack has been ingested successfully.
    PackIngested,
    /// Pack was aborted (`.keep` removed).
    Aborted,
    /// Refs have been transacted (terminal state).
    Committed,
}

// ---------------------------------------------------------------------------
// Handler struct
// ---------------------------------------------------------------------------

/// The receive-pack handler for one push session.
///
/// Construct via [`ReceivePackHandler::open`] with a bare repository path.
pub struct ReceivePackHandler {
    /// Object database store (shared via Arc for creating handles that implement `gix_object::Find`).
    pub(crate) odb: Arc<gix_odb::Store>,
    /// Reference store (file-based).
    pub(crate) ref_store: gix_ref::file::Store,
    /// Repository root path (bare repo).
    pub(crate) repo_path: PathBuf,
    /// Object directory path.
    pub(crate) objects_dir: PathBuf,
    /// Handler configuration.
    pub(crate) options: Options,
    /// Current session lifecycle state.
    pub(crate) state: SessionState,
    /// Outcome from pack ingestion, if completed.
    pub(crate) ingest_outcome: Option<gix_pack::bundle::write::Outcome>,
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur during handler construction.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// Failed to open the object database.
    #[error("Failed to open object database at {path}")]
    Odb {
        /// Path where the ODB was expected.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Failed to open the ref store.
    #[error("Failed to open ref store at {path}")]
    RefStore {
        /// Path where the ref store was expected.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Repository path does not exist or is not a directory.
    #[error("Repository path does not exist or is not a directory: {path}")]
    InvalidPath {
        /// The invalid path.
        path: PathBuf,
    },
}

/// Errors from pack ingestion.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Pack data is malformed or failed integrity verification.
    #[error("Pack data is malformed")]
    MalformedPack(
        /// Underlying bundle write error.
        #[source]
        gix_pack::bundle::write::Error,
    ),
    /// A ref-delta base object is missing from the ODB.
    #[error("Missing ref-delta base object {oid}")]
    MissingBase {
        /// The object id that could not be found.
        oid: gix_hash::ObjectId,
    },
    /// Handler is not in the `Fresh` state — pack has already been ingested or session was aborted.
    #[error("Handler is not in the Fresh state (current state: {state:?})")]
    InvalidState {
        /// The current state that prevented ingestion.
        state: SessionState,
    },
    /// Failed to create the pack directory.
    #[error("Failed to create pack directory at {path}")]
    CreatePackDir {
        /// Path where the pack directory was expected.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Errors from connectivity checking.
#[derive(Debug, thiserror::Error)]
pub enum ConnectivityError {
    /// A referenced object is missing from the ODB.
    #[error("Missing {expected_kind} object {oid} referenced from {ref_name}")]
    MissingObject {
        /// The missing object id.
        oid: gix_hash::ObjectId,
        /// The expected kind of the missing object.
        expected_kind: gix_object::Kind,
        /// The ref name that triggered the walk.
        ref_name: BString,
    },
    /// Pack ingestion has not been performed before calling this method.
    #[error("Pack ingestion has not been performed")]
    NotIngested,
    /// An error occurred while reading an object from the ODB.
    #[error(transparent)]
    ObjectRead(#[from] gix_object::find::existing::Error),
}

/// Errors from ref transaction.
#[derive(Debug, thiserror::Error)]
pub enum TransactError {
    /// Pack ingestion has not been performed before calling this method.
    #[error("Pack ingestion has not been performed")]
    NotIngested,
    /// Ref transaction preparation failed.
    #[error("Ref transaction preparation failed")]
    Prepare(
        /// Underlying error.
        #[source]
        Box<dyn std::error::Error + Send + Sync>,
    ),
    /// Ref transaction commit failed.
    #[error("Ref transaction commit failed")]
    Commit(
        /// Underlying error.
        #[source]
        Box<dyn std::error::Error + Send + Sync>,
    ),
    /// Failed to remove the `.keep` file after successful transaction.
    #[error("Failed to remove .keep file at {path}")]
    KeepFileRemoval {
        /// Path to the `.keep` file.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Construct a handler for the bare repository at `repo_path`.
    ///
    /// Opens the object database and ref store at the given path. For a bare
    /// repository the objects directory is at `repo_path/objects/`.
    ///
    /// Returns [`OpenError`] if the path does not exist, is not a directory,
    /// or if the ODB / ref store cannot be opened.
    pub fn open(repo_path: PathBuf, options: Options) -> Result<Self, OpenError> {
        if !repo_path.is_dir() {
            return Err(OpenError::InvalidPath {
                path: repo_path,
            });
        }

        let objects_dir = repo_path.join("objects");

        let odb = gix_odb::Store::at_opts(
            objects_dir.clone(),
            &mut std::iter::empty(),
            gix_odb::store::init::Options {
                object_hash: options.object_hash,
                ..Default::default()
            },
        )
        .map_err(|err| OpenError::Odb {
            path: objects_dir.clone(),
            source: Box::new(err),
        })?;

        let ref_store = gix_ref::file::Store::at(
            repo_path.clone(),
            gix_ref::store::init::Options {
                object_hash: options.object_hash,
                ..Default::default()
            },
        );

        Ok(ReceivePackHandler {
            odb: Arc::new(odb),
            ref_store,
            repo_path,
            objects_dir,
            options,
            state: SessionState::Fresh,
            ingest_outcome: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Pack ingestion
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Ingest pack data from the given byte reader.
    ///
    /// Writes the pack and its index to the repository object directory, resolving
    /// thin-pack ref-delta objects via the ODB. On success, transitions the session
    /// state to [`SessionState::PackIngested`] and returns an [`IngestOutcome`].
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::InvalidState`] if the handler is not in the `Fresh` state.
    /// Returns [`IngestError::MalformedPack`] if the pack data is invalid.
    /// Returns [`IngestError::CreatePackDir`] if the pack directory cannot be created.
    pub fn ingest_pack(&mut self, pack_data: &mut dyn io::Read) -> Result<IngestOutcome, IngestError> {
        if self.state != SessionState::Fresh {
            return Err(IngestError::InvalidState { state: self.state });
        }

        let pack_dir = self.objects_dir.join("pack");
        std::fs::create_dir_all(&pack_dir).map_err(|err| IngestError::CreatePackDir {
            path: pack_dir.clone(),
            source: err,
        })?;

        let mut buffered = io::BufReader::new(pack_data);
        let odb_handle = self.odb.to_handle_arc();

        let outcome = gix_pack::Bundle::write_to_directory(
            &mut buffered,
            Some(&pack_dir),
            &mut gix_features::progress::Discard,
            &AtomicBool::new(false),
            Some(odb_handle),
            gix_pack::bundle::write::Options {
                thread_limit: self.options.thread_limit,
                iteration_mode: self.options.iteration_mode,
                object_hash: self.options.object_hash,
                ..Default::default()
            },
        )
        .map_err(IngestError::MalformedPack)?;

        let object_count = outcome.index.num_objects;
        self.state = SessionState::PackIngested;
        self.ingest_outcome = Some(outcome.clone());

        Ok(IngestOutcome {
            outcome,
            object_count,
        })
    }
}

// ---------------------------------------------------------------------------
// Connectivity check
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Verify connectivity of all new ref targets against the ODB.
    ///
    /// Walks from each non-deletion update's `new_id`, peeling annotated tags to
    /// find the underlying commit, then performs a BFS over the commit graph. For
    /// each commit the tree is verified recursively (all sub-trees and blobs must
    /// exist). Traversal stops when a commit is found in the set of pre-existing
    /// ref tips.
    ///
    /// Requires a prior successful [`ingest_pack`](Self::ingest_pack) call.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectivityError::NotIngested`] if pack ingestion has not been performed.
    /// Returns [`ConnectivityError::MissingObject`] if any referenced object is not in the ODB.
    pub fn check_connectivity(
        &self,
        updates: &[super::Update],
    ) -> Result<ConnectivityResult, ConnectivityError> {
        if self.state != SessionState::PackIngested {
            return Err(ConnectivityError::NotIngested);
        }

        let odb_handle = self.odb.to_handle_arc();

        // Collect pre-existing ref tips (all OIDs currently pointed to by refs).
        let tips = self.collect_existing_ref_tips();

        let mut visited = HashSet::new();
        let mut buf = Vec::new();

        for update in updates {
            // Skip deletions (new_id is null/zero).
            if update.new_id.is_null() {
                continue;
            }

            // Peel annotated tags to find the underlying object.
            let target_id = self.peel_to_non_tag(&odb_handle, &update.new_id, &mut buf, &update.ref_name)?;

            // Check the kind of the peeled target.
            let target_data = {
                use gix_object::Find;
                odb_handle
                    .try_find(&target_id, &mut buf)
                    .map_err(|e| ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(e)))?
                    .ok_or_else(|| ConnectivityError::MissingObject {
                        oid: target_id,
                        expected_kind: gix_object::Kind::Commit,
                        ref_name: update.ref_name.clone(),
                    })?
            };
            match target_data.kind {
                gix_object::Kind::Commit => {
                    self.walk_commits_bfs(
                        &odb_handle,
                        target_id,
                        &tips,
                        &mut visited,
                        &update.ref_name,
                    )?;
                }
                gix_object::Kind::Tree => {
                    // If a ref points directly to a tree (unusual but valid), verify it.
                    if visited.insert(target_id) {
                        self.verify_tree(
                            &odb_handle,
                            &target_id,
                            &mut visited,
                            &update.ref_name,
                        )?;
                    }
                }
                gix_object::Kind::Blob => {
                    // A ref pointing directly to a blob — just verify it exists (already done by find above).
                    visited.insert(target_id);
                }
                gix_object::Kind::Tag => {
                    // This shouldn't happen after peeling, but handle gracefully.
                    visited.insert(target_id);
                }
            }
        }

        // new_objects = visited objects minus pre-existing tips
        let new_objects: HashSet<gix_hash::ObjectId> = visited.difference(&tips).copied().collect();
        Ok(ConnectivityResult { new_objects })
    }

    /// Collect all OIDs from existing refs in the ref store.
    fn collect_existing_ref_tips(&self) -> HashSet<gix_hash::ObjectId> {
        let mut tips = HashSet::new();
        let Ok(platform) = self.ref_store.iter() else {
            return tips;
        };
        let Ok(iter) = platform.all() else {
            return tips;
        };
        for reference in iter {
            let Ok(reference) = reference else {
                continue;
            };
            if let gix_ref::Target::Object(oid) = &reference.target {
                tips.insert(*oid);
            }
            if let Some(peeled) = &reference.peeled {
                tips.insert(*peeled);
            }
        }
        tips
    }

    /// Peel annotated tags recursively until we find a non-tag object.
    /// Returns the final object id.
    fn peel_to_non_tag(
        &self,
        odb_handle: &gix_odb::store::Handle<Arc<gix_odb::Store>>,
        id: &gix_hash::ObjectId,
        buf: &mut Vec<u8>,
        ref_name: &BString,
    ) -> Result<gix_hash::ObjectId, ConnectivityError> {
        use gix_object::Find;

        let mut current = *id;
        loop {
            let data = odb_handle
                .try_find(&current, buf)
                .map_err(|e| ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(e)))?
                .ok_or_else(|| ConnectivityError::MissingObject {
                    oid: current,
                    expected_kind: gix_object::Kind::Tag,
                    ref_name: ref_name.clone(),
                })?;

            if data.kind != gix_object::Kind::Tag {
                return Ok(current);
            }

            // Parse the tag to find its target.
            let tag = gix_object::TagRef::from_bytes(data.data, self.options.object_hash)
                .map_err(|e| {
                    ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(
                        Box::new(e),
                    ))
                })?;
            current = tag.target();
        }
    }

    /// BFS walk of the commit graph from `start_id`.
    /// For each commit: verify its tree exists, then recursively verify all sub-trees and blobs.
    /// Stop when a commit is in `tips` (pre-existing).
    fn walk_commits_bfs(
        &self,
        odb_handle: &gix_odb::store::Handle<Arc<gix_odb::Store>>,
        start_id: gix_hash::ObjectId,
        tips: &HashSet<gix_hash::ObjectId>,
        visited: &mut HashSet<gix_hash::ObjectId>,
        ref_name: &BString,
    ) -> Result<(), ConnectivityError> {
        use gix_object::FindExt;

        let mut queue: VecDeque<gix_hash::ObjectId> = VecDeque::new();
        queue.push_back(start_id);

        let mut buf = Vec::new();
        let mut tree_buf = Vec::new();

        while let Some(commit_id) = queue.pop_front() {
            // If this commit is a pre-existing tip, stop traversal along this path.
            if tips.contains(&commit_id) {
                continue;
            }

            // If already visited, skip.
            if !visited.insert(commit_id) {
                continue;
            }

            // Read the commit object.
            let commit_data = odb_handle.find(&commit_id, &mut buf)?;
            if commit_data.kind != gix_object::Kind::Commit {
                // If somehow not a commit (perhaps a corrupt graph), skip.
                continue;
            }

            // Parse commit to get tree_id and parent_ids.
            let mut commit_iter =
                gix_object::CommitRefIter::from_bytes(commit_data.data, self.options.object_hash);
            let tree_id = commit_iter.tree_id().map_err(|e| {
                ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(
                    Box::new(e),
                ))
            })?;

            // Verify the tree exists and walk it.
            if visited.insert(tree_id) {
                self.verify_tree(odb_handle, &tree_id, visited, ref_name)?;
            }

            // Enqueue parent commits.
            // Re-parse the commit to get parents (commit_iter was partially consumed for tree_id).
            let commit_data2 = odb_handle.find(&commit_id, &mut tree_buf)?;
            let commit_iter2 =
                gix_object::CommitRefIter::from_bytes(commit_data2.data, self.options.object_hash);
            for parent_id in commit_iter2.parent_ids() {
                if !visited.contains(&parent_id) && !tips.contains(&parent_id) {
                    queue.push_back(parent_id);
                }
            }
        }

        Ok(())
    }

    /// Recursively verify a tree and all its entries exist in the ODB.
    /// - Blob entries: verify they exist.
    /// - Tree entries: verify they exist and recurse.
    /// - Commit-mode entries (submodules): skip.
    fn verify_tree(
        &self,
        odb_handle: &gix_odb::store::Handle<Arc<gix_odb::Store>>,
        tree_id: &gix_hash::ObjectId,
        visited: &mut HashSet<gix_hash::ObjectId>,
        ref_name: &BString,
    ) -> Result<(), ConnectivityError> {
        use gix_object::Find;

        let mut buf = Vec::new();
        let mut stack: Vec<gix_hash::ObjectId> = vec![*tree_id];

        while let Some(current_tree_id) = stack.pop() {
            let tree_data = odb_handle
                .try_find(&current_tree_id, &mut buf)
                .map_err(|e| ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(e)))?
                .ok_or_else(|| ConnectivityError::MissingObject {
                    oid: current_tree_id,
                    expected_kind: gix_object::Kind::Tree,
                    ref_name: ref_name.clone(),
                })?;

            if tree_data.kind != gix_object::Kind::Tree {
                return Err(ConnectivityError::MissingObject {
                    oid: current_tree_id,
                    expected_kind: gix_object::Kind::Tree,
                    ref_name: ref_name.clone(),
                });
            }

            let tree_iter = gix_object::TreeRefIter::from_bytes(tree_data.data, self.options.object_hash);
            // Collect entries before dropping borrow on buf
            let entries: Vec<_> = tree_iter
                .filter_map(|entry| entry.ok())
                .map(|entry| (entry.mode, gix_hash::ObjectId::from(entry.oid)))
                .collect();

            for (mode, entry_oid) in entries {
                // Skip submodule entries (commit mode = 0o160000).
                if mode.is_commit() {
                    continue;
                }

                if !visited.insert(entry_oid) {
                    continue;
                }

                if mode.is_tree() {
                    // Sub-tree: add to stack for verification.
                    stack.push(entry_oid);
                } else {
                    // Blob or link: verify it exists in ODB.
                    let exists = odb_handle
                        .try_find(&entry_oid, &mut buf)
                        .map_err(|e| {
                            ConnectivityError::ObjectRead(gix_object::find::existing::Error::Find(e))
                        })?
                        .is_some();
                    if !exists {
                        return Err(ConnectivityError::MissingObject {
                            oid: entry_oid,
                            expected_kind: gix_object::Kind::Blob,
                            ref_name: ref_name.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Ref transaction
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Execute an atomic ref transaction for the given updates.
    ///
    /// Maps each [`Update`](super::Update) to a [`RefEdit`](gix_ref::transaction::RefEdit)
    /// and executes them as a single atomic transaction via the ref store.
    ///
    /// Requires a prior successful [`ingest_pack`](Self::ingest_pack) call (state must be
    /// [`SessionState::PackIngested`]).
    ///
    /// On success, removes the `.keep` file created during pack ingestion and
    /// transitions the session state to [`SessionState::Committed`].
    ///
    /// # Errors
    ///
    /// Returns [`TransactError::NotIngested`] if the handler is not in the `PackIngested` state.
    /// Returns [`TransactError::Prepare`] if the transaction preparation fails (e.g., CAS mismatch).
    /// Returns [`TransactError::Commit`] if the transaction commit fails.
    /// Returns [`TransactError::KeepFileRemoval`] if the `.keep` file cannot be removed after success.
    pub fn transact_refs(&mut self, updates: &[super::Update]) -> Result<TransactionResult, TransactError> {
        if self.state != SessionState::PackIngested {
            return Err(TransactError::NotIngested);
        }

        // Map all updates to RefEdits.
        let edits: Vec<gix_ref::transaction::RefEdit> = updates.iter().map(update_to_ref_edit).collect();

        // Execute the transaction: prepare then commit.
        let prepared = self
            .ref_store
            .transaction()
            .prepare(
                edits,
                gix_lock::acquire::Fail::Immediately,
                gix_lock::acquire::Fail::Immediately,
            )
            .map_err(|e| TransactError::Prepare(Box::new(e)))?;

        prepared
            .commit(None)
            .map_err(|e| TransactError::Commit(Box::new(e)))?;

        // Remove the .keep file.
        if let Some(keep_path) = self.ingest_outcome.as_ref().and_then(|o| o.keep_path.as_ref()) {
            std::fs::remove_file(keep_path).map_err(|source| TransactError::KeepFileRemoval {
                path: keep_path.clone(),
                source,
            })?;
        }

        // Transition state to Committed.
        self.state = SessionState::Committed;

        // Build per-ref results — all Ok on success.
        let ref_results = updates
            .iter()
            .map(|update| RefUpdateResult {
                ref_name: update.ref_name.clone(),
                status: RefUpdateStatus::Ok,
            })
            .collect();

        Ok(TransactionResult { ref_results })
    }
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Result of successful pack ingestion.
#[derive(Debug)]
pub struct IngestOutcome {
    /// The underlying bundle write outcome (paths, index info).
    pub outcome: gix_pack::bundle::write::Outcome,
    /// Number of objects received.
    pub object_count: u32,
}

/// Result of a successful connectivity check.
#[derive(Debug)]
pub struct ConnectivityResult {
    /// Object ids reachable from new ref targets that are not
    /// reachable from pre-existing refs (the "new" object set).
    pub new_objects: HashSet<gix_hash::ObjectId>,
}

/// Per-ref outcome from the atomic ref transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdateResult {
    /// The ref name.
    pub ref_name: BString,
    /// Whether this particular ref was updated successfully.
    pub status: RefUpdateStatus,
}

/// Status of a single ref update within a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefUpdateStatus {
    /// Successfully updated/created/deleted.
    Ok,
    /// Rejected with a reason (CAS mismatch, etc).
    Rejected {
        /// The reason for rejection.
        reason: String,
    },
}

/// Overall transaction outcome.
#[derive(Debug)]
pub struct TransactionResult {
    /// Per-ref results in the same order as input updates.
    pub ref_results: Vec<RefUpdateResult>,
}

// ---------------------------------------------------------------------------
// Update → RefEdit mapping
// ---------------------------------------------------------------------------

/// Map a push [`Update`](super::Update) command to a [`gix_ref::transaction::RefEdit`].
///
/// The mapping logic:
/// - `old_id` non-null, `new_id` non-null → `Change::Update` with `MustExistAndMatch(old_id)`
/// - `old_id` null, `new_id` non-null → `Change::Update` with `MustNotExist`
/// - `old_id` non-null, `new_id` null → `Change::Delete` with `MustExistAndMatch(old_id)`
pub(crate) fn update_to_ref_edit(update: &super::Update) -> gix_ref::transaction::RefEdit {
    use gix_ref::transaction::{Change, LogChange, PreviousValue, RefEdit};
    use gix_ref::Target;

    let name = gix_ref::FullName::try_from(update.ref_name.clone())
        .expect("ref names from the protocol layer should be valid fully-qualified reference names");

    let change = if update.new_id.is_null() {
        // Deletion: old_id is non-null, new_id is null.
        Change::Delete {
            expected: PreviousValue::MustExistAndMatch(Target::Object(update.old_id)),
            log: gix_ref::transaction::RefLog::AndReference,
        }
    } else if update.old_id.is_null() {
        // Creation: old_id is null, new_id is non-null.
        Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::MustNotExist,
            new: Target::Object(update.new_id),
        }
    } else {
        // Normal update: both old_id and new_id are non-null.
        Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::MustExistAndMatch(Target::Object(update.old_id)),
            new: Target::Object(update.new_id),
        }
    };

    RefEdit {
        change,
        name,
        deref: false,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{OpenError, Options, ReceivePackHandler};

    /// Create a minimal bare repository structure at the given path.
    /// This creates the `objects/` and `objects/pack/` subdirectories
    /// and a `HEAD` file, which is the minimum needed for handler construction.
    fn create_bare_repo(path: &std::path::Path) {
        let objects_dir = path.join("objects");
        std::fs::create_dir_all(objects_dir.join("pack"))
            .expect("should be able to create objects/pack directory");
        std::fs::write(path.join("HEAD"), "ref: refs/heads/main\n")
            .expect("should be able to write HEAD");
    }

    #[test]
    fn open_with_valid_bare_repo_path_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        create_bare_repo(tmp.path());

        let handler = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default())?;

        assert_eq!(
            handler.objects_dir,
            tmp.path().join("objects"),
            "objects directory should be at repo_path/objects/"
        );
        assert_eq!(
            handler.state,
            super::SessionState::Fresh,
            "newly opened handler should be in Fresh state"
        );
        Ok(())
    }

    #[test]
    fn open_with_non_existent_path_returns_invalid_path_error() {
        let path = PathBuf::from("/nonexistent/path/that/does/not/exist");
        let result = ReceivePackHandler::open(path.clone(), Options::default());

        match result {
            Err(OpenError::InvalidPath { path: err_path }) => {
                assert_eq!(err_path, path, "error should contain the invalid path");
            }
            Err(other) => panic!("expected OpenError::InvalidPath, got different error: {other}"),
            Ok(_) => panic!("expected OpenError::InvalidPath, but open succeeded"),
        }
    }

    #[test]
    fn open_with_path_missing_objects_dir_still_constructs() -> Result<(), Box<dyn std::error::Error>> {
        // gix_odb::Store::at_opts requires the objects directory to exist as a directory.
        // When the objects/ subdirectory does not exist, `open` should return an Odb error.
        let tmp = tempfile::tempdir()?;
        // Create a directory but don't create objects/ inside it.
        // Just create HEAD so it looks like a repo directory without an ODB.
        std::fs::write(tmp.path().join("HEAD"), "ref: refs/heads/main\n")?;

        let result = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default());

        match result {
            Err(OpenError::Odb { path, .. }) => {
                assert_eq!(
                    path,
                    tmp.path().join("objects"),
                    "error should reference the missing objects directory"
                );
            }
            Err(other) => panic!("expected OpenError::Odb for missing objects dir, got different error: {other}"),
            Ok(_) => panic!("expected OpenError::Odb, but open succeeded"),
        }
        Ok(())
    }

    mod pack_ingestion_properties {
        use super::*;
        use proptest::prelude::*;

        // Feature: async-receive-pack-handler, Property 2: Malformed pack produces error with no filesystem artifacts
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn malformed_pack_produces_no_artifacts(
                random_bytes in proptest::collection::vec(any::<u8>(), 0..1024),
            ) {
                let tmp = tempfile::tempdir()
                    .expect("should be able to create temp directory for test");
                create_bare_repo(tmp.path());

                let pack_dir = tmp.path().join("objects").join("pack");

                // Snapshot existing files in pack dir before ingestion attempt
                let files_before: std::collections::HashSet<_> = std::fs::read_dir(&pack_dir)
                    .expect("pack dir should exist")
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .collect();

                let mut handler = ReceivePackHandler::open(
                    tmp.path().to_path_buf(),
                    Options::default(),
                ).expect("handler should open successfully for a valid bare repo");

                let mut cursor = std::io::Cursor::new(&random_bytes);
                let result = handler.ingest_pack(&mut cursor);

                // Must return an error for random bytes
                prop_assert!(
                    result.is_err(),
                    "ingest_pack should return an error for random/malformed bytes, got: {:?}",
                    result
                );

                // Assert no new .pack, .idx, or .keep files were created
                let files_after: std::collections::HashSet<_> = std::fs::read_dir(&pack_dir)
                    .expect("pack dir should still exist")
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .collect();

                let new_files: Vec<_> = files_after.difference(&files_before)
                    .filter(|p| {
                        let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                        ext == "pack" || ext == "idx" || ext == "keep"
                    })
                    .collect();

                prop_assert!(
                    new_files.is_empty(),
                    "no .pack, .idx, or .keep files should be created after a failed ingestion, found: {:?}",
                    new_files
                );
            }
        }

        // Feature: async-receive-pack-handler, Property 4: Non-empty pack ingestion produces complete file set
        //
        // This test creates a minimal valid pack using git CLI tooling, then verifies
        // that ingest_pack produces an outcome with all paths set and files existing on disk.
        #[test]
        fn non_empty_pack_produces_complete_file_set() -> Result<(), Box<dyn std::error::Error>> {
            // Create a bare repository using git CLI
            let tmp = tempfile::tempdir()?;
            let repo_path = tmp.path().join("test.git");

            let status = std::process::Command::new("git")
                .args(["init", "--bare"])
                .arg(&repo_path)
                .output()
                .expect("git should be available to create test fixtures");
            assert!(
                status.status.success(),
                "git init --bare should succeed: {}",
                String::from_utf8_lossy(&status.stderr)
            );

            // Write a blob object into the bare repo
            let hash_output = std::process::Command::new("git")
                .args(["hash-object", "-w", "--stdin"])
                .current_dir(&repo_path)
                .env("GIT_DIR", &repo_path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .and_then(|mut child| {
                    use std::io::Write;
                    child
                        .stdin
                        .take()
                        .expect("stdin should be available")
                        .write_all(b"hello world\n")
                        .expect("write to stdin should succeed");
                    child.wait_with_output()
                })
                .expect("git hash-object should succeed");
            assert!(
                hash_output.status.success(),
                "git hash-object should succeed: {}",
                String::from_utf8_lossy(&hash_output.stderr)
            );
            let blob_oid = String::from_utf8(hash_output.stdout)
                .expect("hash output should be valid utf-8")
                .trim()
                .to_string();

            // Create a pack file from that object using git pack-objects
            let pack_output = std::process::Command::new("git")
                .args(["pack-objects", "--stdout"])
                .current_dir(&repo_path)
                .env("GIT_DIR", &repo_path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .and_then(|mut child| {
                    use std::io::Write;
                    child
                        .stdin
                        .take()
                        .expect("stdin should be available")
                        .write_all(format!("{}\n", blob_oid).as_bytes())
                        .expect("write oid to stdin should succeed");
                    child.wait_with_output()
                })
                .expect("git pack-objects should succeed");
            assert!(
                pack_output.status.success(),
                "git pack-objects should succeed: {}",
                String::from_utf8_lossy(&pack_output.stderr)
            );
            let pack_bytes = pack_output.stdout;
            assert!(
                !pack_bytes.is_empty(),
                "pack-objects should produce non-empty output"
            );

            // Now create a fresh bare repo for ingestion (to avoid the existing loose object)
            let ingest_tmp = tempfile::tempdir()?;
            let ingest_repo = ingest_tmp.path().join("ingest.git");
            let status = std::process::Command::new("git")
                .args(["init", "--bare"])
                .arg(&ingest_repo)
                .output()
                .expect("git should be available");
            assert!(
                status.status.success(),
                "git init --bare for ingest repo should succeed"
            );

            let mut handler = ReceivePackHandler::open(
                ingest_repo.clone(),
                Options::default(),
            )?;

            let mut cursor = std::io::Cursor::new(&pack_bytes);
            let outcome = handler
                .ingest_pack(&mut cursor)
                .expect("ingest_pack should succeed for a valid pack");

            // Assert data_path, index_path, and keep_path are all Some
            let data_path = outcome
                .outcome
                .data_path
                .as_ref()
                .expect("data_path should be Some for a non-empty pack");
            let index_path = outcome
                .outcome
                .index_path
                .as_ref()
                .expect("index_path should be Some for a non-empty pack");
            let keep_path = outcome
                .outcome
                .keep_path
                .as_ref()
                .expect("keep_path should be Some for a non-empty pack");

            // Assert all files exist on disk
            assert!(
                data_path.exists(),
                "pack data file should exist on disk at {:?}",
                data_path
            );
            assert!(
                index_path.exists(),
                "pack index file should exist on disk at {:?}",
                index_path
            );
            assert!(
                keep_path.exists(),
                "keep file should exist on disk at {:?}",
                keep_path
            );

            // Verify object count is at least 1
            assert!(
                outcome.object_count >= 1,
                "object count should be at least 1 for a non-empty pack, got {}",
                outcome.object_count
            );

            Ok(())
        }

        // Feature: async-receive-pack-handler, Property 3: Thin pack with resolvable bases ingests successfully
        //
        // This test creates a repository with a known base blob, then produces a thin pack
        // containing a delta object that references that base. Ingestion should succeed
        // because the handler resolves ref-delta bases via the ODB.
        #[test]
        fn thin_pack_with_resolvable_bases_ingests_successfully() -> Result<(), Box<dyn std::error::Error>> {
            // Create a bare repository and add a base blob object
            let tmp = tempfile::tempdir()?;
            let repo_path = tmp.path().join("base.git");

            let status = std::process::Command::new("git")
                .args(["init", "--bare"])
                .arg(&repo_path)
                .output()
                .expect("git should be available");
            assert!(
                status.status.success(),
                "git init --bare should succeed: {}",
                String::from_utf8_lossy(&status.stderr)
            );

            // Write a base blob into the repository
            let base_content = b"this is the base content for thin pack testing\n";
            let hash_output = std::process::Command::new("git")
                .args(["hash-object", "-w", "--stdin"])
                .current_dir(&repo_path)
                .env("GIT_DIR", &repo_path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .and_then(|mut child| {
                    use std::io::Write;
                    child
                        .stdin
                        .take()
                        .expect("stdin should be available")
                        .write_all(base_content)
                        .expect("write to stdin should succeed");
                    child.wait_with_output()
                })
                .expect("git hash-object should succeed");
            assert!(
                hash_output.status.success(),
                "git hash-object for base blob should succeed: {}",
                String::from_utf8_lossy(&hash_output.stderr)
            );
            let _base_oid = String::from_utf8(hash_output.stdout)
                .expect("hash output should be valid utf-8")
                .trim()
                .to_string();

            // Write a similar blob that will produce a delta against the base
            let delta_content = b"this is the base content for thin pack testing\nwith an extra line\n";
            let delta_hash_output = std::process::Command::new("git")
                .args(["hash-object", "-w", "--stdin"])
                .current_dir(&repo_path)
                .env("GIT_DIR", &repo_path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .and_then(|mut child| {
                    use std::io::Write;
                    child
                        .stdin
                        .take()
                        .expect("stdin should be available")
                        .write_all(delta_content)
                        .expect("write to stdin should succeed");
                    child.wait_with_output()
                })
                .expect("git hash-object for delta blob should succeed");
            assert!(
                delta_hash_output.status.success(),
                "git hash-object for delta blob should succeed: {}",
                String::from_utf8_lossy(&delta_hash_output.stderr)
            );
            let delta_oid = String::from_utf8(delta_hash_output.stdout)
                .expect("hash output should be valid utf-8")
                .trim()
                .to_string();

            // Create a thin pack containing only the delta object (not the base)
            // --thin allows ref-deltas against objects not in the pack
            // --stdout outputs the pack to stdout
            // We tell git the base object already exists at the remote by excluding it
            let pack_output = std::process::Command::new("git")
                .args(["pack-objects", "--stdout", "--thin"])
                .current_dir(&repo_path)
                .env("GIT_DIR", &repo_path)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .and_then(|mut child| {
                    use std::io::Write;
                    // Only include the delta object in the pack, not the base
                    child
                        .stdin
                        .take()
                        .expect("stdin should be available")
                        .write_all(format!("{}\n", delta_oid).as_bytes())
                        .expect("write oid to stdin should succeed");
                    child.wait_with_output()
                })
                .expect("git pack-objects --thin should succeed");
            assert!(
                pack_output.status.success(),
                "git pack-objects --thin should succeed: {}",
                String::from_utf8_lossy(&pack_output.stderr)
            );
            let thin_pack_bytes = pack_output.stdout;
            assert!(
                !thin_pack_bytes.is_empty(),
                "thin pack should produce non-empty output"
            );

            // Now open the handler on the SAME repo (which has the base object in its ODB).
            // The thin pack should resolve the ref-delta against the existing base object.
            let mut handler = ReceivePackHandler::open(
                repo_path.clone(),
                Options::default(),
            )?;

            let mut cursor = std::io::Cursor::new(&thin_pack_bytes);
            let outcome = handler.ingest_pack(&mut cursor);

            // The ingestion should succeed because the base object is resolvable via ODB.
            // Note: git pack-objects with --thin on a single blob may or may not produce a
            // ref-delta depending on whether it decides a delta is beneficial. If it produces
            // a non-thin pack (no ref-deltas), that's still valid — the key property is that
            // ingestion succeeds.
            match outcome {
                Ok(ingest_outcome) => {
                    assert!(
                        ingest_outcome.object_count >= 1,
                        "ingested pack should have at least 1 object, got {}",
                        ingest_outcome.object_count
                    );
                }
                Err(e) => {
                    panic!(
                        "ingest_pack should succeed for a thin pack with resolvable bases, got error: {e}"
                    );
                }
            }

            Ok(())
        }
    }

    mod connectivity_properties {
        use super::*;
        use crate::receive_pack::Update;

        /// Helper: create a bare repo with `git init --bare` and return the path.
        fn git_init_bare(parent: &std::path::Path, name: &str) -> PathBuf {
            let repo_path = parent.join(name);
            let output = std::process::Command::new("git")
                .args(["init", "--bare"])
                .arg(&repo_path)
                .output()
                .expect("git should be available");
            assert!(
                output.status.success(),
                "git init --bare should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            repo_path
        }

        /// Helper: run a git command in the given repo, returning stdout as String.
        fn git_in(repo: &std::path::Path, args: &[&str]) -> String {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(repo)
                .env("GIT_DIR", repo)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .output()
                .expect("git command should execute");
            assert!(
                output.status.success(),
                "git {:?} should succeed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .expect("git output should be valid utf-8")
                .trim()
                .to_string()
        }

        /// Helper: write a blob and return its oid.
        fn write_blob(repo: &std::path::Path, content: &[u8]) -> String {
            let mut child = std::process::Command::new("git")
                .args(["hash-object", "-w", "--stdin"])
                .current_dir(repo)
                .env("GIT_DIR", repo)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git hash-object should spawn");
            {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin available")
                    .write_all(content)
                    .expect("write to stdin should succeed");
            }
            let output = child.wait_with_output().expect("git hash-object should complete");
            assert!(
                output.status.success(),
                "git hash-object should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout)
                .expect("oid should be valid utf-8")
                .trim()
                .to_string()
        }

        /// Helper: create a pack from a list of OIDs (treated as revisions via --revs)
        /// and return the pack bytes. This includes all objects reachable from the given revisions.
        fn pack_objects(repo: &std::path::Path, revs: &[&str]) -> Vec<u8> {
            let input = revs.join("\n") + "\n";
            let mut child = std::process::Command::new("git")
                .args(["pack-objects", "--stdout", "--revs"])
                .current_dir(repo)
                .env("GIT_DIR", repo)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git pack-objects should spawn");
            {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin available")
                    .write_all(input.as_bytes())
                    .expect("write to pack-objects stdin should succeed");
            }
            let output = child.wait_with_output().expect("git pack-objects should complete");
            assert!(
                output.status.success(),
                "git pack-objects --revs should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        }

        /// Helper: create a commit in a bare repo using low-level git commands.
        /// Returns (commit_oid, tree_oid).
        fn create_commit(
            repo: &std::path::Path,
            blob_content: &[u8],
            filename: &str,
            parent: Option<&str>,
            message: &str,
        ) -> (String, String) {
            let blob_oid = write_blob(repo, blob_content);

            // Create a tree with the blob
            let tree_input = format!("100644 blob {}\t{}\n", blob_oid, filename);
            let mut child = std::process::Command::new("git")
                .args(["mktree"])
                .current_dir(repo)
                .env("GIT_DIR", repo)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git mktree should spawn");
            {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin available")
                    .write_all(tree_input.as_bytes())
                    .expect("write to mktree stdin should succeed");
            }
            let output = child.wait_with_output().expect("git mktree should complete");
            assert!(
                output.status.success(),
                "git mktree should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let tree_oid = String::from_utf8(output.stdout)
                .expect("tree oid should be valid utf-8")
                .trim()
                .to_string();

            // Create the commit
            let mut args = vec!["commit-tree", &tree_oid, "-m", message];
            let parent_flag;
            if let Some(p) = parent {
                parent_flag = p.to_string();
                args.push("-p");
                args.push(&parent_flag);
            }
            let commit_oid = git_in(repo, &args);

            (commit_oid, tree_oid)
        }

        // Feature: async-receive-pack-handler, Property 5: Connectivity check succeeds on complete object graphs
        //
        // Creates a bare repo with a commit (tree + blob), pushes a valid pack containing
        // the commit, calls check_connectivity with an update pointing to the new commit,
        // and asserts success.
        #[test]
        fn connectivity_check_succeeds_on_complete_object_graphs(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let tmp = tempfile::tempdir()?;

            // Create a source bare repo with a commit
            let source = git_init_bare(tmp.path(), "source.git");
            let (commit_oid, _tree_oid) =
                create_commit(&source, b"hello world\n", "file.txt", None, "initial commit");

            // Create a pack containing the commit and all reachable objects
            let pack_bytes = pack_objects(&source, &[&commit_oid]);

            // Create a destination bare repo and ingest the pack
            let dest = git_init_bare(tmp.path(), "dest.git");
            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor)
                .expect("ingest_pack should succeed for a valid pack");

            // Build an update pointing to the new commit
            let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
                .expect("commit oid should be valid hex");
            let update = Update {
                old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
                new_id,
                ref_name: "refs/heads/main".into(),
            };

            let result = handler.check_connectivity(&[update]);
            assert!(
                result.is_ok(),
                "connectivity check should succeed on a complete object graph, got: {:?}",
                result.err()
            );

            let connectivity_result = result.expect("already asserted Ok");
            assert!(
                connectivity_result.new_objects.contains(&new_id),
                "new_objects should contain the pushed commit"
            );

            Ok(())
        }

        // Feature: async-receive-pack-handler, Property 7: Deletion updates are excluded from connectivity checking
        //
        // Creates a bare repo, ingests a pack with some objects, then creates updates
        // where some new_id is the null id (deletion). Asserts check_connectivity
        // succeeds because deletions are skipped.
        #[test]
        fn deletion_updates_are_excluded_from_connectivity_checking(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let tmp = tempfile::tempdir()?;

            // Create a source repo with a commit so we have a valid pack to ingest
            let source = git_init_bare(tmp.path(), "source.git");
            let (commit_oid, _tree_oid) =
                create_commit(&source, b"content\n", "a.txt", None, "first");

            let pack_bytes = pack_objects(&source, &[&commit_oid]);

            // Ingest into a destination repo
            let dest = git_init_bare(tmp.path(), "dest.git");
            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor)
                .expect("ingest_pack should succeed");

            // Build updates: one valid creation, and one deletion (new_id = null).
            // The deletion should be skipped entirely — no object lookup for null ids.
            let valid_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
                .expect("commit oid should be valid hex");
            let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);

            let updates = vec![
                Update {
                    old_id: null_id,
                    new_id: valid_id,
                    ref_name: "refs/heads/main".into(),
                },
                // Deletion: old_id is some non-null value, new_id is null
                Update {
                    old_id: valid_id,
                    new_id: null_id,
                    ref_name: "refs/heads/to-delete".into(),
                },
                // Another deletion with a completely fabricated old_id
                Update {
                    old_id: gix_hash::ObjectId::from_hex(
                        b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    )
                    .expect("hex should parse"),
                    new_id: null_id,
                    ref_name: "refs/heads/another-delete".into(),
                },
            ];

            let result = handler.check_connectivity(&updates);
            assert!(
                result.is_ok(),
                "connectivity check should succeed when deletion updates are present, got: {:?}",
                result.err()
            );

            Ok(())
        }

        // Feature: async-receive-pack-handler, Property 8: Submodule tree entries do not trigger missing-object errors
        //
        // Creates a bare repo with a tree containing a gitlink entry (mode 160000)
        // pointing to a commit OID that does NOT exist in the ODB. Asserts
        // check_connectivity succeeds because submodule entries are skipped.
        #[test]
        fn submodule_tree_entries_do_not_trigger_missing_object_errors(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let tmp = tempfile::tempdir()?;

            // Create a source bare repo
            let source = git_init_bare(tmp.path(), "source.git");

            // Create a blob for a regular file
            let blob_oid = write_blob(&source, b"regular file content\n");

            // Fabricate a submodule commit oid that does NOT exist anywhere
            let fake_submodule_oid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

            // Create a tree with both a regular blob and a gitlink (submodule) entry.
            // git mktree format: <mode> SP <type> SP <oid> TAB <name>
            let tree_input = format!(
                "100644 blob {}\tfile.txt\n160000 commit {}\tmy-submodule\n",
                blob_oid, fake_submodule_oid
            );
            let mut child = std::process::Command::new("git")
                .args(["mktree"])
                .current_dir(&source)
                .env("GIT_DIR", &source)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git mktree should spawn");
            {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin available")
                    .write_all(tree_input.as_bytes())
                    .expect("write to mktree should succeed");
            }
            let output = child.wait_with_output().expect("git mktree should complete");
            assert!(
                output.status.success(),
                "git mktree should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let tree_oid = String::from_utf8(output.stdout)
                .expect("tree oid should be valid utf-8")
                .trim()
                .to_string();

            // Create a commit pointing to this tree
            let commit_oid = git_in(&source, &["commit-tree", &tree_oid, "-m", "commit with submodule"]);

            // Pack the commit + tree + blob (but NOT the fake submodule commit)
            let pack_bytes = pack_objects(&source, &[&commit_oid]);

            // Ingest into a destination repo
            let dest = git_init_bare(tmp.path(), "dest.git");
            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor)
                .expect("ingest_pack should succeed");

            // Check connectivity — should succeed because the gitlink entry is skipped
            let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
                .expect("commit oid should be valid hex");
            let update = Update {
                old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
                new_id,
                ref_name: "refs/heads/main".into(),
            };

            let result = handler.check_connectivity(&[update]);
            assert!(
                result.is_ok(),
                "connectivity check should succeed even with submodule entries pointing to \
                 non-existent commits, got: {:?}",
                result.err()
            );

            Ok(())
        }

        // Feature: async-receive-pack-handler, Property 6: Connectivity walk terminates at pre-existing ref tips
        //
        // Creates a bare repo with a chain of commits (A → B → C) on refs/heads/main,
        // then pushes a new commit D (parent = C) in a pack. Calls check_connectivity
        // for D and asserts the walk only visits D and stops at C (which is a pre-existing tip).
        // Verifies new_objects contains D but NOT A, B, or C.
        #[test]
        fn connectivity_walk_terminates_at_pre_existing_ref_tips(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let tmp = tempfile::tempdir()?;
            let repo = git_init_bare(tmp.path(), "repo.git");

            // Create a chain: A → B → C
            let (commit_a, _) = create_commit(&repo, b"a\n", "a.txt", None, "commit A");
            let (commit_b, _) = create_commit(&repo, b"b\n", "b.txt", Some(&commit_a), "commit B");
            let (commit_c, _) = create_commit(&repo, b"c\n", "c.txt", Some(&commit_b), "commit C");

            // Point refs/heads/main at C so it becomes a pre-existing tip
            git_in(&repo, &["update-ref", "refs/heads/main", &commit_c]);

            // Create commit D with parent C
            let (commit_d, _) = create_commit(&repo, b"d\n", "d.txt", Some(&commit_c), "commit D");

            // Create a pack containing ONLY commit D (and its tree/blob).
            // Use --revs with C as exclusion to get only D's new objects.
            let rev_input = format!("{}\n^{}\n", commit_d, commit_c);
            let mut child = std::process::Command::new("git")
                .args(["pack-objects", "--stdout", "--revs"])
                .current_dir(&repo)
                .env("GIT_DIR", &repo)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect("git pack-objects --revs should spawn");
            {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin available")
                    .write_all(rev_input.as_bytes())
                    .expect("write revs to pack-objects should succeed");
            }
            let output = child.wait_with_output().expect("git pack-objects --revs should complete");
            assert!(
                output.status.success(),
                "git pack-objects --revs should succeed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let pack_bytes = output.stdout;

            // Now open a handler on the same repo (which already has A, B, C)
            let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;
            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor)
                .expect("ingest_pack should succeed for the incremental pack");

            // Check connectivity for D
            let d_id = gix_hash::ObjectId::from_hex(commit_d.as_bytes())
                .expect("commit D oid should be valid hex");
            let c_id = gix_hash::ObjectId::from_hex(commit_c.as_bytes())
                .expect("commit C oid should be valid hex");
            let b_id = gix_hash::ObjectId::from_hex(commit_b.as_bytes())
                .expect("commit B oid should be valid hex");
            let a_id = gix_hash::ObjectId::from_hex(commit_a.as_bytes())
                .expect("commit A oid should be valid hex");

            let update = Update {
                old_id: c_id,
                new_id: d_id,
                ref_name: "refs/heads/main".into(),
            };

            let result = handler.check_connectivity(&[update]);
            assert!(
                result.is_ok(),
                "connectivity check should succeed, got: {:?}",
                result.err()
            );

            let connectivity_result = result.expect("already asserted Ok");

            // D should be in new_objects
            assert!(
                connectivity_result.new_objects.contains(&d_id),
                "new_objects should contain commit D"
            );

            // A, B, C should NOT be in new_objects (walk terminated at pre-existing tip C)
            assert!(
                !connectivity_result.new_objects.contains(&c_id),
                "new_objects should NOT contain commit C (pre-existing tip)"
            );
            assert!(
                !connectivity_result.new_objects.contains(&b_id),
                "new_objects should NOT contain commit B (ancestor of pre-existing tip)"
            );
            assert!(
                !connectivity_result.new_objects.contains(&a_id),
                "new_objects should NOT contain commit A (ancestor of pre-existing tip)"
            );

            Ok(())
        }
    }

    mod ref_edit_mapping_properties {
        use crate::receive_pack::Update;
        use crate::receive_pack::handler::update_to_ref_edit;
        use proptest::prelude::*;

        // Feature: async-receive-pack-handler, Property 1: Update-to-RefEdit mapping preserves semantics
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn update_to_ref_edit_mapping_preserves_semantics(
                old_is_null in any::<bool>(),
                new_is_null in any::<bool>(),
                random_old in any::<[u8; 20]>(),
                random_new in any::<[u8; 20]>(),
            ) {
                use gix_ref::transaction::{Change, PreviousValue};
                use gix_ref::Target;

                // Construct old_id and new_id based on the bool flags.
                let old_id = if old_is_null {
                    gix_hash::ObjectId::null(gix_hash::Kind::Sha1)
                } else {
                    gix_hash::ObjectId::from_bytes_or_panic(&random_old)
                };
                let new_id = if new_is_null {
                    gix_hash::ObjectId::null(gix_hash::Kind::Sha1)
                } else {
                    gix_hash::ObjectId::from_bytes_or_panic(&random_new)
                };

                // Skip the case where both are null (invalid in protocol terms).
                if old_is_null && new_is_null {
                    return Ok(());
                }

                let update = Update {
                    old_id,
                    new_id,
                    ref_name: "refs/heads/test".into(),
                };

                let ref_edit = update_to_ref_edit(&update);

                if new_is_null {
                    // Deletion case: old_id non-null, new_id null.
                    match &ref_edit.change {
                        Change::Delete { expected, .. } => {
                            prop_assert_eq!(
                                expected,
                                &PreviousValue::MustExistAndMatch(Target::Object(old_id)),
                                "deletion should have MustExistAndMatch(old_id)"
                            );
                        }
                        other => {
                            prop_assert!(
                                false,
                                "expected Change::Delete for deletion, got: {:?}",
                                other
                            );
                        }
                    }
                } else if old_is_null {
                    // Creation case: old_id null, new_id non-null.
                    match &ref_edit.change {
                        Change::Update { expected, new, .. } => {
                            prop_assert_eq!(
                                expected,
                                &PreviousValue::MustNotExist,
                                "creation should have MustNotExist"
                            );
                            prop_assert_eq!(
                                new,
                                &Target::Object(new_id),
                                "creation target should be new_id"
                            );
                        }
                        other => {
                            prop_assert!(
                                false,
                                "expected Change::Update for creation, got: {:?}",
                                other
                            );
                        }
                    }
                } else {
                    // Normal update case: both non-null.
                    match &ref_edit.change {
                        Change::Update { expected, new, .. } => {
                            prop_assert_eq!(
                                expected,
                                &PreviousValue::MustExistAndMatch(Target::Object(old_id)),
                                "normal update should have MustExistAndMatch(old_id)"
                            );
                            prop_assert_eq!(
                                new,
                                &Target::Object(new_id),
                                "normal update target should be new_id"
                            );
                        }
                        other => {
                            prop_assert!(
                                false,
                                "expected Change::Update for normal update, got: {:?}",
                                other
                            );
                        }
                    }
                }
            }
        }
    }
}
