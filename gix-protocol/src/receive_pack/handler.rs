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
// Accessors
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Return the current session state.
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Return the path to the object directory.
    pub fn objects_dir(&self) -> &std::path::Path {
        &self.objects_dir
    }

    /// Return a shared reference to the ODB store, useful for lookups in tests
    /// or integrators that need to verify object availability.
    pub fn odb(&self) -> &Arc<gix_odb::Store> {
        &self.odb
    }

    /// Return the `.keep` file path from the most recent pack ingestion, if available.
    pub fn ingest_pack_keep_path(&self) -> Option<&std::path::Path> {
        self.ingest_outcome
            .as_ref()
            .and_then(|o| o.keep_path.as_deref())
    }

    /// Disable reflog writing on the internal ref store.
    ///
    /// This is useful in test scenarios where no committer identity is configured.
    pub fn disable_reflog(&mut self) {
        self.ref_store.write_reflog = gix_ref::store::WriteReflog::Disable;
    }
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
// Pack abort
// ---------------------------------------------------------------------------

impl ReceivePackHandler {
    /// Abort the current session: remove the `.keep` file so the pack becomes
    /// eligible for garbage collection.
    ///
    /// This is a no-op if the session is already in [`SessionState::Committed`],
    /// [`SessionState::Aborted`], or [`SessionState::Fresh`] (nothing to abort).
    ///
    /// # Errors
    ///
    /// Returns an I/O error only if the `.keep` file exists but cannot be removed.
    pub fn abort_pack(&mut self) -> Result<(), std::io::Error> {
        match self.state {
            SessionState::Fresh | SessionState::Committed | SessionState::Aborted => Ok(()),
            SessionState::PackIngested => {
                if let Some(keep_path) =
                    self.ingest_outcome.as_ref().and_then(|o| o.keep_path.as_ref())
                {
                    match std::fs::remove_file(keep_path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // Already removed — that's fine.
                        }
                        Err(e) => return Err(e),
                    }
                }
                self.state = SessionState::Aborted;
                Ok(())
            }
        }
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

// ---------------------------------------------------------------------------
// Delegate trait implementation
// ---------------------------------------------------------------------------

impl super::Delegate for ReceivePackHandler {
    /// Run the full receive-pack pipeline: ingest pack → connectivity check → ref transaction.
    ///
    /// Maps pipeline failures to appropriate [`Response`](super::Response) values:
    /// - Pack ingestion failure → `UnpackStatus::Error`, all refs `Rejected`
    /// - Connectivity failure → `UnpackStatus::Error`, all refs `Rejected`
    /// - Ref transaction failure → `UnpackStatus::Error`, per-ref statuses from transaction if available
    /// - Success → `UnpackStatus::Ok`, one `RefStatus::Ok` per updated ref
    fn receive(
        &mut self,
        request: &super::Request,
        pack_data: &mut dyn io::Read,
    ) -> Result<super::Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
        // Step 1: Ingest pack data
        if let Err(e) = self.ingest_pack(pack_data) {
            let error_msg = e.to_string();
            let ref_statuses = request
                .updates
                .iter()
                .map(|update| super::RefStatus::Rejected {
                    ref_name: update.ref_name.clone(),
                    message: format!("unpack failed: {error_msg}").into(),
                })
                .collect();
            return Ok(super::Response {
                unpack_status: super::UnpackStatus::Error(error_msg.into()),
                ref_statuses,
                sideband_messages: Vec::new(),
            });
        }

        // Step 2: Connectivity check
        if let Err(e) = self.check_connectivity(&request.updates) {
            let error_msg = e.to_string();
            let ref_statuses = request
                .updates
                .iter()
                .map(|update| super::RefStatus::Rejected {
                    ref_name: update.ref_name.clone(),
                    message: format!("connectivity check failed: {error_msg}").into(),
                })
                .collect();
            return Ok(super::Response {
                unpack_status: super::UnpackStatus::Error(error_msg.into()),
                ref_statuses,
                sideband_messages: Vec::new(),
            });
        }

        // Step 3: Ref transaction
        match self.transact_refs(&request.updates) {
            Ok(transaction_result) => {
                let ref_statuses = transaction_result
                    .ref_results
                    .into_iter()
                    .map(|r| match r.status {
                        RefUpdateStatus::Ok => super::RefStatus::Ok {
                            ref_name: r.ref_name,
                        },
                        RefUpdateStatus::Rejected { reason } => super::RefStatus::Rejected {
                            ref_name: r.ref_name,
                            message: reason.into(),
                        },
                    })
                    .collect();
                Ok(super::Response {
                    unpack_status: super::UnpackStatus::Ok,
                    ref_statuses,
                    sideband_messages: Vec::new(),
                })
            }
            Err(e) => {
                let error_msg = e.to_string();
                let ref_statuses = request
                    .updates
                    .iter()
                    .map(|update| super::RefStatus::Rejected {
                        ref_name: update.ref_name.clone(),
                        message: format!("ref transaction failed: {error_msg}").into(),
                    })
                    .collect();
                Ok(super::Response {
                    unpack_status: super::UnpackStatus::Error(error_msg.into()),
                    ref_statuses,
                    sideband_messages: Vec::new(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
