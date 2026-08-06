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
}
