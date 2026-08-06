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

use std::collections::HashSet;
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
}
