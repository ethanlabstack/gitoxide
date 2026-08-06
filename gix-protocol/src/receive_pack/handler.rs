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
use std::path::PathBuf;

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
pub(crate) enum SessionState {
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
    /// Object database store.
    pub(crate) odb: gix_odb::Store,
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
