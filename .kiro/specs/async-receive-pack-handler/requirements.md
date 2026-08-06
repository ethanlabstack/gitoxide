# Requirements Document

## Introduction

This feature implements a reusable async server-side receive-pack handler for gitoxide. The handler orchestrates the backend work that happens *after* the protocol framing layer has parsed the incoming push request: unpacking and indexing the incoming pack data (with thin-pack resolution), executing an atomic ref transaction with compare-and-swap semantics, performing a connectivity check to guarantee repository integrity, and exposing a pipeline step API that lets server operators insert custom logic between steps.

The handler targets bare repositories and builds on `gix-pack`, `gix-ref`, `gix-traverse`, and `gix-odb`. The existing `gix-protocol::receive_pack` module provides the protocol framing (request parsing, response writing, sideband) and `Delegate` trait — this spec covers the concrete `ReceivePackHandler` implementation that a server instantiates with a repository path/ODB.

## Glossary

- **Handler**: The `ReceivePackHandler` struct that orchestrates pack ingestion, connectivity verification, hook invocation, and ref transaction for one push session.
- **ODB**: Object Database — the combined loose-object and multi-pack storage accessed via `gix_odb::Store`.
- **Pack_Ingester**: The subsystem responsible for reading incoming pack bytes, resolving thin-pack ref-deltas via ODB lookup, writing the pack and index to the repository's object directory.
- **Connectivity_Checker**: The subsystem that walks from new ref targets to verify all referenced objects are reachable in the ODB (including the newly ingested pack).
- **Ref_Transactor**: The subsystem that maps parsed `Update` commands to `gix_ref` `RefEdit` operations and commits them atomically with compare-and-swap semantics.
- **Pipeline**: The sequence of discrete steps (ingest, connectivity check, ref transaction) exposed as individual public methods on the Handler, allowing integrators to insert custom logic between steps.
- **Thin_Pack**: A pack file that contains ref-delta entries whose base objects are not included in the pack itself but are expected to exist in the receiving repository's ODB.
- **Update**: A parsed ref update command containing `old_id`, `new_id`, and `ref_name` as provided by the protocol layer.
- **Compare_And_Swap**: A ref update constraint where the current ref value must match the expected `old_id` before the new value is written; also known as CAS.

## Requirements

### Requirement 1: Pack Ingestion and Indexing

**User Story:** As a server operator, I want incoming pack data to be written to disk with a valid index (resolving thin-pack ref-deltas against the existing ODB), so that pushed objects become available for subsequent operations.

#### Acceptance Criteria

1. WHEN pack data bytes are provided via the protocol `Delegate::receive` callback, THE Pack_Ingester SHALL write the pack and its index to the repository object directory using `gix_pack::Bundle::write_to_directory`.
2. WHEN the incoming pack is a Thin_Pack, THE Pack_Ingester SHALL resolve ref-delta base objects by looking them up in the ODB via `LookupRefDeltaObjectsIter` and rewrite resolved ref-deltas as offset-deltas in the written pack.
3. WHEN a ref-delta base object is not found in the ODB and cannot be resolved within the pack itself, THE Pack_Ingester SHALL return an error indicating the missing base object id during the index-writing phase.
4. WHEN pack ingestion completes successfully and the pack contains one or more objects, THE Pack_Ingester SHALL produce a `gix_pack::bundle::write::Outcome` containing the paths to the written pack file, index file, and a `.keep` file that prevents garbage collection until refs are updated.
5. WHEN the incoming pack is empty (zero objects), THE Pack_Ingester SHALL treat the push as valid and produce an outcome with no written files (data_path, index_path, and keep_path are all None).
6. IF pack data is malformed or fails integrity verification, THEN THE Pack_Ingester SHALL return an error describing the failure and SHALL NOT persist any pack or index files to the repository object directory.
7. WHEN pack ingestion writes a new pack to disk, THE Pack_Ingester SHALL use temporary files and only persist them to their final paths atomically, so that a failure at any point leaves no partial pack or index in the object directory.

### Requirement 2: Connectivity Check

**User Story:** As a server operator, I want the handler to verify that all objects referenced by new ref targets are reachable in the ODB (including the just-received pack), so that the repository remains consistent after accepting a push.

#### Acceptance Criteria

1. WHEN ref updates introduce new targets (new_id is not the zero id), THE Connectivity_Checker SHALL resolve each new ref target to a commit (peeling annotated tags if necessary) and walk the commit graph, and for each commit encountered SHALL verify that its tree and all recursively referenced trees and blobs exist in the ODB.
2. WHILE walking the object graph from a new ref target, THE Connectivity_Checker SHALL stop traversal along a path when it encounters a commit reachable from any ref that existed prior to this push (i.e., a commit already present in the pre-push ref tips' ancestor set).
3. WHEN a referenced object (commit, tree, or blob) is not found in the ODB during the walk, THE Connectivity_Checker SHALL return an error identifying the missing object id, its expected type, and the ref that triggered the walk.
4. WHEN all new ref targets are reachable and every commit, tree, and blob encountered during the walk exists in the ODB, THE Connectivity_Checker SHALL report success.
5. WHEN a ref update is a deletion (new_id is the zero id), THE Connectivity_Checker SHALL skip the connectivity check for that ref.
6. WHILE traversing tree entries, IF the Connectivity_Checker encounters a submodule entry (tree entry with commit mode), THEN THE Connectivity_Checker SHALL skip that entry without treating it as a missing object.

### Requirement 3: Atomic Ref Transaction

**User Story:** As a server operator, I want ref updates to be applied atomically with compare-and-swap semantics, so that concurrent pushes to the same refs are detected and rejected rather than silently overwriting.

> **Note:** The compare-and-swap mechanism relies on `gix_ref::file::Store`, which uses POSIX filesystem lock files (`.lock` + atomic rename). This requires a local filesystem with rename atomicity and will not work on object-store backends (e.g., S3) without a custom ref storage implementation.

#### Acceptance Criteria

1. WHEN connectivity checks pass, THE Ref_Transactor SHALL map each parsed `Update` with a non-zero `old_id` to a `gix_ref::transaction::RefEdit` with `Change::Update`, setting `expected` to `PreviousValue::MustExistAndMatch` with the `old_id` and `new` to `Target::Object` with the `new_id`.
2. WHEN an `Update` has `old_id` equal to the zero id (ref creation), THE Ref_Transactor SHALL use `PreviousValue::MustNotExist` as the expected previous value and `Target::Object` with the `new_id` as the new value.
3. WHEN an `Update` has `new_id` equal to the zero id (ref deletion), THE Ref_Transactor SHALL produce a `Change::Delete` edit with `expected` set to `PreviousValue::MustExistAndMatch` with the `old_id`.
4. THE Ref_Transactor SHALL execute all ref edits as a single atomic transaction via `gix_ref::file::Store::transaction()`, using the ref name from the `Update` as the reflog message context.
5. IF any compare-and-swap check fails during transaction preparation, THEN THE Ref_Transactor SHALL return a per-ref error identifying the ref name and the expected value that did not match.
6. IF the transaction commit fails after successful preparation, THEN THE Ref_Transactor SHALL return an error that includes the underlying failure reason from the store.
7. WHEN the transaction commits successfully, THE Ref_Transactor SHALL remove the `.keep` file (if present) that was created during pack ingestion to allow garbage collection of the pack.
8. IF the `.keep` file removal fails after a successful transaction commit, THEN THE Ref_Transactor SHALL return an error identifying the file path that could not be removed.

### Requirement 4: Pipeline Step API

**User Story:** As a server integrator, I want each pipeline stage exposed as an independent public method, so that I can insert custom logic (authorization, secrets scanning, policy enforcement) between steps without being constrained by a hook trait.

#### Acceptance Criteria

1. THE Handler SHALL expose an `ingest_pack` method that accepts pack data bytes and returns the pack write outcome along with an ODB handle that resolves objects from the newly-written pack.
2. THE Handler SHALL expose a `check_connectivity` method that accepts the list of updates and the ingest outcome, and returns the set of object ids reachable from new ref targets that are not reachable from pre-existing refs.
3. THE Handler SHALL expose a `transact_refs` method that accepts the list of updates and ingest outcome, executes the atomic ref transaction, and returns per-ref status results. THE Handler SHALL allow `transact_refs` to be called without a prior `check_connectivity` call, enabling integrators to skip or replace the connectivity check.
4. WHEN the integrator calls `abort_pack`, THE Handler SHALL remove the `.keep` file associated with the ingested pack so that the pack becomes eligible for garbage collection by future repository maintenance.
5. WHEN `ingest_pack` succeeds, THE Handler SHALL ensure subsequent calls to `check_connectivity` and `transact_refs` can access the newly-written objects through the ODB.
6. IF `abort_pack` is called when no `.keep` file is present (because the pack was already committed or previously aborted), THEN THE Handler SHALL return success without error.
7. IF `check_connectivity` or `transact_refs` is called without a preceding successful `ingest_pack` call in the same session, THEN THE Handler SHALL return an error indicating that pack ingestion has not been performed.

### Requirement 5: Handler Orchestration and Lifecycle

**User Story:** As a server integrator, I want a single `ReceivePackHandler` struct that I can instantiate with a repository path and use either as the `Delegate` for the protocol layer (full pipeline) or as a step-by-step API, so that both simple and advanced use cases are supported.

#### Acceptance Criteria

1. THE Handler SHALL implement the `gix_protocol::receive_pack::Delegate` trait so it can be passed directly to `serve_v1` or `serve_v2` (and their async variants) for the convenience all-in-one path.
2. WHEN `receive` is called on the Handler via the Delegate trait, THE Handler SHALL execute the full pipeline in order: pack ingestion, connectivity check, ref transaction.
3. WHEN the full pipeline completes successfully, THE Handler SHALL return a `Response` with `UnpackStatus::Ok` and one `RefStatus::Ok` entry per successfully updated ref.
4. WHEN pack ingestion fails, THE Handler SHALL report `UnpackStatus::Error` with the underlying error message, set every requested ref update to `RefStatus::Rejected` with a message indicating the pack could not be unpacked, and skip all subsequent pipeline stages.
5. WHEN connectivity check fails, THE Handler SHALL report `UnpackStatus::Error` with a message identifying the missing object id, set every requested ref update to `RefStatus::Rejected` with a message indicating the connectivity failure, and skip the ref transaction.
6. THE Handler SHALL accept configuration via an `Options` struct that includes: an optional thread limit for pack indexing (where `None` means use all available cores), the `gix_hash::Kind` for object hashing, and the `gix_pack::data::input::Mode` controlling pack iteration integrity verification.
7. THE Handler SHALL be constructable from a repository path by opening the `gix_odb::Store` and `gix_ref::file::Store` at that path, returning an error if the path does not contain a valid object directory or ref store.
8. WHILE operating on a bare repository where the object directory is at the repository root, THE Handler SHALL resolve pack and index paths relative to the repository root rather than a `objects/` subdirectory.
9. IF the Delegate's `receive` method encounters an internal error not categorized as a pack ingestion or connectivity failure, THEN THE Handler SHALL return a `Response` with `UnpackStatus::Error` describing the failure and all ref updates set to `RefStatus::Rejected`.
