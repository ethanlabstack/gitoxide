# Implementation Plan: Async Receive-Pack Handler

## Overview

Implement a synchronous `ReceivePackHandler` in the `gix-protocol` crate's `receive_pack` module that orchestrates pack ingestion, connectivity checking, and atomic ref transactions for server-side `git push` handling. The handler exposes both a Pipeline Step API (individual public methods) and a `Delegate` trait implementation for all-in-one usage. Implementation uses `gix-pack`, `gix-ref`, `gix-traverse`, and `gix-odb` as building blocks, targeting bare repositories.

## Tasks

- [x] 1. Define core types, error enums, and module structure
  - [x] 1.1 Create the `receive_pack/handler` submodule with `Options`, `SessionState`, and `ReceivePackHandler` struct
    - Create `gix-protocol/src/receive_pack/handler/mod.rs` (or appropriate submodule path based on existing layout)
    - Define `Options` struct with `thread_limit`, `object_hash`, and `iteration_mode` fields
    - Define `SessionState` enum (`Fresh`, `PackIngested`, `Aborted`, `Committed`)
    - Define `ReceivePackHandler` struct with `odb`, `ref_store`, `repo_path`, `objects_dir`, `options`, `state`, and `ingest_outcome` fields
    - Implement `Default` for `Options`
    - _Requirements: 5.6, 5.7_

  - [x] 1.2 Define error types using `thiserror`
    - Create `OpenError`, `IngestError`, `ConnectivityError`, and `TransactError` enums
    - Follow existing `thiserror` patterns in gix-protocol (check `Cargo.toml` to confirm)
    - Include all variants specified in the design: `MalformedPack`, `MissingBase`, `MissingObject`, `NotIngested`, `Prepare`, `Commit`, `KeepFileRemoval`
    - _Requirements: 1.3, 1.6, 2.3, 3.5, 3.6, 3.8, 4.7_

  - [x] 1.3 Define result types: `IngestOutcome`, `ConnectivityResult`, `RefUpdateResult`, `RefUpdateStatus`, `TransactionResult`
    - `IngestOutcome` wraps `gix_pack::bundle::write::Outcome` with `object_count`
    - `ConnectivityResult` holds `new_objects: HashSet<gix_hash::ObjectId>`
    - `RefUpdateResult` has `ref_name: BString` and `status: RefUpdateStatus`
    - `RefUpdateStatus` is `Ok` or `Rejected { reason: String }`
    - `TransactionResult` holds `ref_results: Vec<RefUpdateResult>`
    - _Requirements: 1.4, 2.4, 3.5, 4.2_

- [x] 2. Implement handler construction (`open`)
  - [x] 2.1 Implement `ReceivePackHandler::open`
    - Accept `repo_path: PathBuf` and `options: Options`
    - Validate repository path exists and is a directory
    - Open `gix_odb::Store` at the objects directory
    - Open `gix_ref::file::Store` at the repository root
    - Resolve paths relative to bare repo root (no `objects/` subdirectory assumption for bare repos where objects dir is at root)
    - Return `OpenError` on failure
    - _Requirements: 5.7, 5.8_

  - [x] 2.2 Write unit tests for handler construction
    - Test: valid bare repo path → success
    - Test: non-existent path → `OpenError::InvalidPath`
    - Test: path without valid object directory → `OpenError::Odb`
    - Use `gix_testtools` for fixture creation
    - _Requirements: 5.7_

- [x] 3. Implement pack ingestion (`ingest_pack`)
  - [x] 3.1 Implement `ReceivePackHandler::ingest_pack`
    - Accept `pack_data: &mut dyn io::Read`
    - Verify state is `Fresh` (error if not)
    - Handle empty pack (zero objects) → produce outcome with `None` paths
    - Create `LookupRefDeltaObjectsIter` closure using the ODB for thin-pack resolution
    - Call `gix_pack::Bundle::write_to_directory` with appropriate options (thread_limit, object_hash, iteration_mode)
    - On success: transition state to `PackIngested`, store outcome
    - On failure: remain in `Fresh` state, return `IngestError`
    - Ensure atomic file operations (tempfiles only persist on success)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [x] 3.2 Write property test for malformed pack producing no artifacts (Property 2)
    - **Property 2: Malformed pack produces error with no filesystem artifacts**
    - Generate random invalid byte sequences as pack data
    - Assert `ingest_pack` returns error AND no new `.pack`, `.idx`, or `.keep` files appear in objects dir
    - **Validates: Requirements 1.6, 1.7**

  - [x] 3.3 Write property test for non-empty pack producing complete file set (Property 4)
    - **Property 4: Non-empty pack ingestion produces complete file set**
    - Generate valid packs with 1+ objects
    - Assert outcome has `Some` for `data_path`, `index_path`, and `keep_path`, and files exist on disk
    - **Validates: Requirements 1.4**

  - [x] 3.4 Write property test for thin pack with resolvable bases (Property 3)
    - **Property 3: Thin pack with resolvable bases ingests successfully**
    - Create repos with known base objects, generate thin packs referencing them
    - Assert `ingest_pack` succeeds and written pack index accounts for all objects
    - **Validates: Requirements 1.2**

- [x] 4. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement connectivity check (`check_connectivity`)
  - [x] 5.1 Implement `ReceivePackHandler::check_connectivity`
    - Accept `updates: &[Update]`
    - Verify state is `PackIngested` (return `NotIngested` otherwise)
    - Skip updates where `new_id` is zero (deletions)
    - For each non-deletion update: peel `new_id` to commit (handle annotated tags)
    - BFS/DFS walk from each new commit:
      - Stop traversal when encountering commits reachable from pre-existing ref tips
      - Verify each commit's tree exists, recursively verify all trees and blobs
      - Skip submodule entries (commit-mode tree entries)
    - On missing object: return `ConnectivityError::MissingObject` with oid, expected kind, ref name
    - On success: return `ConnectivityResult` with the set of new object ids
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 5.2 Write property test for connectivity on complete graphs (Property 5)
    - **Property 5: Connectivity check succeeds on complete object graphs**
    - Generate random DAGs of commit/tree/blob objects where all are present in ODB
    - Assert `check_connectivity` returns Ok
    - **Validates: Requirements 2.1, 2.4**

  - [x] 5.3 Write property test for deletion updates excluded (Property 7)
    - **Property 7: Deletion updates are excluded from connectivity checking**
    - Generate random updates with some `new_id = zero`
    - Assert those updates never trigger object lookup (connectivity check skips them)
    - **Validates: Requirements 2.5**

  - [x] 5.4 Write property test for submodule entries skipped (Property 8)
    - **Property 8: Submodule tree entries do not trigger missing-object errors**
    - Generate trees containing entries with commit mode (gitlinks)
    - Assert no missing-object error for those entries
    - **Validates: Requirements 2.6**

  - [x] 5.5 Write property test for connectivity walk termination at pre-existing tips (Property 6)
    - **Property 6: Connectivity walk terminates at pre-existing ref tips**
    - Generate object graphs where new commits have ancestors reachable from pre-existing refs
    - Assert walk stops at those ancestors and does not require objects below them
    - **Validates: Requirements 2.2**

- [ ] 6. Implement atomic ref transaction (`transact_refs`)
  - [x] 6.1 Implement Update-to-RefEdit mapping logic
    - Map `old_id ≠ zero, new_id ≠ zero` → `Change::Update` + `MustExistAndMatch(old_id)`
    - Map `old_id = zero, new_id ≠ zero` → `Change::Update` + `MustNotExist`
    - Map `old_id ≠ zero, new_id = zero` → `Change::Delete` + `MustExistAndMatch(old_id)`
    - _Requirements: 3.1, 3.2, 3.3_

  - [x] 6.2 Write property test for Update-to-RefEdit mapping (Property 1)
    - **Property 1: Update-to-RefEdit mapping preserves semantics**
    - Generate random `Update` commands with varied zero/non-zero id combinations
    - Assert produced `RefEdit` matches expected `Change` variant and `PreviousValue`
    - **Validates: Requirements 3.1, 3.2, 3.3**

  - [x] 6.3 Implement `ReceivePackHandler::transact_refs`
    - Accept `updates: &[Update]`
    - Verify state is `PackIngested` (return `NotIngested` otherwise)
    - Map all updates to `RefEdit` operations using the mapping from 6.1
    - Execute as single atomic transaction via `gix_ref::file::Store::transaction()`
    - On CAS mismatch: return per-ref `RefUpdateStatus::Rejected` (not a top-level error)
    - On infrastructure failure: return `TransactError::Prepare` or `TransactError::Commit`
    - On success: remove `.keep` file, transition state to `Committed`
    - If `.keep` removal fails: return `TransactError::KeepFileRemoval`
    - _Requirements: 3.4, 3.5, 3.6, 3.7, 3.8_

  - [~] 6.4 Write property test for successful transaction removing .keep file (Property 9)
    - **Property 9: Successful ref transaction removes the .keep file**
    - Set up sessions with ingested packs producing `.keep` files
    - Assert `.keep` file no longer exists after successful `transact_refs`
    - **Validates: Requirements 3.7**

- [ ] 7. Implement abort and lifecycle methods
  - [~] 7.1 Implement `ReceivePackHandler::abort_pack`
    - Remove `.keep` file if present
    - Transition state to `Aborted`
    - No-op if already in `Committed` or `Aborted` state (return Ok)
    - _Requirements: 4.4, 4.6_

  - [~] 7.2 Write property test for abort_pack removing .keep file (Property 10)
    - **Property 10: abort_pack removes the .keep file**
    - Set up sessions with ingested packs
    - Assert `.keep` file no longer exists after `abort_pack`
    - **Validates: Requirements 4.4**

  - [~] 7.3 Write unit tests for state machine enforcement
    - Test: `check_connectivity` before `ingest_pack` → `NotIngested`
    - Test: `transact_refs` before `ingest_pack` → `NotIngested`
    - Test: `abort_pack` after commit → Ok (no-op)
    - Test: double `abort_pack` → Ok (no-op)
    - _Requirements: 4.6, 4.7_

- [~] 8. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement Delegate trait
  - [~] 9.1 Implement `Delegate` for `ReceivePackHandler`
    - Implement `receive` method running full pipeline: ingest → connectivity check → transact
    - On pack ingestion failure: `UnpackStatus::Error`, all refs `Rejected`
    - On connectivity failure: `UnpackStatus::Error`, all refs `Rejected`
    - On ref transaction failure: `UnpackStatus::Error`, per-ref statuses from transaction
    - On success: `UnpackStatus::Ok`, one `RefStatus::Ok` per updated ref
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.9_

  - [~] 9.2 Write property test for pipeline failure yielding all refs rejected (Property 12)
    - **Property 12: Pipeline failure yields Error status with all refs rejected**
    - Generate various failure modes (malformed pack, missing objects)
    - Assert response has `UnpackStatus::Error` and every ref is `RefStatus::Rejected`
    - **Validates: Requirements 5.4, 5.5, 5.9**

  - [~] 9.3 Write property test for successful pipeline yielding all Ok (Property 13)
    - **Property 13: Successful pipeline yields Ok status for all refs**
    - Generate valid pushes with N refs
    - Assert response has `UnpackStatus::Ok` and exactly N `RefStatus::Ok` entries in order
    - **Validates: Requirements 5.3**

  - [~] 9.4 Write property test for ingested objects accessible through ODB (Property 11)
    - **Property 11: Ingested objects are accessible through ODB**
    - Ingest valid packs, then resolve every object id from the pack through ODB
    - Assert all objects are found
    - **Validates: Requirements 4.1, 4.5**

  - [~] 9.5 Write property test for new-object set correctness (Property 14)
    - **Property 14: check_connectivity returns the correct new-object set**
    - Generate repos with pre-existing refs and new pushes
    - Assert returned set equals objects reachable from new tips minus objects reachable from old tips
    - **Validates: Requirements 4.2**

- [ ] 10. Integration tests
  - [~] 10.1 Write integration test: full push round-trip via Delegate
    - Create bare repo fixture, push a branch with commits/trees/blobs
    - Verify refs are updated and objects accessible after push
    - Use `gix_testtools` for fixture setup
    - _Requirements: 5.1, 5.2, 5.3_

  - [~] 10.2 Write integration test: thin pack resolution
    - Create bare repo with base objects, push thin pack referencing them
    - Verify successful ingestion with resolved deltas
    - _Requirements: 1.2_

  - [~] 10.3 Write integration test: Pipeline Step API usage
    - Exercise integrator-style usage: `ingest_pack` → custom logic → `transact_refs` (skipping connectivity)
    - Verify `transact_refs` works without prior `check_connectivity`
    - _Requirements: 4.1, 4.3, 4.5_

  - [~] 10.4 Write integration test: CAS mismatch detection
    - Set up two handlers on same repo, one modifies a ref, second gets CAS mismatch
    - Verify per-ref rejection with appropriate error
    - _Requirements: 3.5_

  - [~] 10.5 Write integration test: connectivity walk termination on deep history
    - Repo with deep history, push one commit on top
    - Verify walk terminates quickly (doesn't traverse entire history)
    - _Requirements: 2.2_

- [~] 11. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document using `proptest`
- Unit tests validate specific examples and edge cases
- Integration tests use `gix_testtools` fixtures with real bare repositories
- The handler uses `thiserror` for errors (matching existing gix-protocol patterns — verify via `Cargo.toml`)
- All code is synchronous; no async runtime dependency
- Follow gitoxide conventions: no `.unwrap()`, use `.expect("reason")` or `?` in tests

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3"] },
    { "id": 1, "tasks": ["2.1"] },
    { "id": 2, "tasks": ["2.2", "3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "3.4", "5.1"] },
    { "id": 4, "tasks": ["5.2", "5.3", "5.4", "5.5", "6.1"] },
    { "id": 5, "tasks": ["6.2", "6.3"] },
    { "id": 6, "tasks": ["6.4", "7.1"] },
    { "id": 7, "tasks": ["7.2", "7.3", "9.1"] },
    { "id": 8, "tasks": ["9.2", "9.3", "9.4", "9.5"] },
    { "id": 9, "tasks": ["10.1", "10.2", "10.3", "10.4", "10.5"] }
  ]
}
```
