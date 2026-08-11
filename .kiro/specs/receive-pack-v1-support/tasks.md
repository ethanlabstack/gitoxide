# Implementation Plan: Receive-Pack V1 Capability Validation & Enforcement

## Overview

Add protocol-layer capability validation and enforcement to the existing `gix-protocol::receive_pack` module. This involves a validation gate before delegate invocation, early object-format mismatch detection, atomic/per-ref transaction modes, no-thin enforcement, quiet suppression, delete-only push detection, and capability-aware response formatting. All changes live in `gix-protocol/src/receive_pack.rs` and `gix-protocol/src/receive_pack/handler.rs`.

## Tasks

- [x] 1. Define ServerCapabilitySet and ServerConfig
  - [x] 1.1 Add `SERVER_CAPABILITIES` constant and `ServerConfig` struct to `gix-protocol/src/receive_pack.rs`
    - Define `pub const SERVER_CAPABILITIES: &[&str]` with all 10 capability names
    - Add `ServerConfig` struct with `object_hash: gix_hash::Kind` field and `Default` impl
    - Add `server_capability_advertisement(config: &ServerConfig) -> Vec<(&'static str, Option<String>)>` helper
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 1.2 Write property test for advertisement completeness (Property 1)
    - **Property 1: Ref advertisement includes all server capabilities with correct values**
    - Generate random `Vec<AdvertisedRef>` and `ServerConfig`, call `write_v1_ref_advertisement` with `server_capability_advertisement`, parse output and verify all capabilities present with correct `object-format` and `agent` values
    - **Validates: Requirements 1.2, 1.3, 1.4**

- [x] 2. Implement capability validation
  - [x] 2.1 Add `validate_capabilities` function and new `Error` variants
    - Add `Error::UnsupportedCapability { name: BString }` variant
    - Add `Error::ReportStatusV2RequiresSideband` variant
    - Implement `validate_capabilities(capabilities: &[Capability]) -> Result<(), Error>` that rejects unknown capabilities (except `agent`) and checks `report-status-v2` requires `side-band-64k`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 2.2 Write property test for capability validation (Property 2)
    - **Property 2: Capability validation rejects unknown capabilities and accepts valid ones**
    - Generate random capability names (mix of valid from `SERVER_CAPABILITIES`, invalid strings, and `agent` with arbitrary values); assert valid subsets pass, invalid names rejected with correct error
    - **Validates: Requirements 2.2, 2.3, 2.6**

- [x] 3. Implement object-format validation
  - [x] 3.1 Add `validate_object_format` function and error variants
    - Add `Error::InvalidObjectFormat { value: BString }` variant
    - Add `Error::UnsupportedObjectFormat { requested: BString, supported: BString }` variant
    - Implement `validate_object_format(capabilities: &[Capability], config: &ServerConfig) -> Result<(), Error>` that checks hash algorithm match, defaults absent to sha1
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 3.2 Write property test for object-format validation (Property 3)
    - **Property 3: Object-format validation rejects mismatches and prevents delegate invocation**
    - Generate random (client_algo, server_algo) pairs from known formats; assert matching pairs pass, mismatches rejected
    - **Validates: Requirements 3.1, 3.2, 3.4**

- [x] 4. Update `serve_v1` to call validation before delegate
  - [x] 4.1 Update `serve_v1` signature to accept `&ServerConfig` and integrate validation calls
    - Add `config: &ServerConfig` parameter to `serve_v1`
    - Call `validate_capabilities` after parsing request (but before delegate)
    - Call `validate_object_format` after capability validation passes
    - Handle no-op push (empty command list) with flush response and early return
    - Update existing `serve_v1` tests to pass `&ServerConfig::default()`
    - _Requirements: 2.6, 3.4, 8.1, 8.2, 8.3_

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement SessionConfig and atomic/per-ref transaction modes
  - [x] 6.1 Add `SessionConfig` struct with `from_request` helper
    - Define `SessionConfig { no_thin: bool, atomic: bool }` with `Default` impl
    - Implement `SessionConfig::from_request(request: &Request) -> Self` that reads `no-thin` and `atomic` capabilities
    - _Requirements: 4.1, 5.1_

  - [x] 6.2 Implement `transact_refs_atomic` and `transact_refs_per_ref` methods on `ReceivePackHandler`
    - Add `transact_refs_atomic(&mut self, updates: &[Update]) -> Result<TransactionResult, TransactError>` that commits all refs in a single transaction, reporting all as rejected on any failure
    - Add `transact_refs_per_ref(&mut self, updates: &[Update]) -> Result<TransactionResult, TransactError>` that processes each ref independently, continuing on CAS failures
    - Update the existing `transact_refs` to dispatch based on `SessionConfig` (add `session_config: &SessionConfig` parameter)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 6.3 Write property test for atomic all-or-nothing semantics (Property 4)
    - **Property 4: Atomic mode — all-or-nothing ref transaction semantics**
    - Generate random update sets (2–10 refs), inject CAS failure on one; assert all refs reported same status (all Ok or all Rejected)
    - **Validates: Requirements 4.1, 4.2**

  - [x] 6.4 Write property test for per-ref independence (Property 5)
    - **Property 5: Per-ref mode — independent ref reporting**
    - Generate random update sets (2–10 refs), inject CAS failure on subset; assert failed refs = Rejected, passing refs = Ok, independent
    - **Validates: Requirements 4.3, 4.4, 4.5**

- [x] 7. Implement no-thin enforcement in `ingest_pack`
  - [x] 7.1 Update `ingest_pack` to accept `SessionConfig` and conditionally disable ODB thin-pack lookup
    - Add `session_config: &SessionConfig` parameter to `ingest_pack`
    - When `session_config.no_thin` is true, pass `None` as thin_pack_base_object_lookup so ref-deltas with missing bases fail
    - When `no_thin` is false, pass `Some(odb_handle)` as before
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 7.2 Write property test for no-thin enforcement (Property 6)
    - **Property 6: No-thin enforcement rejects thin packs**
    - Generate thin-pack-like data with missing bases; assert with no-thin: error; without no-thin + bases in ODB: success
    - **Validates: Requirements 5.1, 5.2, 5.3**

- [x] 8. Implement quiet suppression and capability-aware response writing
  - [x] 8.1 Update `write_v1_response` to suppress progress messages when `quiet` is negotiated
    - When `request.has_capability("quiet")` is true, skip sideband messages with `kind == Progress`
    - Always transmit error messages (channel 3) regardless of quiet
    - Ensure report-status payload is only written when `report-status` or `report-status-v2` is negotiated
    - Ensure sideband framing only wraps when `side-band-64k` is negotiated
    - _Requirements: 6.1, 6.2, 6.3, 9.1, 9.2, 9.3, 9.4_

  - [x] 8.2 Write property test for quiet suppression (Property 7)
    - **Property 7: Quiet suppresses progress but preserves errors**
    - Generate random `Response` with N progress + M error sideband messages; assert with quiet: 0 progress in output, M errors present; without quiet: N + M present
    - **Validates: Requirements 6.1, 6.2, 6.3**

  - [x] 8.3 Write property test for response format (Property 9)
    - **Property 9: Response format matches negotiated capabilities**
    - Generate random capability combinations × random Response; assert output structure matches decision matrix (report-status if negotiated, sideband framing if side-band-64k, flush-only otherwise)
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**

- [x] 9. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement delete-only push detection in `Delegate::receive`
  - [x] 10.1 Update `ReceivePackHandler::receive` to detect delete-only pushes and skip pack/connectivity
    - Check `request.updates.iter().all(|u| u.new_id.is_null())` for delete-only detection
    - When delete-only: skip `ingest_pack`, skip `check_connectivity`, proceed directly to `transact_refs` with deletion edits
    - When mixed: follow normal flow (ingest → connectivity → transact)
    - Pass `SessionConfig` through to `ingest_pack` and `transact_refs` calls
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [x] 10.2 Write property test for delete-only detection (Property 8)
    - **Property 8: Delete-only pushes skip pack ingestion and connectivity**
    - Generate random update sets with all/some/no deletions; assert all-delete → no pack ingestion; mixed → normal flow
    - **Validates: Requirements 7.1, 7.2, 7.3**

- [x] 11. Wire everything together and integration tests
  - [x] 11.1 Write integration tests for end-to-end validation flows
    - Test full push with valid capabilities through updated `serve_v1` with `ServerConfig`
    - Test push with unknown capability (e.g., `ofs-delta`) returns error before delegate invocation
    - Test sha256 mismatch returns early rejection
    - Test no-op push (empty flush) returns flush and no-op Outcome
    - Test delete-only push round-trip processes deletions without pack data
    - Test quiet mode filters progress messages but preserves errors in sideband output
    - _Requirements: 2.6, 3.4, 7.1, 8.1, 8.2, 8.3, 6.1, 9.1_

- [x] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests use `proptest` (already in workspace dev-dependencies) and validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- All code lives in `gix-protocol/src/receive_pack.rs` and `gix-protocol/src/receive_pack/handler.rs` — no new crates
- Follow gitoxide conventions: no `.unwrap()`, use `thiserror` for error enums (gix-protocol still uses thiserror), `gix_testtools` for test scaffolding
- The `serve_v1` signature change is breaking — existing callers must be updated to pass `&ServerConfig::default()`

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1", "6.1"] },
    { "id": 2, "tasks": ["2.2", "3.1"] },
    { "id": 3, "tasks": ["3.2", "4.1"] },
    { "id": 4, "tasks": ["6.2", "7.1", "8.1"] },
    { "id": 5, "tasks": ["6.3", "6.4", "7.2", "8.2", "8.3"] },
    { "id": 6, "tasks": ["10.1"] },
    { "id": 7, "tasks": ["10.2", "11.1"] }
  ]
}
```
