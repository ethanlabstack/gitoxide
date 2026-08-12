# Implementation Plan: Upload-Pack State Machine Simplification

## Overview

Non-breaking internal refactor of `gix-protocol`'s upload-pack server implementation. The monolithic `upload_pack.rs` (~800 LOC) is decomposed into focused modules (`parse.rs`, `response/`, `negotiate.rs`, `state.rs`) behind `pub(crate)` boundaries. The public API (`serve_v2`, `parse_v2_request`, `write_*`, `Delegate`) remains unchanged throughout Phase 1.

## Tasks

- [x] 1. Extract parsing into `parse.rs`
  - [x] 1.1 Create `gix-protocol/src/upload_pack/parse.rs` and move all parsing functions
    - Move `parse_v2_request`, `parse_header_lines`, `parse_feature_line`, `parse_ls_refs_arguments`, `parse_fetch_arguments`, `read_text_lines`, `validate_object_format`, and all parsing helper functions from `upload_pack.rs` into `upload_pack/parse.rs`
    - Mark module as `pub(crate)` in `upload_pack/mod.rs`
    - Re-export `parse_v2_request` from `upload_pack/mod.rs` so the public API is unchanged
    - Ensure `#[cfg(any(feature = "blocking-server", feature = "async-server"))]` gates are preserved on appropriate items
    - _Requirements: 8.1, 8.3_

  - [x] 1.2 Write property test for parse round-trip (Property 11)
    - **Property 11: Parse round-trip for V2 requests**
    - Generate random valid `Request` values (LsRefs and Fetch variants with valid OID hex lengths matching configured hash kind), serialize as pkt-line wire format, parse with `parse_v2_request`, and assert equivalence
    - Use `proptest` with `ProptestConfig { cases: 100, .. }`
    - **Validates: Requirements 1.1, 1.2, 1.3**

- [x] 2. Extract response writers into `response/` directory
  - [x] 2.1 Create `gix-protocol/src/upload_pack/response/mod.rs` with `SectionWriter` trait
    - Define the `pub(crate) trait SectionWriter` with `Input` associated type, `has_content`, and `write` methods
    - Create `response/` directory structure: `mod.rs`, `ack.rs`, `shallow.rs`, `wanted_refs.rs`, `packfile.rs`
    - _Requirements: 4.1, 4.3, 8.1_

  - [x] 2.2 Implement section writers and migrate `write_*` functions
    - Move `write_ls_refs_response`, `write_fetch_response`, `write_fetch_metadata_sections`, `write_v2_capability_advertisement`, and all formatting helpers (`format_acknowledgement_line`, `format_shallow_update_line`, `format_wanted_ref_line`, `format_ls_ref_line`, `matches_ref_prefixes`) into `response/`
    - Implement `AckSection`, `ShallowSection`, `WantedRefsSection`, `PackfileSection` as `SectionWriter` implementors
    - Re-export the public `write_*` functions unchanged from `upload_pack/mod.rs`
    - Existing public functions delegate to section writers internally
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 8.1_

  - [x] 2.3 Write property test for acknowledgment section framing (Property 2)
    - **Property 2: Acknowledgment section framing**
    - Generate random `Vec<Acknowledgement>` (with/without Ready); write via `AckSection`; parse output bytes and verify delimiter vs flush behavior
    - **Validates: Requirements 1.1, 1.2**

  - [x] 2.4 Write property test for optional section framing (Property 3)
    - **Property 3: Optional section framing and empty-section skipping**
    - Generate random combinations of shallow-info and wanted-refs entries (0–20 each); verify non-empty sections emit header+entries+delimiter, empty sections emit no bytes
    - **Validates: Requirements 1.3, 4.2**

  - [x] 2.5 Write property test for packfile sideband encoding (Property 4)
    - **Property 4: Packfile sideband encoding and size bounds**
    - Generate random byte vectors (0–128KB); write via `PackfileSection`; decode sideband channel 1 packets and verify concatenation equals original, each packet ≤ `MAX_SIDEBAND_DATA_BYTES`
    - **Validates: Requirements 1.4, 4.4**

- [x] 3. Checkpoint
  - Ensure all tests pass with `cargo test -p gix-protocol --features blocking-server`, ask the user if questions arise.
  - Ensure all tests pass with `cargo test -p gix-protocol --features async-server`, ask the user if questions arise.

- [x] 4. Extract negotiation into `negotiate.rs`
  - [x] 4.1 Create `gix-protocol/src/upload_pack/negotiate.rs` with `NegotiationState`
    - Implement `NegotiationState` struct with `BTreeSet` for have deduplication and `Vec` for ordered output
    - Implement `acknowledge_have()`, `acknowledgements()` truth-table logic, and `is_ready()` predicate
    - Move the core logic from `negotiate_fetch_with_repository` into `NegotiationState::evaluate()`
    - Keep `negotiate_fetch_with_repository` as a thin public wrapper that constructs `NegotiationState`, calls evaluate, and returns `FetchNegotiation`
    - Mark module as `pub(crate)`
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 8.1_

  - [x] 4.2 Write property test for negotiation ack correctness (Property 1)
    - **Property 1: Negotiation acknowledgement correctness**
    - Generate random `(done: bool, common_haves: Vec<ObjectId>)` pairs; verify acknowledgement list matches truth table: done+empty→empty, done+non-empty→ends with Ready, !done+empty→[NAK], !done+non-empty→only Common entries
    - **Validates: Requirements 1.5, 1.6, 1.7, 1.8**

  - [x] 4.3 Write property test for have deduplication (Property 5)
    - **Property 5: Have deduplication**
    - Generate random OID lists with intentional duplicates and a random existence predicate; verify `common_haves` output contains each acknowledged ID exactly once in first-seen order
    - **Validates: Requirements 3.2**

  - [x] 4.4 Write property test for readiness predicate (Property 6)
    - **Property 6: Readiness predicate**
    - Exhaustive combinations of `(done: bool, wait_for_done: bool, common_have_count: 0..100)`; verify readiness is true iff `done` is true
    - **Validates: Requirements 3.3, 3.5**

  - [x] 4.5 Write property test for want/want-ref unification (Property 7)
    - **Property 7: Want and want-ref unification**
    - Generate random wants + want-refs with random ref resolution; verify unified requested object set is the union of known wants and resolved want-ref targets, deduplicated
    - **Validates: Requirements 3.4**

- [x] 5. Introduce state machine types in `state.rs`
  - [x] 5.1 Create `gix-protocol/src/upload_pack/state.rs` with V2 and V1 phase types
    - Define `pub(crate) mod v2` with `Parsed`, `Negotiated`, `SendPack`, `Done` structs
    - Define `pub(crate) mod v1` with `Advertise`, `Negotiate`, `SendPack`, `Done` structs
    - Implement transition methods: `Parsed::negotiate()` → `Negotiated`, `Negotiated::resolve()` → `Either<SendPack, Done>`, `SendPack::send()` → `Done`
    - Transitions consume `self` by value to enforce ownership semantics
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 6.1_

  - [x] 5.2 Wire `serve_v2` to use state machine internally
    - Refactor `serve_v2` body to construct `v2::Parsed` from parse result, call `negotiate()`, then `resolve()`, then `send()` or flush
    - External behavior unchanged — same inputs produce same outputs
    - Verify async path in `async_io.rs` continues to work (it calls shared metadata writers)
    - _Requirements: 2.5, 8.1, 8.3_

- [x] 6. Checkpoint
  - Ensure all tests pass with `cargo test -p gix-protocol --features blocking-server` and `cargo test -p gix-protocol --features async-server`, ask the user if questions arise.

- [x] 7. Add V1 ref advertisement writer
  - [x] 7.1 Implement `write_v1_ref_advertisement` public function
    - New additive public function in `upload_pack/response/` (or `upload_pack/mod.rs`)
    - Format: each ref as `<hex-oid> <refname>\n`, capabilities NUL-appended to first line, terminated by flush packet
    - Uses shared `Writer` and `encode::flush_to_write` infrastructure from `response/`
    - Accepts `&[Ref]` and a capabilities list; returns `Result<usize, Error>` (number of refs written)
    - _Requirements: 6.2, 6.4, 8.1_

  - [x] 7.2 Write property test for V1 ref advertisement format (Property 9)
    - **Property 9: V1 ref advertisement format**
    - Generate random ref lists (1–50 refs); verify each output line is `<hex-oid> <refname>\n`, first line has capabilities after NUL, output terminates with flush
    - **Validates: Requirements 6.2**

- [x] 8. Final checkpoint
  - Ensure all tests pass with `cargo test -p gix-protocol --features blocking-server` and `cargo test -p gix-protocol --features async-server`, ask the user if questions arise.

- [x] 9. Phase 2 placeholder (future breaking changes)
  - [x] 9.1 Document Phase 2 migration plan
    - Add a `MIGRATION.md` or doc section outlining:
      - Step 2.1: Promote state types from `pub(crate)` to `pub`
      - Step 2.2: Split `Delegate` trait into `NegotiateDelegate` + `PackDelegate` with blanket compat impl
      - Step 2.3: Migrate error types from `thiserror` to `gix-error` patterns
      - Step 2.4: Implement V1 multi-round negotiation handler using `v1::Negotiate` state
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 7.2, 8.4_

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major extraction step
- Property tests validate universal correctness properties from the design document
- All Phase 1 changes are internal (`pub(crate)`) — no public API changes except the additive `write_v1_ref_advertisement`
- Test command: `cargo test -p gix-protocol --features blocking-server`
- `proptest` is already in `[dev-dependencies]`
- The async path (`async_io.rs`) shares `write_fetch_metadata_sections` with blocking; verify it compiles under `--features async-server` at each checkpoint

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["2.2"] },
    { "id": 3, "tasks": ["2.3", "2.4", "2.5"] },
    { "id": 4, "tasks": ["4.1"] },
    { "id": 5, "tasks": ["4.2", "4.3", "4.4", "4.5"] },
    { "id": 6, "tasks": ["5.1"] },
    { "id": 7, "tasks": ["5.2"] },
    { "id": 8, "tasks": ["7.1"] },
    { "id": 9, "tasks": ["7.2", "9.1"] }
  ]
}
```
