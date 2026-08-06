# Implementation Plan: Async Upload-Pack Bridge

## Overview

Add an `async-server` feature flag and `upload_pack::async_io` submodule to `gix-protocol`. The module bridges async byte streams into the existing blocking upload-pack plumbing using `futures_lite::io::BlockOn`, following the pattern established by `receive_pack::async_io`. A native async `write_fetch_response` streams pack data without blocking the executor.

## Tasks

- [x] 1. Add `async-server` feature flag and restructure upload_pack module
  - [x] 1.1 Add the `async-server` feature to `gix-protocol/Cargo.toml`
    - Add feature definition with dependencies: `gix-transport/async-client`, `dep:async-trait`, `dep:futures-io`, `futures-lite`, `dep:gix-object`, `dep:gix-pack`, `dep:gix-traverse`
    - Add a `[[test]]` entry for `async-server` tests: `name = "async-server"`, `path = "tests/async-server.rs"`, `required-features = ["async-server"]`
    - _Requirements: 1.1, 1.3, 1.4, 1.5_

  - [x] 1.2 Change `upload_pack` module gate in `gix-protocol/src/lib.rs`
    - Replace `#[cfg(feature = "blocking-server")]` on `pub mod upload_pack` with `#[cfg(any(feature = "blocking-server", feature = "async-server"))]`
    - This allows the shared types to be available under either feature
    - _Requirements: 1.1, 1.2, 8.3_

  - [x] 1.3 Add `async_io` submodule declaration inside `upload_pack.rs`
    - Add `#[cfg(feature = "async-server")] pub mod async_io;` near the top of `gix-protocol/src/upload_pack.rs`
    - Create the directory `gix-protocol/src/upload_pack/` and place `async_io.rs` inside it
    - Note: Rust edition 2024 supports both `upload_pack.rs` and `upload_pack/` directory coexisting (same as `receive_pack`)
    - _Requirements: 8.1, 8.3_

- [x] 2. Implement BlockOn-bridged async functions
  - [x] 2.1 Implement `serve_v2`, `parse_v2_request`, `write_ls_refs_response`, `write_v2_capability_advertisement` in `async_io.rs`
    - Create `gix-protocol/src/upload_pack/async_io.rs` with module-level doc comment
    - Implement `serve_v2` wrapping async streams with `BlockOn`, calling `super::serve_v2`, flushing output
    - Implement `parse_v2_request` wrapping async reader with `BlockOn`, calling `super::parse_v2_request`
    - Implement `write_ls_refs_response` wrapping async writer with `BlockOn`, calling `super::write_ls_refs_response`, flushing
    - Implement `write_v2_capability_advertisement` wrapping async writer with `BlockOn`, calling `super::write_v2_capability_advertisement`, flushing
    - Follow the exact pattern from `receive_pack::async_io` (borrow `&mut *output` for BlockOn, then flush)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 5.1, 5.2, 5.3, 5.4, 6.1, 6.2, 6.3, 6.4, 7.1, 7.2, 7.3_

  - [x] 2.2 Add re-exports of shared types from parent module
    - Re-export `Request`, `Command`, `LsRefs`, `Fetch`, `Feature`, `Capability`, `FetchNegotiation`, `Outcome`, `Error`, `Delegate` from `super`
    - Re-export `negotiate_fetch_with_repository` as-is (synchronous)
    - _Requirements: 8.4, 9.1, 9.2_

- [x] 3. Implement `AsyncFetchOutput` and native async `write_fetch_response`
  - [x] 3.1 Define `AsyncFetchOutput` struct in `async_io.rs`
    - Fields: `acknowledgements: Vec<Acknowledgement>`, `shallow_updates: Vec<ShallowUpdate>`, `wanted_refs: Vec<WantedRef>`, `pack_data: Option<Box<dyn AsyncRead + Unpin + Send>>`
    - Implement `new(pack_data: impl AsyncRead + Unpin + Send + 'static) -> Self`
    - Implement `without_pack() -> Self`
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 3.2 Implement native async `write_fetch_response` in `async_io.rs`
    - Write metadata sections (acks, shallow-info, wanted-refs) using BlockOn for small buffered writes
    - Stream pack data natively async: read chunks up to `MAX_SIDEBAND_DATA_BYTES`, encode as sideband channel 1 packets, write with `output.write_all().await`
    - Write flush packet and flush the stream before returning
    - Return `Result<u64, super::Error>` with total raw pack bytes sent
    - Use `gix_transport::packetline::blocking_io::encode::{band_to_write, flush_to_write, delim_to_write}` for packet framing
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 4. Checkpoint - Ensure module compiles
  - Run `cargo check -p gix-protocol --features async-server,sha1` to verify compilation
  - Run `cargo check -p gix-protocol --features blocking-server,sha1` to verify no regressions
  - Run `cargo clippy -p gix-protocol --features async-server,sha1` for lint checks
  - Ensure all checks pass, ask the user if questions arise.

- [x] 5. Write tests for async bridge functions
  - [x] 5.1 Create test harness in `gix-protocol/tests/async-server.rs`
    - Create the test binary entrypoint file gated on `async-server` feature
    - Add `mod upload_pack;` submodule declaration
    - Create `gix-protocol/tests/protocol/upload_pack.rs` (or inline) with async tests using `#[async_std::test]`
    - _Requirements: 1.1_

  - [x] 5.2 Write test: `serve_v2` bridges async transport for ls-refs
    - Build a valid protocol V2 ls-refs request as bytes using packetline encoding
    - Create `futures_lite::io::Cursor` for input and output
    - Implement a mock `Delegate` that returns known refs
    - Call `upload_pack::async_io::serve_v2` and verify `Outcome::LsRefs { refs_sent }` matches expected count
    - Parse output bytes with `StreamingPeekableIter` to verify correct packetline framing
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 5.3 Write test: `serve_v2` bridges async transport for fetch with pack data
    - Build a valid protocol V2 fetch request with `done=true` and a `want` line
    - Mock `Delegate::fetch` returns `FetchOutput` with acknowledgements and pack data
    - Call `upload_pack::async_io::serve_v2` and verify `Outcome::Fetch` fields
    - Parse output to verify acknowledgments section, packfile sideband framing, and flush packet
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [x] 5.4 Write test: `write_fetch_response` streams async pack data correctly
    - Create `AsyncFetchOutput::new(Cursor::new(pack_bytes))` with known acknowledgements
    - Call `write_fetch_response` on a `Cursor<Vec<u8>>` output
    - Verify returned byte count equals input pack data length
    - Parse output to verify sideband channel 1 framing: each packet has `\x01` prefix, concatenated payloads equal original pack bytes
    - Verify flush packet at end
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [x] 5.5 Write test: `write_fetch_response` without pack data writes only metadata sections
    - Create `AsyncFetchOutput::without_pack()` with acknowledgements and shallow updates populated
    - Call `write_fetch_response` and verify returned byte count is 0
    - Parse output to verify acknowledgments and shallow-info sections present, no packfile section
    - _Requirements: 3.3_

  - [x] 5.6 Write property test: BlockOn-bridged output matches blocking output byte-for-byte
    - **Property 1: BlockOn-bridged functions produce identical results to blocking counterparts**
    - For a set of representative inputs (ls-refs, fetch), call both blocking and async versions
    - Compare output byte buffers for equality
    - Compare return values for equality
    - **Validates: Requirements 2.2, 5.2, 6.2, 7.2, 10.2**

  - [x] 5.7 Write property test: pack data round-trip through sideband framing
    - **Property 2: Async write_fetch_response correctly frames pack data as sideband channel 1**
    - Test with various pack data sizes (empty, small, exactly MAX_SIDEBAND_DATA_BYTES, larger requiring multiple chunks)
    - Extract sideband payloads from output, concatenate, verify equals original input
    - **Validates: Requirements 3.2, 3.3**

  - [x] 5.8 Write property test: returned byte count equals pack data length
    - **Property 3: write_fetch_response byte count equals pack bytes consumed**
    - Test with known-length pack data inputs of varying sizes
    - Verify returned `u64` equals input length
    - **Validates: Requirements 3.4**

- [x] 6. Final checkpoint - Ensure all tests pass
  - Run `cargo test -p gix-protocol --features async-server,sha1` to verify all async-server tests pass
  - Run `cargo test -p gix-protocol --features blocking-server,sha1` to verify no regressions to blocking tests
  - Run `cargo clippy -p gix-protocol --features async-server,sha1` for lint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The module restructuring (task 1.3) follows the pattern of `receive_pack.rs` + `receive_pack/async_io.rs` which works in Rust edition 2024
- `negotiate_fetch_with_repository` is re-exported synchronously — it's pure computation over in-memory data with no I/O
- All tests use `futures_lite::io::Cursor` and `#[async_std::test]` runtime, matching existing async test patterns in `gix-protocol`
- Follow AGENTS.md: no `.unwrap()`, use `.expect("context")` or `?` with `gix_testtools::Result` / `Box<dyn std::error::Error>`
- The `async-server` feature is independent from both `blocking-server` and `async-client`

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3"] },
    { "id": 2, "tasks": ["2.1", "2.2"] },
    { "id": 3, "tasks": ["3.1"] },
    { "id": 4, "tasks": ["3.2"] },
    { "id": 5, "tasks": ["5.1"] },
    { "id": 6, "tasks": ["5.2", "5.3", "5.4", "5.5"] },
    { "id": 7, "tasks": ["5.6", "5.7", "5.8"] }
  ]
}
```
