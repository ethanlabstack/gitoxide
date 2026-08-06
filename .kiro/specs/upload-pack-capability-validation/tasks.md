# Implementation Plan: Upload-Pack Capability Validation

## Overview

Add server-side capability validation to the `upload_pack` module in `gix-protocol`. This introduces a `ServerConfig` struct, validates the client's `object-format` feature against it at parse time, enforces correct OID hex lengths in fetch argument parsing, and updates all public function signatures (blocking and async) to accept the configuration. A performance fix hoists the sideband packet buffer out of the async write loop.

## Tasks

- [x] 1. Define ServerConfig and new Error variants
  - [x] 1.1 Add `ServerConfig` struct with `object_hash` field, derive `Debug, Clone, Copy, PartialEq, Eq`, implement `Default` returning `Sha1`
    - Add to `gix-protocol/src/upload_pack.rs` near the top (after existing type definitions)
    - _Requirements: 1.1, 1.2, 1.3_
  - [x] 1.2 Add `UnsupportedObjectFormat`, `InvalidObjectFormat`, and `ObjectIdLengthMismatch` variants to the `Error` enum
    - `UnsupportedObjectFormat { requested: BString, supported: BString }`
    - `InvalidObjectFormat { value: BString }`
    - `ObjectIdLengthMismatch { actual: usize, expected: usize, hash_kind: gix_hash::Kind }`
    - _Requirements: 2.2, 2.3, 3.1, 4.2_

- [x] 2. Implement validation logic and update parsers
  - [x] 2.1 Add `KNOWN_OBJECT_FORMATS` constant and `validate_object_format` function
    - Define `const KNOWN_OBJECT_FORMATS: &[&str] = &["sha1", "sha256"]`
    - Implement `validate_object_format(features: &[Feature], config: &ServerConfig) -> Result<(), Error>`
    - Do NOT use `gix_hash::Kind::from_str()` — compare against hardcoded list then match string representation
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 5.1, 5.2_
  - [x] 2.2 Update `parse_object_id` to accept `object_hash: gix_hash::Kind` parameter
    - Add hex length check: if `hex.len() != object_hash.len_in_hex()` return `Error::ObjectIdLengthMismatch`
    - Keep existing `from_hex` call for character validation after length check
    - _Requirements: 4.1, 4.2_
  - [x] 2.3 Update `parse_fetch_arguments` to accept `object_hash: gix_hash::Kind` parameter
    - Pass `object_hash` to all `parse_object_id` calls (want, have, shallow lines)
    - _Requirements: 4.1, 7.4_
  - [x] 2.4 Update `parse_v2_request` to accept `config: &ServerConfig` parameter
    - Call `validate_object_format(&features, config)` after parsing header lines
    - Pass `config.object_hash` to `parse_fetch_arguments`
    - _Requirements: 7.1, 7.3, 7.4_
  - [x] 2.5 Update `serve_v2` to accept `config: &ServerConfig` parameter
    - Pass `config` through to `parse_v2_request`
    - _Requirements: 6.1, 6.3_

- [x] 3. Update async bridge functions
  - [x] 3.1 Update `async_io::parse_v2_request` to accept `config: &super::ServerConfig`
    - Pass `config` through to `super::parse_v2_request` via `BlockOn`
    - _Requirements: 7.2_
  - [x] 3.2 Update `async_io::serve_v2` to accept `config: &super::ServerConfig`
    - Pass `config` through to `super::serve_v2` via `BlockOn`
    - _Requirements: 6.2_
  - [x] 3.3 Fix performance issue: hoist `packet_buf` Vec out of loop in `async_io::write_fetch_response`
    - Move `let mut packet_buf = Vec::with_capacity(super::MAX_SIDEBAND_DATA_BYTES + 10)` before the loop
    - Use `packet_buf.clear()` at the start of each iteration instead of allocating a new Vec
    - _Requirements: N/A (performance fix per design)_

- [x] 4. Checkpoint - Ensure all code compiles
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Update existing tests and add validation tests
  - [x] 5.1 Update all existing test calls to `parse_v2_request` and `serve_v2` to pass `&ServerConfig::default()`
    - This covers `parse_ls_refs_request`, `parse_fetch_request`, `parse_fetch_request_with_negotiation_arguments`, `parse_fetch_request_with_invalid_deepen_value`, `serve_ls_refs_with_prefix_filter`, `serve_fetch_with_pack_sideband`, and any others
    - _Requirements: 9.1, 9.3_
  - [x] 5.2 Add unit tests for `ServerConfig` and `validate_object_format`
    - Test `ServerConfig::default()` returns SHA-1
    - Test matching format accepted (sha1 config + sha1 feature → Ok)
    - Test mismatched format rejected (sha1 config + sha256 feature → UnsupportedObjectFormat)
    - Test invalid format rejected (sha1 config + "blake3" feature → InvalidObjectFormat)
    - Test absent object-format feature → Ok (assumes server's hash)
    - Test empty value → InvalidObjectFormat
    - Test non-UTF-8 value → InvalidObjectFormat
    - _Requirements: 1.2, 2.1, 2.2, 2.3, 2.4, 3.1_
  - [x] 5.3 Add unit tests for OID length enforcement
    - Test SHA-1 config rejects 64-char hex (returns ObjectIdLengthMismatch)
    - Test SHA-256 config rejects 40-char hex (returns ObjectIdLengthMismatch)
    - Test SHA-1 config accepts 40-char valid hex
    - Test SHA-256 config accepts 64-char valid hex
    - _Requirements: 4.1, 4.2_
  - [x] 5.4 Add unit test verifying `serve_v2` never calls delegate on validation failure
    - Send request with mismatched object-format, assert delegate methods not called
    - _Requirements: 6.3_
  - [x] 5.5 Add unit test for informational feature pass-through
    - Send request with `agent=git/test` and `object-format=sha1`, verify both features present in parsed result
    - _Requirements: 5.1, 5.2, 5.3_
  - [x] 5.6 Write property test for object-format validation (Property 1)
    - **Property 1: Object-format validation accepts matching, rejects mismatched or invalid**
    - Generate random `Kind` values and random feature value strings
    - Assert validation result matches expected classification (accept/unsupported/invalid)
    - **Validates: Requirements 2.1, 2.3, 3.1**
  - [x] 5.7 Write property test for OID length enforcement (Property 2)
    - **Property 2: OID length enforcement**
    - Generate random `Kind` values and random hex strings of varying lengths (0..128)
    - Assert `parse_object_id` succeeds iff length == `kind.len_in_hex()` and all chars are hex
    - **Validates: Requirements 4.1, 4.2, 7.4**
  - [x] 5.8 Write property test for non-object-format feature pass-through (Property 3)
    - **Property 3: Non-object-format features pass through without rejection**
    - Generate random feature names (excluding "object-format") and random optional values
    - Assert parsing succeeds and the feature appears in the result
    - **Validates: Requirements 5.1, 5.2, 5.3**

- [x] 6. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The design explicitly avoids `gix_hash::Kind::from_str()` due to compile-time feature gates — use `KNOWN_OBJECT_FORMATS` constant instead
- `ServerConfig` is `Copy` (single `gix_hash::Kind` field) so passing by reference or value is equally cheap
- The async bridge functions already exist and only need signature updates + pass-through of config
- All existing tests must continue to pass with `&ServerConfig::default()` to verify backward compatibility
- The `gix-protocol` crate uses `thiserror` (not `gix-error`), so new error variants follow the existing `thiserror` pattern
- Property tests should use `proptest` which is available in the gitoxide ecosystem

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3"] },
    { "id": 3, "tasks": ["2.4", "3.3"] },
    { "id": 4, "tasks": ["2.5", "3.1"] },
    { "id": 5, "tasks": ["3.2"] },
    { "id": 6, "tasks": ["5.1", "5.2", "5.3", "5.4", "5.5"] },
    { "id": 7, "tasks": ["5.6", "5.7", "5.8"] }
  ]
}
```
