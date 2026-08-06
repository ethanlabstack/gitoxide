# Implementation Plan: Experimental Upload-Pack Feature Gate

## Overview

Introduce an `experimental` Cargo feature on the `gix` crate that gates compilation of an in-process `BuiltinUploadPack` transport and `RepositoryDelegate`. The workspace `gitoxide` crate forwards the feature and includes it in `max`/`max-pure`. A `--builtin-upload-pack` CLI flag on `clone`/`fetch` activates the built-in path for `file://` URLs. A journey test validates end-to-end clone correctness.

## Tasks

- [x] 1. Wire up the `experimental` Cargo feature flag
  - [x] 1.1 Add `experimental` feature to `gix/Cargo.toml`
    - Add feature definition: `experimental = ["gix-protocol/blocking-server"]`
    - Include any additional optional deps needed (e.g., `gix-odb`, `gix-ref` if not already unconditional)
    - Ensure `cargo check -p gix --no-default-features` still compiles without experimental symbols
    - _Requirements: 1.1, 1.2, 1.5_

  - [x] 1.2 Forward `experimental` in workspace root `Cargo.toml`
    - Add `experimental = ["gix/experimental"]` to `[features]`
    - Add `"experimental"` to the `max` feature array
    - Add `"experimental"` to the `max-pure` feature array
    - Confirm `"experimental"` is NOT in `small`, `lean`, or `lean-async`
    - _Requirements: 4.1, 4.2, 4.3_

- [x] 2. Implement `BuiltinUploadPack` transport and `RepositoryDelegate`
  - [x] 2.1 Create `gix/src/transport/builtin_upload_pack.rs` module gated behind `#[cfg(feature = "experimental")]`
    - Define `BuiltinUploadPack` struct with fields: `path: BString`, `desired_version: Protocol`, state after handshake
    - Implement `BuiltinUploadPack::new(path, version, trace)` constructor
    - Implement `client::TransportWithoutIO` trait (`to_url`, `connection_persists_across_multiple_requests`, `configure`)
    - Implement `client::blocking_io::Transport` trait (`handshake`, `request`)
    - On `handshake()`: open target repo's ref store and ODB, build V2 capability advertisement, return `SetServiceResponse`
    - On `request()`: create pipe pair, drive `serve_v2()` via delegate, stream response back
    - Use proper error handling (no `.unwrap()`, return meaningful errors with path context)
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 2.2 Implement `RepositoryDelegate` in the same module
    - Define struct with `gix_ref::file::Store`, `gix_odb::Handle`, `gix_hash::Kind`
    - Implement `upload_pack::Delegate` trait
    - `ls_refs()`: iterate packed + loose refs, apply prefix filters, resolve symref targets and peeled OIDs
    - `fetch()`: call `negotiate_fetch_with_repository`, produce pack with only objects not in client's have set
    - Return errors that include the failing repository path or ref name
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 2.3 Register the module in `gix/src/transport/mod.rs` (or appropriate parent)
    - Add `#[cfg(feature = "experimental")] pub(crate) mod builtin_upload_pack;`
    - Ensure the module is only visible when feature is active
    - _Requirements: 1.2_

- [x] 3. Checkpoint - Ensure transport compiles
  - Run `cargo check -p gix --features experimental,blocking-network-client,sha1`
  - Run `cargo check -p gix --no-default-features --features sha1` to confirm no leakage without feature
  - Ensure all checks pass, ask the user if questions arise.

- [x] 4. Add `--builtin-upload-pack` CLI flag and plumb it through
  - [x] 4.1 Add the flag to `clone::Platform` in `src/plumbing/options/mod.rs`
    - Add `#[clap(long, hide = !cfg!(feature = "experimental"))]` field `builtin_upload_pack: bool`
    - _Requirements: 2.1, 1.3_

  - [x] 4.2 Add the flag to `fetch::Platform` in `src/plumbing/options/mod.rs`
    - Same pattern as clone
    - _Requirements: 2.1_

  - [x] 4.3 Add `builtin_upload_pack: bool` to `gitoxide-core::repository::clone::Options`
    - Thread the field through the `Options` struct
    - _Requirements: 2.1_

  - [x] 4.4 Add `builtin_upload_pack: bool` to `gitoxide-core::repository::fetch::Options` (if separate)
    - Thread the field through the fetch options struct
    - _Requirements: 2.1_

  - [x] 4.5 Implement early-exit error when flag is passed without `experimental` feature
    - In the CLI dispatch (`src/plumbing/main.rs`), when `builtin_upload_pack` is true and `cfg!(not(feature = "experimental"))`, bail with message: `--builtin-upload-pack requires the 'experimental' feature (build with --features experimental)`
    - Exit with non-zero code (anyhow error is sufficient)
    - _Requirements: 2.5_

- [x] 5. Integrate built-in transport into clone/fetch connection path
  - [x] 5.1 Wire the flag into `PrepareFetch` / `configure_connection` callback
    - When `builtin_upload_pack` is true and URL scheme is `file://`, use `BuiltinUploadPack` transport instead of `SpawnProcessOnDemand`
    - When URL scheme is not `file://`, ignore the flag and proceed with standard transport
    - Thread the flag via transport configuration (e.g., `configure_connection` closure on `PrepareFetch`)
    - _Requirements: 2.2, 2.3, 2.4, 3.1_

  - [x] 5.2 Wire the flag into fetch command's connection path
    - Same integration for the fetch subcommand
    - _Requirements: 2.2, 2.3, 2.4_

- [x] 6. Checkpoint - Ensure full build works
  - Run `cargo build --no-default-features --features max` to verify the integrated binary compiles
  - Run `cargo build --no-default-features --features small` to verify no experimental code leaks
  - Run `cargo clippy --no-default-features --features max` for lint checks
  - Ensure all checks pass, ask the user if questions arise.

- [x] 7. Add journey test for built-in upload-pack clone
  - [x] 7.1 Add journey test section in `tests/journey/gix.sh`
    - Guard with `if test "$kind" = "max" || test "$kind" = "max-pure"; then`
    - Create fixture repo with at least one branch and one tag
    - Clone with `$exe_plumbing clone --builtin-upload-pack "file://$fixture_repo" "$dest"`
    - Verify exit code 0
    - Verify `git -C "$dest" rev-parse HEAD` resolves to a valid commit
    - Clone same repo without the flag to `$dest_standard`
    - Compare `for-each-ref` output between both clones (refs and OIDs must match)
    - Verify branch and tag refs are both present in the builtin clone output
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 8. Write unit/integration tests
  - [ ]* 8.1 Write test: feature compilation gate
    - Verify `cargo check -p gix --no-default-features --features sha1` succeeds without experimental symbols
    - Verify `cargo check -p gix --features experimental,sha1` includes built-in upload-pack types
    - _Requirements: 1.2, 1.5_

  - [ ]* 8.2 Write test: CLI flag visibility
    - With `experimental` feature: verify `gix clone --help` includes `--builtin-upload-pack`
    - _Requirements: 1.3_

  - [ ]* 8.3 Write test: error message without experimental feature
    - Invoke binary built without `experimental`, pass `--builtin-upload-pack`, verify stderr contains expected error text and non-zero exit code
    - _Requirements: 2.5_

  - [ ]* 8.4 Write test: RepositoryDelegate returns error with path context for invalid repo
    - Open a non-existent path via `BuiltinUploadPack`, verify error message contains the path
    - _Requirements: 3.3_

  - [ ]* 8.5 Write property test for built-in/external equivalence
    - **Property 1: Built-in / external equivalence**
    - For repositories with varying ref structures, clone via both paths, compare ref maps and reachable object sets
    - **Validates: Requirements 2.6, 3.2**

  - [ ]* 8.6 Write property test for non-file scheme passthrough
    - **Property 2: Non-file scheme passthrough**
    - Generate random non-file URL schemes, verify `--builtin-upload-pack` flag has no effect on transport selection
    - **Validates: Requirements 2.4**

  - [ ]* 8.7 Write property test for error context inclusion
    - **Property 3: Error messages include context**
    - Generate random invalid paths and ref names, verify error messages contain the failing path or ref name
    - **Validates: Requirements 3.3**

  - [ ]* 8.8 Write property test for incremental fetch minimal pack
    - **Property 4: Incremental fetch produces minimal pack**
    - Generate commit DAGs, select random "have" subsets, fetch via built-in path, verify pack contains only objects not in have set
    - **Validates: Requirements 3.4**

- [x] 9. Final checkpoint - Ensure all tests pass
  - Run `cargo test --no-default-features --features max` for full test suite
  - Run journey tests with `max` build kind
  - Run `cargo clippy --no-default-features --features max`
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The `BuiltinUploadPack` transport lives in the `gix` crate (not `gix-transport`) because it depends on `gix-odb`, `gix-ref`, and `gix-protocol/blocking-server` — heavyweight deps that belong at the porcelain layer
- All code must follow AGENTS.md: no `.unwrap()`, proper error chains, `anyhow` in binaries
- The `experimental` feature is deliberately excluded from `small`, `lean`, and `lean-async` to keep production-oriented builds minimal
- Property tests use `proptest` (already a dev-dependency in `gix-protocol`)
- Journey tests are guarded by build kind to only run when experimental is compiled in

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "2.2"] },
    { "id": 2, "tasks": ["2.3"] },
    { "id": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "id": 4, "tasks": ["4.5", "5.1", "5.2"] },
    { "id": 5, "tasks": ["7.1"] },
    { "id": 6, "tasks": ["8.1", "8.2", "8.3", "8.4"] },
    { "id": 7, "tasks": ["8.5", "8.6", "8.7", "8.8"] }
  ]
}
```
