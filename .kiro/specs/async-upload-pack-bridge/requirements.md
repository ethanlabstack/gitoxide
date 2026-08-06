# Requirements Document

## Introduction

Provide an async transport bridge for the existing blocking `upload_pack` server-side plumbing in `gix-protocol`. This enables async server applications (e.g. tokio/axum-based GitPlane) to serve upload-pack protocol V2 requests without `spawn_blocking`, following the same pattern already established by `receive_pack::async_io`.

## Glossary

- **Async_Bridge**: The `upload_pack::async_io` submodule in `gix-protocol` that adapts async byte streams into the blocking upload-pack plumbing.
- **Upload_Pack_Module**: The existing `gix_protocol::upload_pack` module providing blocking server-side protocol V2 request parsing and response writing.
- **Async_Server_Feature**: The `async-server` Cargo feature flag that gates the async bridge module, parallel to the existing `blocking-server` feature.
- **BlockOn_Adapter**: The `futures_lite::io::BlockOn` wrapper that converts `AsyncRead`/`AsyncWrite` streams into synchronous `Read`/`Write` for bridging into blocking code.
- **AsyncRead_Stream**: A byte stream implementing `futures_io::AsyncRead + Unpin`.
- **AsyncWrite_Stream**: A byte stream implementing `futures_io::AsyncWrite + Unpin`.
- **Delegate**: The `upload_pack::Delegate` trait that server integrations implement to provide repository data for `ls-refs` and `fetch` commands.
- **FetchOutput**: The response payload struct containing acknowledgements, shallow updates, wanted refs, and optional pack data for a `fetch` command.
- **Async_FetchOutput**: A variant of `FetchOutput` where the `pack_data` field uses `Box<dyn AsyncRead + Unpin + Send>` instead of `Box<dyn io::Read + Send>`.
- **Outcome**: The `upload_pack::Outcome` enum describing the result of serving a single protocol V2 command.

## Requirements

### Requirement 1: Async Server Feature Flag

**User Story:** As a library consumer, I want an `async-server` feature flag so that I can opt into async upload-pack plumbing without pulling in blocking-server dependencies.

#### Acceptance Criteria

1. WHEN the `async-server` feature is enabled, THE gix-protocol crate SHALL expose the `upload_pack::async_io` submodule.
2. WHILE the `async-server` feature is disabled, THE gix-protocol crate SHALL not compile the `upload_pack::async_io` submodule.
3. THE Async_Server_Feature SHALL depend on `gix-transport/async-client`, `dep:async-trait`, `dep:futures-io`, and `futures-lite`.
4. THE Async_Server_Feature SHALL also depend on `dep:gix-object`, `dep:gix-pack`, and `dep:gix-traverse` to match the repository-access dependencies of `blocking-server`.
5. THE Async_Server_Feature SHALL be independent from `blocking-server` and `async-client`, allowing all three to coexist without conflict.

### Requirement 2: Async serve_v2 Function

**User Story:** As a server developer, I want an async `serve_v2` function so that I can handle upload-pack requests within an async runtime without blocking the executor.

#### Acceptance Criteria

1. THE Async_Bridge SHALL provide a public async function `serve_v2` accepting an AsyncRead_Stream, an AsyncWrite_Stream, and a mutable reference to a Delegate implementation.
2. WHEN `serve_v2` is called, THE Async_Bridge SHALL adapt the async streams using the BlockOn_Adapter and delegate to the blocking `upload_pack::serve_v2` implementation.
3. WHEN the blocking `serve_v2` completes, THE Async_Bridge SHALL flush the AsyncWrite_Stream before returning.
4. THE async `serve_v2` function SHALL return `Result<Outcome, Error>` using the same error and outcome types as the blocking version.

### Requirement 3: Async write_fetch_response Function

**User Story:** As a server developer, I want an async `write_fetch_response` function so that I can stream fetch responses with async pack data sources without blocking the executor.

#### Acceptance Criteria

1. THE Async_Bridge SHALL provide a public async function `write_fetch_response` accepting an AsyncWrite_Stream and a mutable reference to an Async_FetchOutput.
2. WHEN `write_fetch_response` is called with an Async_FetchOutput containing `pack_data`, THE Async_Bridge SHALL read pack bytes from the async reader and write them as sideband channel 1 data.
3. WHEN the Async_FetchOutput has no `pack_data`, THE Async_Bridge SHALL write only the acknowledgements, shallow-info, and wanted-refs sections.
4. THE async `write_fetch_response` function SHALL return `Result<u64, Error>` representing the number of raw pack bytes sent on sideband channel 1.
5. WHEN writing is complete, THE Async_Bridge SHALL flush the AsyncWrite_Stream before returning.

### Requirement 4: Async FetchOutput Type

**User Story:** As a server developer, I want an async-aware FetchOutput type so that I can provide pack data from an async source without converting it to a synchronous reader first.

#### Acceptance Criteria

1. THE Async_Bridge SHALL define an `AsyncFetchOutput` struct with the same fields as `FetchOutput` except the `pack_data` field SHALL use `Option<Box<dyn AsyncRead + Unpin + Send>>`.
2. THE AsyncFetchOutput SHALL provide a `new` constructor accepting an `impl AsyncRead + Unpin + Send + 'static` for pack data.
3. THE AsyncFetchOutput SHALL provide a `without_pack` constructor creating a response with no pack data.

### Requirement 5: Async write_ls_refs_response Function

**User Story:** As a server developer, I want an async `write_ls_refs_response` function so that I can write ls-refs responses over async transports.

#### Acceptance Criteria

1. THE Async_Bridge SHALL provide a public async function `write_ls_refs_response` accepting an AsyncWrite_Stream, a reference to an `LsRefs` request, and a slice of `Ref` values.
2. WHEN `write_ls_refs_response` is called, THE Async_Bridge SHALL adapt the async writer using the BlockOn_Adapter and delegate to the blocking `upload_pack::write_ls_refs_response`.
3. WHEN writing is complete, THE Async_Bridge SHALL flush the AsyncWrite_Stream before returning.
4. THE async `write_ls_refs_response` function SHALL return `Result<usize, Error>` representing the number of refs written.

### Requirement 6: Async write_v2_capability_advertisement Function

**User Story:** As a server developer, I want an async `write_v2_capability_advertisement` function so that I can send the initial capability advertisement over async transports.

#### Acceptance Criteria

1. THE Async_Bridge SHALL provide a public async function `write_v2_capability_advertisement` accepting an AsyncWrite_Stream and a slice of `Capability` values.
2. WHEN `write_v2_capability_advertisement` is called, THE Async_Bridge SHALL adapt the async writer using the BlockOn_Adapter and delegate to the blocking `upload_pack::write_v2_capability_advertisement`.
3. WHEN writing is complete, THE Async_Bridge SHALL flush the AsyncWrite_Stream before returning.
4. THE async `write_v2_capability_advertisement` function SHALL return `Result<(), Error>`.

### Requirement 7: Async parse_v2_request Function

**User Story:** As a server developer, I want an async `parse_v2_request` function so that I can parse incoming requests from async byte streams.

#### Acceptance Criteria

1. THE Async_Bridge SHALL provide a public async function `parse_v2_request` accepting an AsyncRead_Stream.
2. WHEN `parse_v2_request` is called, THE Async_Bridge SHALL adapt the async reader using the BlockOn_Adapter and delegate to the blocking `upload_pack::parse_v2_request`.
3. THE async `parse_v2_request` function SHALL return `Result<Request, Error>` using the same types as the blocking version.

### Requirement 8: Module Structure

**User Story:** As a maintainer, I want the async bridge to follow the established module pattern so that the codebase remains consistent and navigable.

#### Acceptance Criteria

1. THE Async_Bridge SHALL reside in a file at `gix-protocol/src/upload_pack/async_io.rs`.
2. THE existing blocking upload-pack code SHALL remain in `gix-protocol/src/upload_pack.rs` without modification to its public API.
3. THE `upload_pack::async_io` submodule SHALL be declared with `#[cfg(feature = "async-server")]` gating.
4. THE Async_Bridge module SHALL re-export the shared types (`Request`, `Command`, `LsRefs`, `Fetch`, `Feature`, `Capability`, `FetchNegotiation`, `Outcome`, `Error`, `Delegate`) from the parent module so consumers can use them without importing from two paths.

### Requirement 9: negotiate_fetch_with_repository Remains Synchronous

**User Story:** As a maintainer, I want the negotiation helper to stay synchronous so that pure-computation code is not unnecessarily wrapped in async.

#### Acceptance Criteria

1. THE `negotiate_fetch_with_repository` function SHALL remain synchronous and accessible from both the blocking module and the async bridge.
2. THE Async_Bridge SHALL not provide an async wrapper for `negotiate_fetch_with_repository`.

### Requirement 10: Shared Parsing Helpers Remain Synchronous

**User Story:** As a maintainer, I want internal parsing helpers to stay synchronous so that buffered-data operations remain simple and testable.

#### Acceptance Criteria

1. THE internal parsing helper functions (`parse_header_lines`, `parse_ls_refs_arguments`, `parse_fetch_arguments`, `parse_feature_line`) SHALL remain synchronous.
2. THE Async_Bridge SHALL reuse the synchronous parsing helpers by bridging the async input stream to blocking before invoking them.
