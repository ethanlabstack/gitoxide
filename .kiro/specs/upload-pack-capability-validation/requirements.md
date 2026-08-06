# Requirements Document

## Introduction

Add capability validation to the `upload_pack` server implementation in `gix-protocol`. Currently, `serve_v2` ignores client-declared features entirely — a client could declare `object-format=sha256` against a SHA-1 server and the server would silently produce garbage. This feature introduces server-side configuration of the supported object hash, validates client-declared `object-format` against that configuration, and ensures OID parsing uses the correct hash length based on the negotiated format.

## Glossary

- **Upload_Pack_Server**: The server-side upload-pack plumbing in `gix-protocol/src/upload_pack.rs` and `upload_pack/async_io.rs` that parses protocol V2 requests and writes responses.
- **Server_Configuration**: A configuration struct passed to the upload-pack server functions that declares the object hash format the server supports (SHA-1 or SHA-256).
- **Object_Format_Feature**: The `object-format=<hash>` feature line that a client sends in a protocol V2 request header to declare which hash algorithm it expects.
- **Hash_Kind**: The `gix_hash::Kind` enum representing supported hash algorithms (`Sha1`, `Sha256`).
- **Feature**: The `upload_pack::Feature` struct storing a parsed feature name and optional value from a client request header.
- **Informational_Feature**: A client feature line (e.g. `agent=git/2.48.0`) that is purely informational and does not require server validation or negotiation.
- **Delegate**: The `upload_pack::Delegate` trait that server integrations implement to provide repository data for `ls-refs` and `fetch` commands.
- **OID_Parser**: The internal `parse_object_id` helper function that converts hex-encoded object IDs from request arguments into `gix_hash::ObjectId` values.

## Requirements

### Requirement 1: Server Configuration Type

**User Story:** As a server integrator, I want to pass server configuration to the upload-pack functions so that the server knows which object hash it supports.

#### Acceptance Criteria

1. THE Upload_Pack_Server SHALL define a public `ServerConfig` struct that contains the server's supported Hash_Kind.
2. THE ServerConfig SHALL default to `gix_hash::Kind::Sha1` when constructed with a `Default` implementation.
3. THE ServerConfig SHALL be `Clone`, `Debug`, and `Copy` to allow cheap reuse across requests.

### Requirement 2: Validate object-format Feature

**User Story:** As a server operator, I want the server to validate the client's declared `object-format` against the server's configuration so that hash mismatches are caught early rather than producing corrupt data.

#### Acceptance Criteria

1. WHEN a request contains an Object_Format_Feature with a value matching the server's configured Hash_Kind, THE Upload_Pack_Server SHALL accept the request and proceed with normal processing.
2. WHEN a request contains an Object_Format_Feature with a value that does not match the server's configured Hash_Kind, THE Upload_Pack_Server SHALL reject the request with an error indicating the unsupported object format.
3. WHEN a request contains an Object_Format_Feature with an unrecognized value (not `sha1` or `sha256`), THE Upload_Pack_Server SHALL reject the request with an error indicating the invalid object format.
4. WHEN a request contains no Object_Format_Feature, THE Upload_Pack_Server SHALL assume the client expects the server's configured Hash_Kind and proceed normally.

### Requirement 3: Error Reporting for Unsupported Object Format

**User Story:** As a client developer, I want a clear error message when my declared object-format is unsupported so that I can diagnose and correct the mismatch.

#### Acceptance Criteria

1. WHEN the server rejects a request due to an unsupported Object_Format_Feature, THE Upload_Pack_Server SHALL return an `Error` variant that includes the client-requested format and the server-supported format.
2. THE error message SHALL clearly state that the server does not support the requested object format.

### Requirement 4: OID Parsing Uses Configured Hash Length

**User Story:** As a server operator, I want OID parsing to enforce the correct hex length based on the negotiated hash so that malformed or mismatched object IDs are rejected rather than silently misinterpreted.

#### Acceptance Criteria

1. WHEN parsing object IDs from `fetch` command arguments (`want`, `have`, `shallow` lines), THE OID_Parser SHALL validate that the hex string length matches the server's configured Hash_Kind (40 hex characters for SHA-1, 64 hex characters for SHA-256).
2. IF a hex string length does not match the expected length for the configured Hash_Kind, THEN THE OID_Parser SHALL return an error indicating the invalid object ID length.

### Requirement 5: Informational Features Pass Through

**User Story:** As a server developer, I want informational features like `agent` to be preserved without validation so that they remain available for logging and diagnostics.

#### Acceptance Criteria

1. WHEN a request contains an Informational_Feature (e.g. `agent=<value>`), THE Upload_Pack_Server SHALL include the feature in the parsed request's feature list without validation or rejection.
2. THE Upload_Pack_Server SHALL treat any feature that is not `object-format` as an Informational_Feature.
3. THE parsed features SHALL remain accessible on the `Request` struct for caller inspection and logging.

### Requirement 6: serve_v2 Accepts Server Configuration

**User Story:** As a server integrator, I want the `serve_v2` function to accept a configuration parameter so that I can specify the server's capabilities for validation.

#### Acceptance Criteria

1. THE `serve_v2` function signature SHALL accept a reference to a ServerConfig in addition to its existing parameters (input, output, delegate).
2. THE async `serve_v2` function in the `async_io` module SHALL also accept a reference to a ServerConfig, maintaining API parity with the blocking variant.
3. WHEN `serve_v2` is called, THE Upload_Pack_Server SHALL validate client features against the provided ServerConfig before invoking the delegate.

### Requirement 7: parse_v2_request Accepts Server Configuration

**User Story:** As a server integrator, I want `parse_v2_request` to accept configuration so that feature validation and correct OID parsing happen at parse time.

#### Acceptance Criteria

1. THE `parse_v2_request` function signature SHALL accept a reference to a ServerConfig in addition to its existing input parameter.
2. THE async `parse_v2_request` function in the `async_io` module SHALL also accept a reference to a ServerConfig.
3. WHEN `parse_v2_request` is called, THE Upload_Pack_Server SHALL validate the Object_Format_Feature against the ServerConfig.
4. WHEN `parse_v2_request` is called, THE OID_Parser SHALL use the Hash_Kind from the ServerConfig to determine the expected hex length for object IDs.

### Requirement 8: Capability Advertisement Reflects Server Configuration

**User Story:** As a server integrator, I want the capability advertisement to accurately reflect the server's configured object format so that clients know what the server supports before sending requests.

#### Acceptance Criteria

1. WHEN constructing capability advertisements, THE Upload_Pack_Server SHALL include `object-format=sha1` if the server's configured Hash_Kind is SHA-1.
2. WHEN constructing capability advertisements, THE Upload_Pack_Server SHALL include `object-format=sha256` if the server's configured Hash_Kind is SHA-256.
3. THE `write_v2_capability_advertisement` function signature SHALL remain unchanged since it already accepts caller-provided `Capability` slices.

### Requirement 9: Backward Compatibility

**User Story:** As an existing user of the upload-pack API, I want the migration path to be clear so that adding configuration does not silently break my code.

#### Acceptance Criteria

1. THE Upload_Pack_Server SHALL provide a `ServerConfig::default()` that preserves SHA-1 behavior matching the current implementation.
2. IF the function signatures change to require ServerConfig, THEN THE Upload_Pack_Server SHALL document the migration in the API change notes.
3. THE existing `Request`, `Feature`, `Command`, `LsRefs`, `Fetch`, and `Outcome` types SHALL remain unchanged.
