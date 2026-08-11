# Requirements Document

## Introduction

This feature hardens the existing V1 receive-pack protocol layer in gitoxide with proper capability validation and enforcement. The protocol framing (`parse_v1_request`, `serve_v1`, `write_v1_response`, `write_v1_ref_advertisement`) already exists, as does the `ReceivePackHandler` with pack ingestion, connectivity checking, and ref transactions. What is missing is the protocol-layer enforcement of constraints that real git clients expect: rejecting unsupported capabilities, early `object-format` mismatch detection, honoring the `atomic` flag for per-ref vs all-or-nothing failure semantics, `no-thin` enforcement, `quiet` suppression, and correct handling of edge cases like delete-only pushes and empty pushes with unknown capabilities.

## Glossary

- **Protocol_Layer**: The `gix-protocol::receive_pack` module responsible for parsing requests, validating capabilities, and writing responses — the code that sits between the transport and the `Delegate`.
- **Server_Capability_Set**: The defined set of receive-pack capabilities that the server advertises to clients and is willing to honor (e.g., `report-status`, `report-status-v2`, `side-band-64k`, `push-options`, `atomic`, `quiet`, `no-thin`, `object-format`).
- **Client_Capabilities**: The capabilities sent by the client on the first update command line during a V1 push request.
- **Capability_Validator**: The subsystem that compares Client_Capabilities against the Server_Capability_Set and rejects unsupported or invalid capabilities.
- **Handler**: The `ReceivePackHandler` struct that orchestrates pack ingestion, connectivity checking, and ref transaction for one push session.
- **Ref_Transactor**: The subsystem within Handler that maps parsed `Update` commands to `gix_ref` `RefEdit` operations and commits them.
- **Update**: A parsed ref update command containing `old_id`, `new_id`, and `ref_name` as provided by the Protocol_Layer.
- **Atomic_Mode**: A push mode where all ref updates succeed or all fail as a single unit — activated when the client sends the `atomic` capability.
- **Per_Ref_Mode**: The default push mode where each ref update is reported independently — a CAS failure on one ref does not prevent other refs from updating.
- **Object_Format**: The hash algorithm identifier (e.g., `sha1`, `sha256`) exchanged via the `object-format` capability to ensure client and server agree on object id format.
- **Thin_Pack**: A pack file containing ref-delta entries whose base objects are not included in the pack itself but exist in the receiving repository's ODB.

## Requirements

### Requirement 1: Server Capability Set Definition

**User Story:** As a server operator, I want a well-defined set of capabilities that the server advertises, so that clients know what features are supported and the server can reject anything outside that set.

#### Acceptance Criteria

1. THE Server_Capability_Set SHALL define the following capabilities as supported: `report-status`, `report-status-v2`, `side-band-64k`, `delete-refs`, `push-options`, `atomic`, `quiet`, `no-thin`, `object-format`, and `agent`.
2. WHEN writing a V1 ref advertisement, THE Protocol_Layer SHALL include all capabilities from the Server_Capability_Set in the NUL-separated capability string on the first ref line.
3. WHEN writing a V1 ref advertisement, THE Protocol_Layer SHALL include the `object-format` capability with a value matching the server's configured hash algorithm (e.g., `object-format=sha1`).
4. WHEN writing a V1 ref advertisement, THE Protocol_Layer SHALL include the `agent` capability with the gitoxide version identifier as its value.

### Requirement 2: Client Capability Validation

**User Story:** As a server operator, I want the protocol layer to reject pushes that request capabilities outside the advertised set, so that clients receive a clear error rather than undefined behavior.

#### Acceptance Criteria

1. WHEN a V1 push request is received, THE Capability_Validator SHALL compare each Client_Capability name against the Server_Capability_Set.
2. WHEN the Capability_Validator encounters a Client_Capability whose name is not in the Server_Capability_Set and is not the `agent` capability, THE Capability_Validator SHALL reject the push with a protocol error identifying the unsupported capability name.
3. WHEN the client sends the `agent` capability, THE Capability_Validator SHALL accept the capability regardless of its value without treating the capability as unsupported.
4. WHEN the client sends `report-status-v2` without also sending `side-band-64k`, THE Capability_Validator SHALL reject the push with a protocol error indicating that `report-status-v2` requires sideband transport.
5. WHEN the client sends `push-options` and the Server_Capability_Set includes `push-options`, THE Capability_Validator SHALL accept the capability and the Protocol_Layer SHALL parse the push-options section from the request.
6. WHEN all Client_Capabilities pass validation, THE Protocol_Layer SHALL proceed with delegate invocation using the validated request.

### Requirement 3: Object Format Early Validation

**User Story:** As a server operator, I want the server to reject pushes immediately at the protocol layer when the client's hash algorithm does not match the server's configured algorithm, so that incompatible clients get a clear error before any pack processing occurs.

#### Acceptance Criteria

1. WHEN the client sends `object-format=<algorithm>` and the algorithm does not match the server's configured `gix_hash::Kind`, THE Protocol_Layer SHALL reject the push with a protocol error identifying the mismatch (client algorithm vs server algorithm).
2. WHEN the client sends `object-format=<algorithm>` and the algorithm matches the server's configured hash algorithm, THE Protocol_Layer SHALL accept the capability and proceed normally.
3. WHEN the client does not send an `object-format` capability, THE Protocol_Layer SHALL assume `sha1` as the default and validate against the server's configured hash algorithm.
4. THE Protocol_Layer SHALL perform object-format validation before invoking the Delegate, so that no pack data is consumed when the hash algorithms are incompatible.

### Requirement 4: Atomic Push Mode

**User Story:** As a server operator, I want the handler to honor the `atomic` capability flag, applying all ref updates as a single atomic unit when requested and allowing partial success otherwise, so that clients get the push semantics they negotiated.

#### Acceptance Criteria

1. WHEN the client sends the `atomic` capability, THE Ref_Transactor SHALL execute all ref updates as a single atomic transaction — all updates succeed or all updates fail.
2. WHEN the client sends the `atomic` capability and any single ref update fails (CAS mismatch or other error), THE Ref_Transactor SHALL report all ref updates as rejected with a message indicating the atomic transaction failed.
3. WHEN the client does not send the `atomic` capability, THE Ref_Transactor SHALL operate in Per_Ref_Mode: each ref update is attempted independently and reported individually.
4. WHILE operating in Per_Ref_Mode, THE Ref_Transactor SHALL report `RefStatus::Ok` for refs that update successfully and `RefStatus::Rejected` for refs that fail, allowing partial success.
5. WHILE operating in Per_Ref_Mode, IF a ref update fails due to CAS mismatch, THEN THE Ref_Transactor SHALL continue processing remaining ref updates rather than aborting the entire push.
6. WHEN all ref updates succeed in Per_Ref_Mode, THE Ref_Transactor SHALL produce identical results to Atomic_Mode (all `RefStatus::Ok`).

### Requirement 5: No-Thin Pack Enforcement

**User Story:** As a server operator, I want the server to enforce thin-pack constraints based on negotiated capabilities, so that clients cannot send thin packs when they have not indicated thin-pack support.

#### Acceptance Criteria

1. WHEN the client sends the `no-thin` capability, THE Handler SHALL configure pack ingestion to reject thin packs (packs containing ref-delta entries whose base objects are not in the pack itself and must be resolved from the ODB).
2. WHEN the client does not send `no-thin`, THE Handler SHALL allow thin packs and resolve ref-delta base objects from the ODB during pack ingestion.
3. IF the client sends `no-thin` and the received pack contains unresolvable ref-delta entries, THEN THE Handler SHALL return an unpack error indicating that thin packs are not permitted in this session.

### Requirement 6: Quiet Capability Handling

**User Story:** As a server operator, I want the server to suppress sideband progress messages when the client requests quiet mode, so that scripted or non-interactive clients receive only essential output.

#### Acceptance Criteria

1. WHEN the client sends the `quiet` capability, THE Protocol_Layer SHALL suppress sideband progress messages (channel 2) in the response written by `write_v1_response`.
2. WHEN the client sends the `quiet` capability, THE Protocol_Layer SHALL still transmit sideband error messages (channel 3) in the response.
3. WHEN the client does not send the `quiet` capability, THE Protocol_Layer SHALL transmit all sideband messages (progress and error) provided by the Delegate response.

### Requirement 7: Delete-Only Push Handling

**User Story:** As a server operator, I want the server to correctly handle pushes that consist entirely of ref deletions (no new objects), so that no pack data is expected or processed.

#### Acceptance Criteria

1. WHEN all updates in a push request have `new_id` equal to the zero id (all deletions), THE Handler SHALL treat the push as having no pack data and skip pack ingestion entirely.
2. WHEN a push consists entirely of deletions, THE Handler SHALL skip the connectivity check (no new ref targets to verify).
3. WHEN a push consists entirely of deletions, THE Ref_Transactor SHALL process the deletion ref edits and report per-ref status for each deletion.
4. WHEN a push mixes deletions with creations or updates, THE Handler SHALL process pack data normally and perform connectivity checking for the non-deletion updates.

### Requirement 8: Empty Push (No-Op) Handling

**User Story:** As a server operator, I want the server to handle the case where a client sends a flush packet with no update commands (refs already up-to-date), so that no error is raised and a clean outcome is returned.

#### Acceptance Criteria

1. WHEN a V1 push request contains no update commands (immediate flush after ref advertisement), THE Protocol_Layer SHALL return a successful no-op outcome without invoking the Delegate.
2. WHEN a no-op push is received, THE Protocol_Layer SHALL write a flush packet to the output stream as acknowledgment.
3. WHEN a no-op push is received, THE Protocol_Layer SHALL report zero updates received, zero ref statuses sent, and no report-status payload written in the outcome.

### Requirement 9: Capability-Aware Response Writing

**User Story:** As a server operator, I want the response writer to honor the negotiated capabilities when formatting the push response, so that clients receive responses in the format they expect.

#### Acceptance Criteria

1. WHEN the client negotiated `report-status` or `report-status-v2`, THE Protocol_Layer SHALL write the unpack-status line and per-ref status lines in the response.
2. WHEN the client negotiated `side-band-64k`, THE Protocol_Layer SHALL wrap the report-status payload in sideband data channel frames.
3. WHEN the client did not negotiate `report-status` or `report-status-v2`, THE Protocol_Layer SHALL write only a flush packet as the response (no report-status payload).
4. WHEN the client negotiated `side-band-64k` but not `report-status`, THE Protocol_Layer SHALL write only a flush packet (sideband framing is only used when there is report-status data or sideband messages to send).
