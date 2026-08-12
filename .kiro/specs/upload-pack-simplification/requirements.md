# Requirements Document

## Introduction

This feature audits the existing V2 upload-pack server-side implementation in `gix-protocol` against the official git protocol specification, proposes simplifications through a state-machine approach, and sketches what a V1 upload-pack server would need. The work is scoped as protocol correctness analysis (framing, section ordering, negotiation semantics) rather than a formal line-by-line spec audit. Breaking changes are proposed as a follow-up; the initial deliverable is non-breaking internal refactors.

## Glossary

- **Upload_Pack_Server**: The server-side upload-pack plumbing in `gix-protocol/src/upload_pack.rs` and its async bridge in `async_io.rs`.
- **Builtin_Transport**: The in-process transport in `gix/src/transport/builtin_upload_pack.rs` that drives `serve_v2()` directly.
- **State_Machine**: A typed encoding of protocol phases (e.g., `Advertise`, `Negotiate`, `SendPack`) with compile-time enforcement of valid transitions.
- **V2_Protocol**: Git protocol version 2, command-based (ls-refs, fetch) with pkt-line framing.
- **V1_Protocol**: Git protocol version 1 with reference advertisement, want/have negotiation, and pack transfer in a single connection.
- **Delegate_Trait**: The `upload_pack::Delegate` trait that repository integrators implement to provide refs and pack data.
- **Negotiation_Round**: A single exchange of `have` lines from client and `ACK`/`NAK` responses from server during fetch negotiation.
- **Section_Framing**: The V2 convention of named sections (acknowledgments, shallow-info, wanted-refs, packfile) delimited by `0001` (delimiter) and `0000` (flush) packets.
- **Sideband**: Multiplexed channel encoding within pkt-line (channel 1 = pack data, channel 2 = progress, channel 3 = error).

## Requirements

### Requirement 1: Protocol Correctness Audit

**User Story:** As a maintainer, I want the upload-pack server to correctly implement V2 protocol framing and negotiation semantics, so that interoperability with canonical git clients is guaranteed.

#### Acceptance Criteria

1. WHEN a V2 `fetch` response contains an `acknowledgments` section without `ready`, THE Upload_Pack_Server SHALL terminate the response with a flush packet (`0000`) and omit subsequent sections.
2. WHEN a V2 `fetch` response contains an `acknowledgments` section with `ready`, THE Upload_Pack_Server SHALL follow the acknowledgments section with a delimiter packet (`0001`) before subsequent sections.
3. WHEN writing optional sections (shallow-info, wanted-refs), THE Upload_Pack_Server SHALL terminate each section with a delimiter packet (`0001`) before the next section.
4. WHEN a `packfile` section is present, THE Upload_Pack_Server SHALL write pack data as sideband channel 1 packets and terminate with a flush packet (`0000`).
5. WHEN the client sends `done` and no common objects exist (fresh clone), THE Upload_Pack_Server SHALL omit the `acknowledgments` section entirely and proceed directly to the packfile section.
6. WHEN the client sends `done` with common haves acknowledged, THE Upload_Pack_Server SHALL emit the `acknowledgments` section ending with `ready` followed by the packfile.
7. WHEN the client has not sent `done` and common haves exist, THE Upload_Pack_Server SHALL emit ACK lines for each common have without `ready` and terminate with flush.
8. WHEN the client has not sent `done` and no common haves exist, THE Upload_Pack_Server SHALL emit a single `NAK` line in the acknowledgments section.

### Requirement 2: State Machine Encoding

**User Story:** As a developer extending upload-pack, I want protocol phase transitions encoded as types, so that invalid transitions are caught at compile time rather than at runtime.

#### Acceptance Criteria

1. THE State_Machine SHALL represent at minimum the phases: `Advertise`, `LsRefs`, `FetchNegotiate`, `SendPack`, and `Done`.
2. WHEN a phase transition occurs, THE State_Machine SHALL consume the current state value and produce the next state value using Rust's ownership semantics.
3. THE State_Machine SHALL expose phase-specific data only in the state where that data is valid (e.g., pack writer only available in `SendPack`).
4. IF a caller attempts a transition that is invalid for the current state, THEN THE State_Machine SHALL produce a compile-time error via type system constraints.
5. THE State_Machine SHALL remain internal to `gix-protocol` and present the existing public API unchanged in the non-breaking refactor phase.

### Requirement 3: Negotiation Logic Simplification

**User Story:** As a maintainer, I want the fetch negotiation logic consolidated into a single clear decision path, so that correctness is easier to verify and multi-round negotiation is supportable.

#### Acceptance Criteria

1. THE Upload_Pack_Server SHALL separate negotiation state tracking from response serialization into distinct modules or types.
2. WHEN processing `have` lines, THE Upload_Pack_Server SHALL accumulate acknowledged object IDs in a set without duplicate entries.
3. WHEN the `wait-for-done` capability is active, THE Upload_Pack_Server SHALL defer sending pack data until the client explicitly sends `done`, regardless of how many common haves are found.
4. WHEN `want-ref` entries are present alongside `want` entries, THE Upload_Pack_Server SHALL resolve both into a unified set of requested object IDs for pack generation.
5. THE Upload_Pack_Server SHALL determine readiness (whether to send a pack) using a single predicate function that considers `done`, `wait-for-done`, and common have count.

### Requirement 4: Response Writer Simplification

**User Story:** As a developer, I want response writing expressed as a pipeline of section writers, so that adding new sections or changing framing requires minimal code changes.

#### Acceptance Criteria

1. THE Upload_Pack_Server SHALL implement each V2 response section (acknowledgments, shallow-info, wanted-refs, packfile) as an independent composable writer unit.
2. WHEN a section has no entries to write, THE Upload_Pack_Server SHALL skip that section entirely without emitting a section header or delimiter.
3. THE Upload_Pack_Server SHALL share section writer logic between blocking and async code paths without duplication of framing logic.
4. WHEN writing sideband pack data, THE Upload_Pack_Server SHALL use a configurable buffer size bounded by the pkt-line maximum payload (65515 bytes).

### Requirement 5: Delegate Trait Refinement Proposal

**User Story:** As a server integrator, I want a Delegate trait that separates concerns (ref enumeration, object existence, pack generation), so that I can implement only what my transport needs.

#### Acceptance Criteria

1. THE Delegate_Trait refinement SHALL split the current monolithic `fetch()` method into negotiation input (object existence check) and pack output (data generation) phases.
2. THE Delegate_Trait refinement SHALL allow the server to perform negotiation without requiring the delegate to produce pack data until readiness is confirmed.
3. THE Delegate_Trait refinement SHALL preserve backward compatibility by providing a default blanket implementation that delegates to the existing single-method interface.
4. WHERE the breaking-change phase is adopted, THE Delegate_Trait SHALL accept typed state tokens proving negotiation completed before requesting pack generation.

### Requirement 6: V1 Protocol Server Sketch

**User Story:** As a maintainer planning V1 support, I want the state machine design to accommodate V1's reference-advertisement-then-negotiate-then-pack flow, so that adding V1 does not require a rewrite.

#### Acceptance Criteria

1. THE State_Machine SHALL define V1-specific phases: `V1Advertise` (send ref advertisement with capabilities), `V1Negotiate` (multi-round want/have/ACK), and `V1SendPack` (sideband pack transfer).
2. WHEN in V1 advertisement phase, THE Upload_Pack_Server SHALL emit each ref as `<oid> <refname>\n` with capabilities appended to the first line, terminated by a flush packet.
3. WHEN in V1 negotiation phase, THE Upload_Pack_Server SHALL process `want` lines terminated by flush, then `have` lines with per-line `ACK`/`NAK` responses, repeating until `done`.
4. THE State_Machine SHALL share pack generation and sideband writing logic between V1 and V2 code paths through the composable section writers.
5. THE State_Machine SHALL allow V1 and V2 entry points to converge into shared `SendPack` handling after their protocol-specific negotiation completes.

### Requirement 7: Error Handling Alignment

**User Story:** As a maintainer, I want upload-pack errors to follow the project's migration path toward `gix-error`, so that error handling is consistent with the rest of the codebase.

#### Acceptance Criteria

1. WHILE `gix-protocol` still uses `thiserror`, THE Upload_Pack_Server SHALL continue using `thiserror` for error types in the non-breaking refactor phase.
2. WHERE the breaking-change phase is adopted, THE Upload_Pack_Server SHALL migrate error types to `gix-error` patterns (`Exn<Message>`, `or_raise`, `message!`).
3. IF a protocol framing error is detected during response writing, THEN THE Upload_Pack_Server SHALL return an error rather than writing a malformed response to the output stream.
4. IF the delegate returns an error during pack generation, THEN THE Upload_Pack_Server SHALL attempt to send a sideband error message (channel 3) before closing the connection.

### Requirement 8: Incremental Delivery

**User Story:** As a maintainer, I want the simplification delivered incrementally, so that each step is reviewable and the public API remains stable until an explicit breaking-change decision.

#### Acceptance Criteria

1. THE Upload_Pack_Server refactor SHALL be delivered as internal-only changes that do not alter the public API of `gix-protocol` in the first phase.
2. WHEN internal state machine types are introduced, THE Upload_Pack_Server SHALL keep them `pub(crate)` until the breaking-change phase promotes them.
3. THE Upload_Pack_Server SHALL maintain all existing tests passing after each incremental refactor step.
4. WHERE the breaking-change phase is adopted, THE Upload_Pack_Server SHALL document the migration path from the old Delegate trait to the new typed interface in a dedicated section of the crate documentation.
