# Bugfix Requirements Document

## Introduction

When a git client clones from a gitoxide-based server using HTTP protocol v2, the clone fails with `fatal: expected 'packfile', received 'acknowledgments'`. The root cause is in `negotiate_fetch_with_repository()` in `gix-protocol`, which always produces a non-empty acknowledgements list (`[Nak]` or `[Common(...)]`) but never accounts for whether the client sent `done`. Per the Git protocol v2 specification, when the server is ready to send pack data:

- If there are common objects, the `acknowledgments` section MUST include a `ready` line to signal that `packfile` follows.
- If there are no common objects (e.g., a fresh clone with `done`), the server should skip the `acknowledgments` section entirely and respond directly with `packfile`.

Without these signals, the client sees `acknowledgments` as the section header and does not expect `packfile` to come next, causing the "expected 'packfile', received 'acknowledgments'" error.

The bug affects both fresh clones (no haves) and fetches where the client already has some objects in common with the server.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN the client sends `done` AND has common haves with the server THEN the system produces an `acknowledgments` section containing only `ACK <id> common` lines without a `ready` line, causing the client to fail

1.2 WHEN the client sends `done` AND has NO haves (fresh clone) THEN the system produces an `acknowledgments` section containing `NAK` before the `packfile` section, causing the client to fail because no `ready` signal indicates packfile follows

1.3 WHEN the client receives an `acknowledgments` section without a `ready` line and pack data follows THEN the client fails with `fatal: expected 'packfile', received 'acknowledgments'`

### Expected Behavior (Correct)

2.1 WHEN the client sends `done` AND has common haves with the server THEN the system SHALL include `Acknowledgement::Ready` in the acknowledgments list after the `Common` entries to signal that a `packfile` section follows

2.2 WHEN the client sends `done` AND has NO haves (fresh clone) THEN the system SHALL produce an empty acknowledgements list so that the `acknowledgments` section is omitted entirely, allowing the response to begin directly with the `packfile` section

2.3 WHEN the client sends `done` AND the response includes both `acknowledgments` and `packfile` sections THEN the `acknowledgments` section SHALL contain a `ready` line signaling to the client that `packfile` follows

### Unchanged Behavior (Regression Prevention)

3.1 WHEN the client has NOT sent `done` AND has common haves THEN the system SHALL CONTINUE TO produce `acknowledgments` containing only `ACK <id> common` lines without `ready` (negotiation is still in progress)

3.2 WHEN the client has NOT sent `done` AND has NO common haves THEN the system SHALL CONTINUE TO produce `acknowledgments` containing only `NAK`

3.3 WHEN the fetch response includes pack data THEN the system SHALL CONTINUE TO emit the `packfile` section with sideband-encoded pack bytes

3.4 WHEN the `Delegate::fetch()` implementation manually constructs a `FetchOutput` with custom acknowledgements THEN the system SHALL CONTINUE TO write those acknowledgements as provided (delegate controls the response)
