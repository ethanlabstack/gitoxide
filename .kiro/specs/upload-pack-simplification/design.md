# Design Document: Upload-Pack State Machine Simplification

## Overview

This document describes the architectural refactoring of the `gix-protocol` upload-pack server implementation from a flat ~800 LOC module into a type-safe state machine with composable section writers. The design accommodates both V2 (current) and V1 (future) protocol flows while sharing pack generation and sideband infrastructure.

The refactoring is delivered in two phases:
1. **Non-breaking** (Phase 1): Internal reorganization behind `pub(crate)` boundaries. The public API (`serve_v2`, `parse_v2_request`, `write_*` functions, `Delegate` trait) remains unchanged.
2. **Breaking** (Phase 2): Promote typed state tokens to the public API, split the `Delegate` trait, migrate errors to `gix-error`.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Public API (unchanged in Phase 1)                  │
│  serve_v2() · parse_v2_request() · write_fetch_response()                │
│  write_ls_refs_response() · write_v2_capability_advertisement()          │
│  Delegate trait · FetchOutput · Outcome                                  │
└──────────────────────────────────────────────────────────────────────────┘
        │                         │                          │
        ▼                         ▼                          ▼
┌──────────────┐    ┌───────────────────────┐    ┌──────────────────────┐
│  state.rs    │    │  negotiate.rs         │    │  response/           │
│  (pub(crate))│    │  (pub(crate))         │    │  (pub(crate))        │
│              │    │                       │    │                      │
│  Protocol    │    │  NegotiationState     │    │  SectionWriter trait │
│  phase types │    │  readiness predicate  │    │  AckSection          │
│  V2/V1 enums │    │  have deduplication   │    │  ShallowSection      │
│              │    │                       │    │  WantedRefsSection   │
└──────────────┘    └───────────────────────┘    │  PackfileSection     │
                                                 └──────────────────────┘
```

### Request Flow (V2)

```
  Client                    Server (state machine)
    │                            │
    │── command=fetch ──────────▶│  [Idle] ──parse──▶ [Parsed]
    │                            │
    │                            │  [Parsed] ──negotiate──▶ [Negotiated]
    │                            │     • deduplicate haves
    │                            │     • resolve want-refs
    │                            │     • evaluate readiness predicate
    │                            │
    │◀── acks / NAK ────────────│  [Negotiated] ──write_acks──▶ [AcksSent]
    │                            │
    │   (if ready)               │  [AcksSent] ──delegate.pack()──▶ [SendPack]
    │◀── packfile section ──────│
    │◀── flush ─────────────────│  [SendPack] ──flush──▶ [Done]
    │                            │
    │   (if not ready)           │  [AcksSent] ──flush──▶ [Done]
    │◀── flush ─────────────────│     (client sends more haves next round)
```

### V1 Flow (Future)

```
  Client                    Server (state machine)
    │                            │
    │◀── ref advertisement ─────│  [V1Advertise] ──write_refs──▶ [V1Negotiate]
    │                            │
    │── want lines + flush ────▶│  [V1Negotiate] ──parse_wants──▶ ...
    │── have lines ────────────▶│     • per-have ACK/NAK
    │◀── ACK/NAK ──────────────│
    │── done ──────────────────▶│  [V1Negotiate] ──done──▶ [SendPack]
    │                            │
    │◀── sideband pack ────────│  [SendPack] (shared with V2)
    │◀── flush ─────────────────│  [SendPack] ──flush──▶ [Done]
```

## Module Structure

```
gix-protocol/src/upload_pack/
├── mod.rs              # Public API re-exports, serve_v2(), Delegate trait
├── async_io.rs         # Async bridge (existing, updated to use shared writers)
├── state.rs            # pub(crate) — Phase type definitions, transitions
├── negotiate.rs        # pub(crate) — NegotiationState, readiness predicate
├── parse.rs            # pub(crate) — Request parsing (extracted from mod.rs)
└── response/
    ├── mod.rs          # SectionWriter trait, ResponsePipeline
    ├── ack.rs          # Acknowledgments section writer
    ├── shallow.rs      # Shallow-info section writer
    ├── wanted_refs.rs  # Wanted-refs section writer
    └── packfile.rs     # Packfile sideband section writer
```

## Components and Interfaces

### State Machine Types (`state.rs`)

The state machine uses Rust's ownership and type system to enforce valid transitions at compile time. Each phase is a distinct type; transition methods consume `self` and return the next phase.

```rust
/// Protocol phases for upload-pack V2.
///
/// Each phase is a zero-sized or small struct that carries only the data
/// valid for that phase. Transition methods take `self` by value, preventing
/// reuse of consumed states.
pub(crate) mod v2 {
    use super::*;

    /// Initial state: request has been parsed, ready for negotiation.
    pub struct Parsed {
        pub request: Request,
    }

    /// Negotiation complete: acknowledgements and readiness determined.
    pub struct Negotiated {
        pub request: Request,
        pub negotiation: NegotiationResult,
    }

    /// Pack data is being sent (only reachable when ready=true).
    pub struct SendPack {
        pub request: Request,
    }

    /// Terminal state: response fully written.
    pub struct Done;
}

/// Protocol phases for upload-pack V1 (future).
pub(crate) mod v1 {
    /// Server is sending ref advertisement with capabilities.
    pub struct Advertise;

    /// Multi-round want/have negotiation in progress.
    pub struct Negotiate {
        pub wants: Vec<gix_hash::ObjectId>,
        pub common: Vec<gix_hash::ObjectId>,
    }

    /// Pack transfer (shared with V2 via SendPack logic).
    pub struct SendPack;

    /// Terminal state.
    pub struct Done;
}
```

#### Transition Enforcement

```rust
impl v2::Parsed {
    /// Perform negotiation, consuming the Parsed state.
    pub fn negotiate(self, delegate: &mut impl NegotiateDelegate)
        -> Result<v2::Negotiated, Error>
    {
        let negotiation = NegotiationState::new(&self.request)
            .evaluate(delegate)?;
        Ok(v2::Negotiated {
            request: self.request,
            negotiation,
        })
    }
}

impl v2::Negotiated {
    /// If ready, transition to SendPack. Otherwise, transition to Done.
    pub fn resolve(self) -> Either<v2::SendPack, v2::Done> {
        if self.negotiation.is_ready() {
            Either::Left(v2::SendPack { request: self.request })
        } else {
            Either::Right(v2::Done)
        }
    }
}

impl v2::SendPack {
    /// Write pack data and transition to Done.
    pub fn send(self, writer: &mut impl io::Write, pack: impl io::Read)
        -> Result<(v2::Done, u64), Error>
    {
        let bytes = PackfileSection.write(writer, pack)?;
        Ok((v2::Done, bytes))
    }
}
```

### Negotiation Logic (`negotiate.rs`)

The negotiation module separates state tracking from response serialization.

```rust
/// Accumulated negotiation state for a single fetch round.
pub(crate) struct NegotiationState {
    /// Deduplicated set of acknowledged have IDs.
    common_haves: BTreeSet<gix_hash::ObjectId>,
    /// Ordered list for response (preserves first-seen order).
    common_haves_ordered: Vec<gix_hash::ObjectId>,
    /// Whether the client sent `done`.
    done: bool,
    /// Whether `wait-for-done` is active.
    wait_for_done: bool,
}

/// The result of evaluating negotiation against repository state.
pub(crate) struct NegotiationResult {
    pub acknowledgements: Vec<Acknowledgement>,
    pub wanted_refs: Vec<WantedRef>,
    pub known_wants: Vec<gix_hash::ObjectId>,
    pub missing_wants: Vec<gix_hash::ObjectId>,
    pub common_haves: Vec<gix_hash::ObjectId>,
    pub unresolved_want_refs: Vec<BString>,
}

impl NegotiationResult {
    /// Single readiness predicate: should the server send a pack?
    ///
    /// Returns `true` when:
    /// - `done` is true (client signals negotiation complete), OR
    /// - `done` is false AND `wait_for_done` is false AND common haves
    ///   meet a threshold (for stateless multi-round, this is always false
    ///   without `done` in V2 — included for V1 compatibility)
    ///
    /// Returns `false` when:
    /// - `wait_for_done` is true AND `done` is false
    /// - `done` is false in V2 (always requires explicit `done`)
    pub fn is_ready(&self) -> bool {
        self.done
    }
}
```

#### Have Deduplication

```rust
impl NegotiationState {
    /// Record a have that exists in the repository. Deduplicates automatically.
    pub fn acknowledge_have(&mut self, id: gix_hash::ObjectId) {
        if self.common_haves.insert(id) {
            self.common_haves_ordered.push(id);
        }
    }
}
```

#### Acknowledgement Generation

```rust
impl NegotiationState {
    /// Generate the acknowledgement list based on current state.
    ///
    /// Truth table:
    /// | done  | common_haves.is_empty() | Result                            |
    /// |-------|-------------------------|-----------------------------------|
    /// | true  | true                    | [] (omit ack section entirely)    |
    /// | true  | false                   | [Common(...), ..., Ready]         |
    /// | false | true                    | [NAK]                             |
    /// | false | false                   | [Common(...), ...]  (no Ready)    |
    pub fn acknowledgements(&self) -> Vec<Acknowledgement> {
        if self.done {
            if self.common_haves_ordered.is_empty() {
                Vec::new() // fresh clone: omit section
            } else {
                let mut acks: Vec<_> = self.common_haves_ordered
                    .iter()
                    .copied()
                    .map(Acknowledgement::Common)
                    .collect();
                acks.push(Acknowledgement::Ready);
                acks
            }
        } else if self.common_haves_ordered.is_empty() {
            vec![Acknowledgement::Nak]
        } else {
            self.common_haves_ordered
                .iter()
                .copied()
                .map(Acknowledgement::Common)
                .collect()
        }
    }
}
```

### Composable Section Writers (`response/`)

Each V2 response section is an independent unit implementing a common trait. This allows the response pipeline to compose sections dynamically, skip empty ones, and share logic between blocking and async paths.

```rust
/// A section writer that can emit its content into a pkt-line stream.
///
/// Implementations handle their own section header and content.
/// The caller is responsible for the terminating delimiter or flush
/// based on protocol rules.
pub(crate) trait SectionWriter {
    /// The data this section needs to produce output.
    type Input;

    /// Returns true if this section has content to write.
    /// When false, the pipeline skips this section entirely (no header, no delimiter).
    fn has_content(input: &Self::Input) -> bool;

    /// Write the section content (header + entries) to the output.
    /// Does NOT write the terminating delimiter/flush — that's the pipeline's job.
    fn write(&self, output: &mut dyn io::Write, input: &Self::Input) -> Result<(), Error>;
}

/// Acknowledgments section writer.
pub(crate) struct AckSection;

impl SectionWriter for AckSection {
    type Input = [Acknowledgement];

    fn has_content(input: &Self::Input) -> bool {
        !input.is_empty()
    }

    fn write(&self, output: &mut dyn io::Write, acks: &Self::Input) -> Result<(), Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"acknowledgments")?;
        for ack in acks {
            writer.write_all(format_acknowledgement_line(*ack).as_ref())?;
        }
        Ok(())
    }
}

/// Shallow-info section writer.
pub(crate) struct ShallowSection;

/// Wanted-refs section writer.
pub(crate) struct WantedRefsSection;

/// Packfile sideband section writer.
pub(crate) struct PackfileSection;

impl PackfileSection {
    /// Write pack data as sideband channel 1 packets.
    ///
    /// Buffer size is bounded by `MAX_SIDEBAND_DATA_BYTES` (65515).
    /// Returns total raw pack bytes written.
    pub fn write(
        &self,
        output: &mut dyn io::Write,
        mut pack_data: impl io::Read,
    ) -> Result<u64, Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"packfile")?;

        let mut buffer = [0u8; MAX_SIDEBAND_DATA_BYTES];
        let mut total = 0u64;
        loop {
            let n = pack_data.read(&mut buffer)?;
            if n == 0 { break; }
            total += n as u64;
            encode::band_to_write(Channel::Data, &buffer[..n], writer.inner_mut())?;
        }
        Ok(total)
    }
}
```

#### Response Pipeline

The pipeline orchestrates section writers with correct framing:

```rust
/// Writes a complete V2 fetch response using composable section writers.
///
/// Framing rules:
/// - If acks are present and contain Ready → delimiter after acks, continue
/// - If acks are present without Ready → flush after acks, stop (no pack)
/// - Optional sections (shallow, wanted-refs) → delimiter after each
/// - Packfile section → flush terminates entire response
/// - Empty sections are skipped entirely (no header emitted)
pub(crate) fn write_fetch_response_pipeline(
    output: &mut dyn io::Write,
    response: &mut FetchOutput,
) -> Result<u64, Error> {
    let already_flushed = write_metadata_sections(
        output,
        &response.acknowledgements,
        &response.shallow_updates,
        &response.wanted_refs,
    )?;

    if already_flushed {
        return Ok(0);
    }

    let pack_bytes = match response.pack_data.as_mut() {
        Some(pack) => PackfileSection.write(output, pack)?,
        None => 0,
    };

    encode::flush_to_write(output)?;
    Ok(pack_bytes)
}
```

### V1/V2 Shared Infrastructure

The key insight is that after negotiation completes, both protocols need the same thing: write pack data as sideband channel 1 packets terminated by flush. The `PackfileSection` writer and the metadata section writers are protocol-version-agnostic.

**Shared between V1 and V2:**
- `PackfileSection` — sideband pack streaming (channel 1)
- `format_acknowledgement_line()` — ACK/NAK line formatting
- `parse_object_id()` — OID hex parsing with hash-kind validation
- `NegotiationState` — have deduplication and common tracking
- Pack generation (`generate_fetch_pack_data_with_repository`)

**V2-specific:**
- `AckSection`, `ShallowSection`, `WantedRefsSection` — named section framing with delimiters
- Command-based dispatch (`ls-refs` vs `fetch`)
- Header/argument parsing with `0001`/`0000` delimiters

**V1-specific (future):**
- Ref advertisement with capabilities on first line
- Per-have ACK/NAK (multi-round, stateful)
- Side-band (not side-band-64k) option
- `want` lines terminated by flush before `have` lines begin

### Async Bridge Strategy

The existing pattern (wrapping blocking I/O with `futures_lite::io::BlockOn`) is preserved for metadata sections since they are small buffered writes. Pack streaming uses native async I/O for large transfers.

The `write_fetch_metadata_sections` function is the shared core between blocking and async paths — it operates on `impl io::Write` (blocking) and is called via `BlockOn` from the async side. This function already exists and remains unchanged.

For the async `PackfileSection`, the response module provides:

```rust
/// Async pack streaming that avoids blocking the executor.
pub(crate) async fn write_packfile_async(
    output: &mut (impl AsyncWrite + Unpin),
    pack_data: &mut (impl AsyncRead + Unpin),
) -> Result<u64, Error> {
    // Write "packfile" header via BlockOn (single small write)
    {
        let mut blocking = futures_lite::io::BlockOn::new(&mut *output);
        let mut writer = Writer::new(&mut blocking);
        writer.enable_text_mode();
        writer.write_all(b"packfile")?;
    }
    // Stream pack data natively async
    let mut buffer = vec![0u8; MAX_SIDEBAND_DATA_BYTES];
    let mut packet_buf = Vec::with_capacity(MAX_SIDEBAND_DATA_BYTES + 10);
    let mut total = 0u64;
    loop {
        let n = pack_data.read(&mut buffer).await?;
        if n == 0 { break; }
        total += n as u64;
        packet_buf.clear();
        encode::band_to_write(Channel::Data, &buffer[..n], &mut packet_buf)?;
        output.write_all(&packet_buf).await?;
    }
    Ok(total)
}
```

### Delegate Trait Refinement (Phase 2 Proposal)

In the breaking-change phase, the monolithic `Delegate::fetch()` method splits into focused callbacks:

```rust
/// Phase 2 delegate trait — separates negotiation from pack generation.
pub trait Delegate {
    /// Return refs to advertise for ls-refs requests.
    fn ls_refs(&mut self, request: &LsRefs) -> Result<Vec<Ref>, BoxError>;

    /// Check whether an object ID exists in the repository.
    /// Called during negotiation to determine common haves.
    fn object_exists(&mut self, id: &gix_hash::oid) -> bool;

    /// Resolve a want-ref name to an object ID.
    /// Called during negotiation for want-ref entries.
    fn resolve_ref(&mut self, name: &BStr) -> Result<Option<gix_hash::ObjectId>, BoxError>;

    /// Produce pack data for the negotiated set of objects.
    ///
    /// Only called when readiness is confirmed. The `token` parameter
    /// is a proof-of-negotiation that can only be obtained from a
    /// completed `Negotiated` state.
    fn generate_pack(
        &mut self,
        token: NegotiationToken,
        wants: &[gix_hash::ObjectId],
        common: &[gix_hash::ObjectId],
    ) -> Result<Box<dyn io::Read + Send>, BoxError>;
}

/// Opaque token proving negotiation completed successfully.
/// Only constructible within the state machine.
pub struct NegotiationToken(pub(crate) ());
```

**Backward compatibility** (Phase 1): The existing `Delegate` trait with `fn fetch(&mut self, &Fetch) -> Result<FetchOutput, BoxError>` remains the public API. The internal state machine calls it at the appropriate phase boundary.

## Data Models

### Readiness Predicate Truth Table

| `done` | `wait_for_done` | `common_haves > 0` | Ready? | Behavior |
|--------|-----------------|---------------------|--------|----------|
| true   | any             | any                 | **yes** | Send pack |
| false  | true            | any                 | **no**  | Wait for more rounds |
| false  | false           | any                 | **no**  | V2 always needs `done` |

Note: In V1, the readiness logic differs — the server can send a pack after `done` is received in the have/ACK multi-round loop. The predicate accommodates this via a protocol-version parameter in the future.

### Section Framing Decision Table

| Acks present? | Contains Ready? | Shallow/WantedRefs? | Pack? | Framing |
|---------------|-----------------|---------------------|-------|---------|
| no            | n/a             | no                  | yes   | packfile section → flush |
| yes           | no              | n/a                 | n/a   | acks → flush (response done) |
| yes           | yes             | no                  | yes   | acks → delim → packfile → flush |
| yes           | yes             | yes                 | yes   | acks → delim → sections → delim each → packfile → flush |
| yes           | yes             | yes                 | no    | acks → delim → sections → delim each → flush |

### Wire Format Examples

**Fresh clone (done=true, no common):**
```
0000                          # flush (no ack section, straight to pack)
```
Wait — in a fresh clone the server omits the ack section entirely and writes pack directly:
```
000epackfile\n                # "packfile" section header
0024\x01PACK...               # sideband channel 1 pack data
0000                          # flush
```

**Negotiation round (done=false, some common):**
```
0015acknowledgments\n         # section header
001cACK <id> common\n         # for each common have
0000                          # flush (no ready = no pack follows)
```

**Final round (done=true, common haves exist):**
```
0015acknowledgments\n
001cACK <id> common\n
0007ready\n
0001                          # delimiter (more sections follow)
000epackfile\n
0024\x01PACK...
0000                          # flush
```

## Error Handling

### Error Types (Phase 1 — thiserror)

The existing `Error` enum in `upload_pack.rs` is preserved. No new public error variants are added in Phase 1.

Internal state-machine errors are mapped to existing variants:
- Negotiation failures → `Error::Delegate(BoxError)`
- Parse failures → existing `Error::MalformedArgument`, `Error::InvalidObjectId`, etc.
- I/O failures → `Error::Io`

### Error Propagation During Response Writing

If an I/O error occurs mid-response (e.g., during sideband pack streaming), the section writer returns `Err(Error::Io(...))` immediately. No attempt is made to write further sections — the output stream may be in an inconsistent state, and the transport layer is responsible for closing the connection.

### Delegate Error Recovery (Phase 2)

When the delegate's `generate_pack` fails after readiness is confirmed:

```rust
// In the SendPack state, if pack generation fails:
match delegate.generate_pack(token, &wants, &common) {
    Ok(pack_reader) => { /* stream pack normally */ }
    Err(e) => {
        // Attempt sideband error message (channel 3) before returning
        let msg = format!("pack generation failed: {e}");
        let _ = encode::band_to_write(Channel::Error, msg.as_bytes(), output);
        let _ = encode::flush_to_write(output);
        return Err(Error::Delegate(e));
    }
}
```

### Error Type Migration (Phase 2 — gix-error)

When the breaking-change phase is adopted:

```rust
// Before (thiserror):
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    // ...
}

// After (gix-error):
pub type Error = gix_error::Exn<gix_error::Message>;

// Construction:
use gix_error::{message, message!, ResultExt};
io_operation().or_raise(|| message!("failed to write ack section"))?;
```

## Incremental Delivery Plan

### Phase 1: Non-Breaking Internal Refactor

**Step 1.1** — Extract parsing into `parse.rs`
- Move `parse_v2_request`, `parse_header_lines`, `parse_feature_line`, `parse_ls_refs_arguments`, `parse_fetch_arguments`, and all parsing helpers to `upload_pack/parse.rs`
- Re-export `parse_v2_request` from `upload_pack/mod.rs` (unchanged public API)
- All existing tests pass without modification

**Step 1.2** — Extract response writers into `response/`
- Move `write_ls_refs_response`, `write_fetch_response`, `write_fetch_metadata_sections`, `write_v2_capability_advertisement`, and formatting helpers to `upload_pack/response/`
- Introduce `SectionWriter` trait as `pub(crate)`
- Implement `AckSection`, `ShallowSection`, `WantedRefsSection`, `PackfileSection`
- The public `write_*` functions delegate to section writers internally
- Re-export unchanged public signatures from `upload_pack/mod.rs`

**Step 1.3** — Extract negotiation into `negotiate.rs`
- Move `negotiate_fetch_with_repository` logic into `NegotiationState`
- Introduce the readiness predicate as an internal function
- The public `negotiate_fetch_with_repository` function becomes a thin wrapper
- Have deduplication uses `BTreeSet` (already present)

**Step 1.4** — Introduce state types in `state.rs`
- Define `v2::Parsed`, `v2::Negotiated`, `v2::SendPack`, `v2::Done`
- Define `v1::Advertise`, `v1::Negotiate`, `v1::SendPack`, `v1::Done`
- All types are `pub(crate)`
- Wire `serve_v2` to use state transitions internally
- External behavior and API unchanged

**Step 1.5** — Add V1 advertisement writer
- Implement `write_v1_ref_advertisement` using the shared formatting infrastructure
- Refs formatted as `<hex-oid> <refname>\n` with capabilities NUL-appended to the first line
- Flush terminates the advertisement
- This is additive (new public function, not breaking)

### Phase 2: Breaking Changes (Future)

**Step 2.1** — Promote state types to public API
- Change `pub(crate)` → `pub` for state machine types
- Document state transitions in crate-level docs

**Step 2.2** — Split Delegate trait
- Introduce `NegotiateDelegate` (object existence, ref resolution)
- Introduce `PackDelegate` (pack generation with token)
- Provide blanket impl mapping old `Delegate` → new traits
- Deprecate old `Delegate::fetch()` method

**Step 2.3** — Migrate errors to gix-error
- Replace `thiserror` enum with `gix_error::Exn<Message>`
- Follow patterns from `gix-error` migration guide in AGENTS.md

**Step 2.4** — Add V1 negotiation handler
- Implement multi-round want/have/ACK flow using `v1::Negotiate` state
- Share `SendPack` logic with V2 path
- Document V1 capabilities and behavior differences

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Negotiation acknowledgement correctness

*For any* combination of (`done`: bool, `common_haves`: set of OIDs), the acknowledgement list SHALL follow this invariant:
- If `done` is true and `common_haves` is empty → acknowledgements list is empty
- If `done` is true and `common_haves` is non-empty → acknowledgements ends with `Ready`, preceded by `Common` entries for each have
- If `done` is false and `common_haves` is empty → acknowledgements is exactly `[NAK]`
- If `done` is false and `common_haves` is non-empty → acknowledgements contains only `Common` entries (no Ready, no NAK)

**Validates: Requirements 1.5, 1.6, 1.7, 1.8**

### Property 2: Acknowledgment section framing

*For any* V2 fetch response, the acknowledgments section framing SHALL be:
- If acknowledgements contain `Ready` → section is followed by delimiter (`0001`)
- If acknowledgements do not contain `Ready` → section is followed by flush (`0000`) and no further sections are emitted

**Validates: Requirements 1.1, 1.2**

### Property 3: Optional section framing and empty-section skipping

*For any* V2 fetch response with optional sections (shallow-info, wanted-refs), each non-empty section SHALL emit its header followed by entries and a delimiter (`0001`). *For any* section with zero entries, the writer SHALL emit no bytes for that section (no header, no delimiter).

**Validates: Requirements 1.3, 4.2**

### Property 4: Packfile sideband encoding and size bounds

*For any* pack data byte stream, the packfile section writer SHALL:
- Emit a `"packfile"` section header
- Encode all data as sideband channel 1 packets where each packet's data payload is at most `MAX_SIDEBAND_DATA_BYTES` (65515) bytes
- Terminate with a flush packet (`0000`)
- The concatenation of all channel 1 payloads SHALL equal the original pack data byte-for-byte

**Validates: Requirements 1.4, 4.4**

### Property 5: Have deduplication

*For any* list of `have` OIDs (possibly containing duplicates) where a subset exists in the repository, the negotiation output's `common_haves` SHALL contain each acknowledged ID exactly once, in first-seen order.

**Validates: Requirements 3.2**

### Property 6: Readiness predicate

*For any* combination of (`done`: bool, `wait_for_done`: bool, `common_have_count`: usize), the readiness predicate SHALL return `true` if and only if `done` is true. When `wait_for_done` is true and `done` is false, readiness SHALL be false regardless of common_have_count.

**Validates: Requirements 3.3, 3.5**

### Property 7: Want and want-ref unification

*For any* fetch request containing both `want` OIDs and `want-ref` names where some refs resolve to OIDs, the set of requested object IDs used for pack generation SHALL be the union of known wants and resolved want-ref targets (deduplicated).

**Validates: Requirements 3.4**

### Property 8: Blocking and async output equivalence

*For any* `FetchOutput` (with deterministic pack data), the blocking `write_fetch_response` and the async `write_fetch_response` SHALL produce byte-identical output.

**Validates: Requirements 4.3**

### Property 9: V1 ref advertisement format

*For any* non-empty list of refs and any `ServerConfig`, the V1 ref advertisement writer SHALL produce output where:
- Each line is `<hex-oid> <refname>\n`
- The first line has capabilities appended after a NUL byte
- The output terminates with a flush packet

**Validates: Requirements 6.2**

### Property 10: V1/V2 sideband pack equivalence

*For any* pack data byte stream, the sideband channel 1 packets produced by the V1 pack writer and the V2 `PackfileSection` writer SHALL be identical (same framing, same chunking for the same buffer size).

**Validates: Requirements 6.4**

### Property 11: Parse round-trip for V2 requests

*For any* valid `Request` value (with valid OID lengths matching the configured hash), serializing it as pkt-line wire format and then parsing with `parse_v2_request` SHALL produce an equivalent `Request`.

**Validates: Requirements 1.1, 1.2, 1.3** (indirectly validates protocol correctness)

### Property 12: Error propagation on I/O failure

*For any* point during response writing where an I/O error occurs, the writer function SHALL return `Err(Error::Io(...))` without writing additional bytes to the output after the failed operation.

**Validates: Requirements 7.3**

## Testing Strategy

### Property-Based Tests

**Library**: `proptest` (already in `[dev-dependencies]`)
**Minimum iterations**: 100 per property

| Property | Generator Strategy |
|---|---|
| 1: Ack correctness | Random `(bool, Vec<ObjectId>)` pairs with random existence predicate |
| 2: Ack framing | Random `FetchOutput` with/without Ready; parse output wire bytes |
| 3: Optional sections | Random combinations of shallow/wanted-ref entries (0–20 each) |
| 4: Packfile encoding | Random byte vectors (0–128KB); verify sideband decode round-trip |
| 5: Have dedup | Random OID lists with intentional duplicates; random existence predicate |
| 6: Readiness | Exhaustive `(bool, bool, 0..100)` combinations |
| 7: Want unification | Random wants + want-refs with random ref resolution |
| 8: Blocking/async equiv | Random FetchOutput; compare byte outputs |
| 9: V1 advertisement | Random ref lists (1–50); parse output |
| 10: V1/V2 sideband | Random pack bytes; compare outputs |
| 11: Parse round-trip | Generate random valid Request; serialize then parse |
| 12: I/O error prop | Inject failures at random byte offsets using a faulty writer |

### Unit Tests (Example-Based)

- Fresh clone end-to-end: `done=true`, no haves → pack without ack section
- Multi-round negotiation: two rounds of haves before `done`
- `wait-for-done` blocks pack: haves acknowledged but no pack until `done`
- Empty `ls-refs` response (no matching refs): flush only
- Large pack chunking: verify packets near 65515-byte boundary
- Object-format validation: sha256 request against sha1 server rejected
- Malformed pkt-line input: verify error variant returned

### Integration Tests

- Round-trip against `git upload-pack --stateless-rpc` where feasible
- Wire-level compatibility with existing test fixtures
- Feature flag combinations: `blocking-server` alone, `async-server` alone, both
