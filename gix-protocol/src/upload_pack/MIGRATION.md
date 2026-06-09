# Phase 2 Migration Plan: Breaking Changes for Upload-Pack

## Overview

Phase 1 (completed) reorganized upload-pack internals into a type-safe state machine
(`state.rs`), composable section writers (`response/`), and separated negotiation logic
(`negotiate.rs`) — all behind `pub(crate)` boundaries with no public API changes.

Phase 2 introduces **breaking changes** that improve type safety, ergonomics, and
extensibility for downstream `Delegate` implementors:

- Typed public state tokens proving which protocol phase is active
- A split `Delegate` trait separating negotiation from pack generation
- Error type migration to `gix-error` patterns
- V1 multi-round negotiation using the existing `v1::Negotiate` state

These changes require a new major version of `gix-protocol`.

---

## Step 2.1: Promote State Types from `pub(crate)` to `pub`

### What Changes

The state machine types in `upload_pack/state.rs` become part of the public API:

```rust
// Before (Phase 1):
pub(crate) mod v2 { ... }
pub(crate) mod v1 { ... }

// After (Phase 2):
pub mod v2 {
    pub struct Parsed { ... }
    pub struct Negotiated { ... }
    pub struct SendPack { ... }
    pub struct Done;
}

pub mod v1 {
    pub struct Advertise;
    pub struct Negotiate { ... }
    pub struct SendPack;
    pub struct Done;
}
```

### Public API Surface

| Type | Purpose |
|------|---------|
| `v2::Parsed` | Holds a parsed fetch request, ready for negotiation |
| `v2::Negotiated` | Carries negotiation result, can resolve to SendPack or Done |
| `v2::SendPack` | Proof that pack transfer is authorized |
| `v2::Done` | Terminal state |
| `v1::Advertise` | V1 ref advertisement phase |
| `v1::Negotiate` | V1 multi-round want/have state with accumulated `wants` and `common` |
| `v1::SendPack` | V1 pack transfer (shares logic with V2) |
| `v1::Done` | Terminal state |
| `Either<L, R>` | Branch enum for `Negotiated::resolve()` |

### Migration for Downstream

No immediate action required — promotion is additive. Consumers who only use
`serve_v2()` see no change. Advanced consumers gain access to fine-grained
protocol phases for custom server implementations.

---

## Step 2.2: Split `Delegate` Trait

### Current Interface

```rust
pub trait Delegate {
    fn ls_refs(&mut self, request: &LsRefs) -> Result<Vec<Ref>, BoxError>;
    fn fetch(&mut self, request: &Fetch) -> Result<FetchOutput, BoxError>;
}
```

The problem: `fetch()` bundles negotiation input (object existence checks, ref
resolution) with pack generation into a single call. The server cannot negotiate
incrementally without the delegate producing pack data upfront.

### New Interface

```rust
/// Negotiation-phase delegate: provides object existence and ref resolution.
pub trait NegotiateDelegate {
    /// Check whether an object ID exists in the repository.
    fn object_exists(&mut self, id: &gix_hash::oid) -> bool;

    /// Resolve a want-ref name to an object ID.
    fn resolve_ref(&mut self, name: &BStr) -> Result<Option<gix_hash::ObjectId>, BoxError>;
}

/// Pack-generation delegate: produces pack data after negotiation.
pub trait PackDelegate {
    /// Produce pack data for the negotiated object set.
    ///
    /// The `token` parameter is an opaque proof that negotiation completed
    /// successfully — it can only be obtained from a `Negotiated` state.
    fn generate_pack(
        &mut self,
        token: NegotiationToken,
        wants: &[gix_hash::ObjectId],
        common: &[gix_hash::ObjectId],
    ) -> Result<Box<dyn io::Read + Send>, BoxError>;
}

/// Opaque token proving negotiation completed. Only constructible within
/// the state machine (private field prevents external construction).
pub struct NegotiationToken(pub(crate) ());
```

The existing `ls_refs()` method remains on `Delegate` (or moves to a
`LsRefsDelegate` trait if further granularity is desired).

### Blanket Compatibility Impl

```rust
/// Blanket implementation: any type implementing the old `Delegate` trait
/// automatically satisfies the new traits during the deprecation period.
impl<T: Delegate> NegotiateDelegate for T {
    fn object_exists(&mut self, _id: &gix_hash::oid) -> bool {
        // Delegates using the old interface don't support incremental
        // negotiation — the state machine falls back to calling `fetch()`.
        true // Conservative: assume all objects exist
    }

    fn resolve_ref(&mut self, _name: &BStr) -> Result<Option<gix_hash::ObjectId>, BoxError> {
        Ok(None) // Old delegates handle this inside `fetch()`
    }
}

impl<T: Delegate> PackDelegate for T {
    fn generate_pack(
        &mut self,
        _token: NegotiationToken,
        _wants: &[gix_hash::ObjectId],
        _common: &[gix_hash::ObjectId],
    ) -> Result<Box<dyn io::Read + Send>, BoxError> {
        // Fall back to calling the old monolithic `fetch()` interface.
        // This is a compatibility shim — new implementations should use
        // PackDelegate directly for better performance.
        unimplemented!("blanket impl delegates to old Delegate::fetch() internally")
    }
}
```

### Deprecation

```rust
#[deprecated(since = "X.Y.Z", note = "Use NegotiateDelegate + PackDelegate instead")]
pub trait Delegate { ... }
```

### Migration for Downstream

1. Replace `impl Delegate for MyServer` with `impl NegotiateDelegate` + `impl PackDelegate`
2. Move object-existence logic into `object_exists()`
3. Move ref resolution into `resolve_ref()`
4. Move pack generation into `generate_pack()`, accepting a `NegotiationToken`
5. Remove the old `fetch()` implementation

---

## Step 2.3: Migrate Error Types to `gix-error`

### Before (thiserror)

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Decode(#[from] gix_transport::packetline::decode::Error),
    #[error("Expected text packetline, got {line_type}")]
    NonTextPacketLine { line_type: &'static str },
    #[error("Malformed request header line {line:?}")]
    MalformedHeaderLine { line: BString },
    #[error("Delegate failed")]
    Delegate(#[source] BoxError),
    // ... more variants
}
```

### After (gix-error)

```rust
use gix_error::{message, message!, Exn, Message, ResultExt};

/// Upload-pack errors use the project-wide `gix-error` pattern.
pub type Error = Exn<Message>;

// Construction examples:

// Wrapping an I/O error with context:
writer.write_all(data)
    .or_raise(|| message("failed to write ack section"))?;

// Wrapping a decode error:
decode_pkt_line(input)
    .or_raise(|| message("failed to decode packetline during upload-pack request"))?;

// Standalone error (no underlying cause):
Err(message!("unsupported upload-pack command {command:?}").raise())

// Wrapping a delegate error:
delegate.generate_pack(token, &wants, &common)
    .or_raise(|| message("delegate failed during pack generation"))?;
```

### Key Differences

| Aspect | thiserror | gix-error |
|--------|-----------|-----------|
| Type | Enum with variants | `Exn<Message>` (single opaque type) |
| Pattern match | `match err { Error::Io(..) => ... }` | Not possible — use `.message()` for display |
| Construction | Variant constructors | `message(...)` + `.raise()` or `.or_raise()` |
| Wrapping | `#[from]` / `#[source]` | `.or_raise()` / `.and_raise()` |
| Trait impl | Implements `std::error::Error` | Does NOT impl `Error` — use `.into_error()` to convert |

### Migration for Downstream

Consumers matching on `Error` variants must switch to handling the opaque
`Exn<Message>` type. Since `Exn` does not implement `std::error::Error` directly:

```rust
// Before:
match upload_pack_result {
    Err(upload_pack::Error::Io(e)) => handle_io(e),
    Err(e) => log::error!("{e}"),
}

// After:
match upload_pack_result {
    Err(e) => {
        // Use Display for logging:
        log::error!("{e}");
        // Or convert to std::error::Error:
        return Err(e.into_error().into());
    }
}
```

---

## Step 2.4: V1 Multi-Round Negotiation Handler

### Design

V1 negotiation uses the existing `v1::Negotiate` state to track multi-round
want/have exchanges with per-have ACK/NAK responses:

```
Client                      Server
  │── want <oid>\n ───────────▶│
  │── want <oid>\n ───────────▶│  (accumulate wants)
  │── flush ──────────────────▶│  [Negotiate: wants populated]
  │                             │
  │── have <oid>\n ───────────▶│  → ACK <oid> continue  (if exists)
  │◀── ACK/NAK ───────────────│  → NAK                  (if not)
  │── have <oid>\n ───────────▶│
  │◀── ACK/NAK ───────────────│
  │── done ───────────────────▶│
  │◀── ACK <oid> ─────────────│  (final ACK of last common)
  │                             │
  │◀── sideband pack ─────────│  [SendPack] (shared with V2)
  │◀── flush ─────────────────│  [Done]
```

### Using `v1::Negotiate`

```rust
pub(crate) mod v1 {
    pub struct Negotiate {
        /// Object IDs the client wants.
        pub wants: Vec<gix_hash::ObjectId>,
        /// Common objects found during negotiation rounds.
        pub common: Vec<gix_hash::ObjectId>,
    }

    impl Negotiate {
        /// Process a `have` line. Returns ACK if object exists, NAK otherwise.
        pub fn have(
            &mut self,
            id: gix_hash::ObjectId,
            delegate: &mut impl NegotiateDelegate,
        ) -> HaveResponse {
            if delegate.object_exists(&id) {
                self.common.push(id);
                HaveResponse::AckContinue(id)
            } else {
                HaveResponse::Nak
            }
        }

        /// Client sent `done`. Transition to SendPack if common objects exist,
        /// or send a final NAK and then pack anyway (thin pack for full clone).
        pub fn done(self) -> (SendPack, Vec<gix_hash::ObjectId>, Vec<gix_hash::ObjectId>) {
            (SendPack, self.wants, self.common)
        }
    }
}

pub enum HaveResponse {
    AckContinue(gix_hash::ObjectId),
    Nak,
}
```

### Shared Infrastructure

V1 and V2 converge at `SendPack`:

- **Pack generation**: `PackDelegate::generate_pack()` with `NegotiationToken`
- **Sideband streaming**: `PackfileSection` writer (channel 1 packets)
- **Error reporting**: Sideband channel 3 error messages
- **Have deduplication**: `NegotiationState` (reusable for V1 multi-round)

### Key Behavioral Differences from V2

| Aspect | V2 | V1 |
|--------|----|----|
| Negotiation style | Batch (all haves at once per round) | Per-have ACK/NAK |
| Readiness | Explicit `done` from client | `done` from client |
| Response framing | Named sections with delimiters | Bare lines, sideband for pack |
| Statefulness | Stateless (done per request) | Stateful (multi-round on same connection) |
| Capabilities | Sent in command arguments | NUL-appended to first ref line |

---

## Migration Guide for Downstream `Delegate` Implementors

### Step-by-Step

1. **Upgrade `gix-protocol`** to the Phase 2 version.

2. **Replace `impl Delegate`** with the new traits:
   ```rust
   // Old:
   impl upload_pack::Delegate for MyServer { ... }

   // New:
   impl upload_pack::NegotiateDelegate for MyServer {
       fn object_exists(&mut self, id: &gix_hash::oid) -> bool { ... }
       fn resolve_ref(&mut self, name: &BStr) -> Result<Option<ObjectId>, BoxError> { ... }
   }

   impl upload_pack::PackDelegate for MyServer {
       fn generate_pack(
           &mut self,
           _token: NegotiationToken,
           wants: &[ObjectId],
           common: &[ObjectId],
       ) -> Result<Box<dyn io::Read + Send>, BoxError> { ... }
   }
   ```

3. **Update error handling** from pattern matching on `Error` variants to
   handling the opaque `Exn<Message>` type (see Step 2.3 above).

4. **Optional: Use state tokens directly** for custom server flows instead of
   the high-level `serve_v2()` entry point.

### Compatibility Period

During the transition:
- The old `Delegate` trait is `#[deprecated]` but remains functional
- Blanket impls bridge old `Delegate` to new `NegotiateDelegate + PackDelegate`
- `serve_v2()` continues to accept `&mut impl Delegate` via the blanket impls
- New code should implement `NegotiateDelegate + PackDelegate` directly

### Timeline

Phase 2 is gated on:
- Phase 1 being merged and stable
- Community feedback on the internal architecture
- A semver-major release of `gix-protocol`
