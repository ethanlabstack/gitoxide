# Design Document

## Overview

This document describes the architecture for the `upload_pack::async_io` module in `gix-protocol`. The module bridges async byte streams into the existing blocking upload-pack server plumbing, following the exact pattern established by `receive_pack::async_io`. The design uses a dual-strategy approach: BlockOn adapters for simple request/response operations, and a native async implementation for streaming pack data in `write_fetch_response`.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Async Server Application (tokio/axum)                    │
│                                                           │
│  AsyncRead input ─┐         ┌─ AsyncWrite output          │
└───────────────────┼─────────┼────────────────────────────┘
                    │         │
┌───────────────────▼─────────▼────────────────────────────┐
│  upload_pack::async_io                                    │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ BlockOn-bridged functions:                          │  │
│  │  • serve_v2()                                       │  │
│  │  • parse_v2_request()                               │  │
│  │  • write_ls_refs_response()                         │  │
│  │  • write_v2_capability_advertisement()              │  │
│  │                                                     │  │
│  │  AsyncRead/Write → BlockOn → blocking impl → flush  │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Native async function:                              │  │
│  │  • write_fetch_response()                           │  │
│  │                                                     │  │
│  │  AsyncFetchOutput.pack_data → sideband framing →    │  │
│  │  async write to output stream                       │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ Re-exported types:                                  │  │
│  │  Request, Command, LsRefs, Fetch, Feature,          │  │
│  │  Capability, FetchNegotiation, Outcome, Error,      │  │
│  │  Delegate, AsyncFetchOutput                         │  │
│  └─────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────┘
                    │         │
┌───────────────────▼─────────▼────────────────────────────┐
│  upload_pack (blocking)                                   │
│  serve_v2, parse_v2_request, write_ls_refs_response,      │
│  write_v2_capability_advertisement, write_fetch_response, │
│  negotiate_fetch_with_repository                          │
└───────────────────────────────────────────────────────────┘
```

## Module Structure

### File Layout

```
gix-protocol/src/
├── upload_pack.rs                  # Existing blocking implementation (unchanged)
├── upload_pack/
│   └── async_io.rs                 # New async bridge module
└── lib.rs                          # Module declaration with cfg gate
```

### Feature Flag Configuration

In `gix-protocol/Cargo.toml`:

```toml
## If set, async server-side upload-pack plumbing is available for in-process transports.
async-server = [
    "gix-transport/async-client",
    "dep:async-trait",
    "dep:futures-io",
    "futures-lite",
    "dep:gix-object",
    "dep:gix-pack",
    "dep:gix-traverse",
]
```

In `gix-protocol/src/lib.rs`, the module is conditionally declared:

```rust
#[cfg(feature = "async-server")]
pub mod upload_pack;
```

Note: Since `upload_pack` is already gated on `blocking-server`, the module declaration needs restructuring. The blocking code moves to `upload_pack.rs` (the file already there) and the `async_io` submodule is declared within it with its own cfg gate:

```rust
// In upload_pack.rs (or upload_pack/mod.rs if restructured)
/// Async transport integration for upload-pack server plumbing.
#[cfg(feature = "async-server")]
pub mod async_io;
```

The parent `upload_pack` module gate in `lib.rs` becomes:

```rust
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub mod upload_pack;
```

This allows the shared types (`Request`, `Command`, `LsRefs`, etc.) to be available under either feature, while the `async_io` submodule is only available with `async-server`.

## Components and Interfaces

### Component 1: BlockOn-Bridged Functions

These functions follow the identical pattern from `receive_pack::async_io`:

1. Wrap async streams with `futures_lite::io::BlockOn`
2. Call the blocking implementation
3. Flush the async output stream
4. Return the result

```rust
use futures_io::{AsyncRead, AsyncWrite};
use futures_lite::AsyncWriteExt as _;

pub async fn serve_v2<R, W, D>(
    input: &mut R,
    output: &mut W,
    delegate: &mut D,
) -> Result<super::Outcome, super::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    D: super::Delegate,
{
    let outcome = {
        let mut blocking_input = futures_lite::io::BlockOn::new(input);
        let mut blocking_output = futures_lite::io::BlockOn::new(&mut *output);
        super::serve_v2(&mut blocking_input, &mut blocking_output, delegate)?
    };
    output.flush().await?;
    Ok(outcome)
}

pub async fn parse_v2_request<R>(input: &mut R) -> Result<super::Request, super::Error>
where
    R: AsyncRead + Unpin,
{
    let mut blocking_input = futures_lite::io::BlockOn::new(input);
    super::parse_v2_request(&mut blocking_input)
}

pub async fn write_ls_refs_response<W>(
    output: &mut W,
    request: &super::LsRefs,
    refs: &[gix_transport::handshake::Ref],
) -> Result<usize, super::Error>
where
    W: AsyncWrite + Unpin,
{
    let refs_sent = {
        let mut blocking_output = futures_lite::io::BlockOn::new(&mut *output);
        super::write_ls_refs_response(&mut blocking_output, request, refs)?
    };
    output.flush().await?;
    Ok(refs_sent)
}

pub async fn write_v2_capability_advertisement<W>(
    output: &mut W,
    capabilities: &[super::Capability],
) -> Result<(), super::Error>
where
    W: AsyncWrite + Unpin,
{
    {
        let mut blocking_output = futures_lite::io::BlockOn::new(&mut *output);
        super::write_v2_capability_advertisement(&mut blocking_output, capabilities)?;
    }
    output.flush().await?;
    Ok(())
}
```

### Component 2: AsyncFetchOutput

A parallel to `FetchOutput` where the pack data source is async:

```rust
use futures_io::AsyncRead;
use crate::fetch::response::{Acknowledgement, ShallowUpdate, WantedRef};

/// Output payload for an async `fetch` response.
pub struct AsyncFetchOutput {
    /// Negotiation acknowledgements to return in the `acknowledgments` section.
    pub acknowledgements: Vec<Acknowledgement>,
    /// Optional shallow boundary updates to return in the `shallow-info` section.
    pub shallow_updates: Vec<ShallowUpdate>,
    /// Optional `wanted-refs` section entries.
    pub wanted_refs: Vec<WantedRef>,
    /// If present, pack data streamed as sideband channel 1 in the `packfile` section.
    pub pack_data: Option<Box<dyn AsyncRead + Unpin + Send>>,
}

impl AsyncFetchOutput {
    /// Create a response output with async `pack_data` and no additional sections.
    pub fn new(pack_data: impl AsyncRead + Unpin + Send + 'static) -> Self {
        Self {
            acknowledgements: Vec::new(),
            shallow_updates: Vec::new(),
            wanted_refs: Vec::new(),
            pack_data: Some(Box::new(pack_data)),
        }
    }

    /// Create a response output without pack data.
    pub fn without_pack() -> Self {
        Self {
            acknowledgements: Vec::new(),
            shallow_updates: Vec::new(),
            wanted_refs: Vec::new(),
            pack_data: None,
        }
    }
}
```

### Component 3: Native Async write_fetch_response

This function cannot use BlockOn because it needs to read from an `AsyncRead` pack source. It implements the sideband framing natively using async I/O:

```rust
use futures_io::AsyncWrite;
use futures_lite::{AsyncReadExt as _, AsyncWriteExt as _};
use gix_transport::packetline::{Channel, blocking_io::encode};

const MAX_SIDEBAND_DATA_BYTES: usize = 65_515;

/// Write a V2 `fetch` response with async pack streaming.
///
/// Returns the number of raw pack bytes sent on sideband channel 1.
pub async fn write_fetch_response<W>(
    output: &mut W,
    response: &mut AsyncFetchOutput,
) -> Result<u64, super::Error>
where
    W: AsyncWrite + Unpin,
{
    // Write metadata sections (acks, shallow-info, wanted-refs) using BlockOn
    // since these are small buffered writes.
    {
        let mut blocking_output = futures_lite::io::BlockOn::new(&mut *output);
        write_fetch_metadata_sections(&mut blocking_output, response)?;
    }

    // Stream pack data natively async
    let mut pack_bytes_sent = 0u64;
    if let Some(pack_data) = response.pack_data.as_mut() {
        // Write "packfile" section header via BlockOn (single small write)
        {
            let mut blocking_output = futures_lite::io::BlockOn::new(&mut *output);
            let mut writer = gix_transport::packetline::blocking_io::Writer::new(&mut blocking_output);
            writer.enable_text_mode();
            std::io::Write::write_all(&mut writer, b"packfile")?;
        }

        // Stream pack data as sideband channel 1 packets using async I/O
        let mut buffer = [0u8; MAX_SIDEBAND_DATA_BYTES];
        loop {
            let bytes_read = pack_data.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            pack_bytes_sent += bytes_read as u64;
            // Encode sideband packet into a temporary buffer, then async-write it
            let mut packet_buf = Vec::new();
            encode::band_to_write(Channel::Data, &buffer[..bytes_read], &mut packet_buf)?;
            output.write_all(&packet_buf).await?;
        }
    }

    // Write flush packet and flush the stream
    {
        let mut flush_buf = Vec::new();
        encode::flush_to_write(&mut flush_buf)?;
        output.write_all(&flush_buf).await?;
    }
    output.flush().await?;
    Ok(pack_bytes_sent)
}

/// Write the non-pack sections of a fetch response (acks, shallow-info, wanted-refs).
/// This is a blocking helper used internally with BlockOn-wrapped streams.
fn write_fetch_metadata_sections(
    mut output: impl std::io::Write,
    response: &AsyncFetchOutput,
) -> Result<(), super::Error> {
    let mut writer = gix_transport::packetline::blocking_io::Writer::new(&mut output);
    writer.enable_text_mode();

    if !response.acknowledgements.is_empty() {
        std::io::Write::write_all(&mut writer, b"acknowledgments")?;
        for ack in &response.acknowledgements {
            std::io::Write::write_all(
                &mut writer,
                super::format_acknowledgement_line(*ack).as_ref(),
            )?;
        }
        encode::delim_to_write(writer.inner_mut())?;
    }

    if !response.shallow_updates.is_empty() {
        std::io::Write::write_all(&mut writer, b"shallow-info")?;
        for update in &response.shallow_updates {
            std::io::Write::write_all(
                &mut writer,
                super::format_shallow_update_line(update).as_ref(),
            )?;
        }
        encode::delim_to_write(writer.inner_mut())?;
    }

    if !response.wanted_refs.is_empty() {
        std::io::Write::write_all(&mut writer, b"wanted-refs")?;
        for wanted in &response.wanted_refs {
            std::io::Write::write_all(
                &mut writer,
                super::format_wanted_ref_line(wanted).as_ref(),
            )?;
        }
        encode::delim_to_write(writer.inner_mut())?;
    }

    Ok(())
}
```

### Component 4: Re-exports

The `async_io` module re-exports shared types from the parent module for ergonomic imports:

```rust
// Re-export shared types so consumers don't need to import from two paths.
pub use super::{
    Capability, Command, Delegate, Error, Fetch, FetchNegotiation,
    Feature, LsRefs, Outcome, Request,
    negotiate_fetch_with_repository,
};
```

Note that `negotiate_fetch_with_repository` is re-exported as-is (synchronous). No async wrapper is provided since it is pure computation over in-memory data.

## Data Models

### AsyncFetchOutput

The primary new data model is `AsyncFetchOutput`, which mirrors `FetchOutput` but replaces the synchronous pack reader with an async one:

| Field | Type | Description |
|-------|------|-------------|
| `acknowledgements` | `Vec<Acknowledgement>` | Negotiation ACKs for the `acknowledgments` section |
| `shallow_updates` | `Vec<ShallowUpdate>` | Shallow boundary updates for `shallow-info` section |
| `wanted_refs` | `Vec<WantedRef>` | Resolved ref entries for `wanted-refs` section |
| `pack_data` | `Option<Box<dyn AsyncRead + Unpin + Send>>` | Async pack byte source for sideband streaming |

All other types (`Request`, `Command`, `LsRefs`, `Fetch`, `Feature`, `Capability`, `FetchNegotiation`, `Outcome`, `Error`, `Delegate`) are shared with the blocking module and re-exported from `async_io`.

## Data Flow

### serve_v2 (BlockOn path)

```
AsyncRead input
    │
    ▼
BlockOn::new(input) → io::Read
    │
    ▼
blocking::parse_v2_request() → Request
    │
    ▼
delegate.ls_refs() or delegate.fetch()
    │
    ▼
blocking::write_ls_refs_response() or blocking::write_fetch_response()
    │                                       │
    ▼                                       ▼
BlockOn::new(output) → io::Write     BlockOn::new(output)
    │                                       │
    ▼                                       ▼
output.flush().await                 output.flush().await
    │                                       │
    ▼                                       ▼
Ok(Outcome::LsRefs{..})             Ok(Outcome::Fetch{..})
```

### write_fetch_response (native async path)

```
AsyncFetchOutput
    │
    ├─ acknowledgements ─┐
    ├─ shallow_updates  ─┼─ BlockOn → blocking packetline writer → output
    ├─ wanted_refs      ─┘
    │
    └─ pack_data: AsyncRead
           │
           ▼
       async read loop (MAX_SIDEBAND_DATA_BYTES chunks)
           │
           ▼
       encode::band_to_write(Channel::Data, chunk) → packet bytes
           │
           ▼
       output.write_all(packet_bytes).await
           │
           ▼
       encode::flush_to_write() → flush packet
           │
           ▼
       output.flush().await
           │
           ▼
       Ok(pack_bytes_sent)
```

## Error Handling

All async functions use the same `upload_pack::Error` type as the blocking implementations. The `Error` enum already includes `Io(io::Error)` which covers async flush failures (`io::Error` from the `AsyncWrite::flush()` call propagates through `?`).

No new error variants are needed. The `BlockOn` adapter converts `Poll::Pending` into blocking waits, so within a single-threaded async context (or when used with `spawn_blocking`), I/O errors propagate identically to the blocking path.

## Interfaces

### Public API Surface

```rust
// gix_protocol::upload_pack::async_io

// --- Functions ---
pub async fn serve_v2<R, W, D>(input: &mut R, output: &mut W, delegate: &mut D)
    -> Result<Outcome, Error>
where R: AsyncRead + Unpin, W: AsyncWrite + Unpin, D: Delegate;

pub async fn parse_v2_request<R>(input: &mut R)
    -> Result<Request, Error>
where R: AsyncRead + Unpin;

pub async fn write_fetch_response<W>(output: &mut W, response: &mut AsyncFetchOutput)
    -> Result<u64, Error>
where W: AsyncWrite + Unpin;

pub async fn write_ls_refs_response<W>(output: &mut W, request: &LsRefs, refs: &[Ref])
    -> Result<usize, Error>
where W: AsyncWrite + Unpin;

pub async fn write_v2_capability_advertisement<W>(output: &mut W, capabilities: &[Capability])
    -> Result<(), Error>
where W: AsyncWrite + Unpin;

// --- Types ---
pub struct AsyncFetchOutput { .. }

// --- Re-exports from parent ---
pub use super::{
    Capability, Command, Delegate, Error, Fetch, FetchNegotiation,
    Feature, LsRefs, Outcome, Request,
    negotiate_fetch_with_repository,
};
```

### Internal Dependencies

The `async_io` module depends on these internal (non-public) helpers from the parent `upload_pack` module:
- `format_acknowledgement_line()`
- `format_shallow_update_line()`
- `format_wanted_ref_line()`

These remain synchronous and are called through `super::` within the `write_fetch_metadata_sections` helper.

## Concurrency Considerations

The `BlockOn` adapter from `futures-lite` blocks the current thread when the underlying async stream returns `Poll::Pending`. This is acceptable for server-side upload-pack because:

1. The caller is expected to run `serve_v2` inside a `spawn_blocking` task or on a dedicated thread pool when using a multi-threaded runtime like tokio.
2. For single-threaded async runtimes (e.g., `async-std`), the blocking is on the same thread that drives the I/O, so forward progress is guaranteed as long as data is available.
3. The native async `write_fetch_response` avoids BlockOn for the hot path (pack streaming), ensuring the executor isn't blocked during large pack transfers.

## Testing Strategy

Tests reside in `gix-protocol/tests/` under a test binary gated on `async-server`:

```toml
[[test]]
name = "async-server"
path = "tests/async-server.rs"
required-features = ["async-server"]
```

Tests use `futures_lite::io::Cursor` for in-memory async streams and `#[async_std::test]` for the async runtime, matching the pattern in `receive_pack::async_io`.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: BlockOn-bridged functions produce identical results to blocking counterparts

*For any* valid protocol V2 request payload (ls-refs or fetch), and for any Delegate implementation, calling the async `serve_v2` with BlockOn-adapted streams SHALL produce the same `Outcome` value and identical output bytes as calling the blocking `upload_pack::serve_v2` with the same input and delegate. This equivalence extends to `parse_v2_request`, `write_ls_refs_response`, and `write_v2_capability_advertisement` individually.

**Validates: Requirements 2.2, 5.2, 6.2, 7.2, 10.2**

### Property 2: Async write_fetch_response correctly frames pack data as sideband channel 1

*For any* byte sequence provided as `AsyncFetchOutput.pack_data`, calling `write_fetch_response` SHALL write every byte of the input as sideband channel 1 (`\x01`-prefixed) data packets, where each packet contains at most `MAX_SIDEBAND_DATA_BYTES` of payload, and the concatenation of all packet payloads equals the original byte sequence.

**Validates: Requirements 3.2, 3.3**

### Property 3: write_fetch_response byte count equals pack bytes consumed

*For any* `AsyncFetchOutput` with pack data of known length N, the `u64` value returned by `write_fetch_response` SHALL equal N (the total number of raw pack bytes read from the async source).

**Validates: Requirements 3.4**

### Property 4: All async bridge functions flush output before returning

*For any* async bridge function call (`serve_v2`, `write_fetch_response`, `write_ls_refs_response`, `write_v2_capability_advertisement`) that completes successfully, the `AsyncWrite` stream SHALL have received a `flush()` call before the function returns `Ok`.

**Validates: Requirements 2.3, 3.5, 5.3, 6.3**
