//! Type-safe protocol phase encoding for upload-pack state machines.
//!
//! Each phase is a distinct type; transition methods consume `self` by value,
//! preventing reuse of consumed states and enforcing valid transitions at
//! compile time via Rust's ownership system.

use std::io;

use super::{Fetch, FetchOutput, Delegate, Error};

/// A left-or-right value used for branching state transitions.
///
/// When a transition can lead to one of two states (e.g., `SendPack` when
/// ready or `Done` when not), this enum encodes the branch.
pub(crate) enum Either<L, R> {
    /// The "left" (primary/continuing) branch.
    Left(L),
    /// The "right" (terminal/alternative) branch.
    Right(R),
}

/// Protocol phases for upload-pack V2.
///
/// Each phase carries only the data valid for that phase. Transition methods
/// take `self` by value, preventing reuse of consumed states.
pub(crate) mod v2 {
    use super::*;

    /// Initial state: request has been parsed, ready for negotiation.
    pub struct Parsed {
        /// The parsed fetch request.
        pub request: Fetch,
    }

    /// Negotiation complete: acknowledgements and readiness determined.
    pub struct Negotiated {
        /// The original fetch request.
        pub request: Fetch,
        /// The negotiation result produced by the delegate.
        pub output: FetchOutput,
    }

    /// Pack data is being sent (only reachable when ready=true).
    pub struct SendPack {
        /// The original fetch request (preserved for Phase 2 delegate split).
        #[allow(dead_code)]
        pub request: Fetch,
        /// The fetch output containing pack data to stream.
        pub output: FetchOutput,
    }

    /// Terminal state: response fully written.
    pub struct Done;

    impl Parsed {
        /// Perform negotiation by calling the delegate, consuming the Parsed state.
        ///
        /// The delegate's `fetch()` method is called with the parsed request, producing
        /// a `FetchOutput` that contains acknowledgements and optional pack data.
        pub fn negotiate(self, delegate: &mut impl Delegate) -> Result<Negotiated, Error> {
            let output = delegate.fetch(&self.request).map_err(Error::Delegate)?;
            Ok(Negotiated {
                request: self.request,
                output,
            })
        }
    }

    impl Negotiated {
        /// Resolve whether pack data should be sent.
        ///
        /// If the output contains pack data, transition to `SendPack`.
        /// Otherwise, transition to `Done` (no pack to send this round).
        pub fn resolve(self) -> Either<SendPack, Done> {
            if self.output.pack_data.is_some() {
                Either::Left(SendPack {
                    request: self.request,
                    output: self.output,
                })
            } else {
                Either::Right(Done)
            }
        }
    }

    impl SendPack {
        /// Write pack data to the output stream, consuming this state.
        ///
        /// Returns `Done`, the consumed `FetchOutput`, and the number of raw
        /// pack bytes written on sideband channel 1.
        pub fn send(
            mut self,
            output: &mut impl io::Write,
        ) -> Result<(Done, FetchOutput, u64), Error> {
            use super::super::response::PackfileSection;

            let pack_bytes = match self.output.pack_data.as_mut() {
                Some(pack_data) => PackfileSection.write(output, &mut **pack_data)?,
                None => 0,
            };
            Ok((Done, self.output, pack_bytes))
        }
    }
}

/// Protocol phases for upload-pack V1 (future).
///
/// These are placeholder types for the V1 protocol flow. Transition methods
/// will be added in Phase 2 when V1 negotiation handling is implemented.
#[allow(dead_code)] // Placeholder for V1 protocol support (Phase 2).
pub(crate) mod v1 {
    /// Server is sending ref advertisement with capabilities.
    pub struct Advertise;

    /// Multi-round want/have negotiation in progress.
    pub struct Negotiate {
        /// Object IDs the client wants.
        pub wants: Vec<gix_hash::ObjectId>,
        /// Common objects found during negotiation rounds.
        pub common: Vec<gix_hash::ObjectId>,
    }

    /// Pack transfer (shared with V2 via SendPack logic).
    pub struct SendPack;

    /// Terminal state.
    pub struct Done;
}
