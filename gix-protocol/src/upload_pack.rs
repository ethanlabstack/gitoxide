//! Blocking server-side plumbing for `upload-pack` protocol V2 interactions.
//!
//! This module provides in-process request/response handling primitives intended for
//! server integrations that own connection handling and authentication.
//! It focuses on protocol framing and command parsing/writing while delegating repository
//! access and pack generation to caller-provided implementations.

/// Async transport integration for upload-pack server plumbing.
#[cfg(feature = "async-server")]
pub mod async_io;

/// Protocol V2 upload-pack request parsing.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) mod parse;
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub use parse::parse_v2_request;

/// Composable section writers for protocol V2 upload-pack responses.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) mod response;

/// Fetch negotiation state and logic.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) mod negotiate;

/// Type-safe protocol phase encoding for upload-pack state machines.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) mod state;

use std::{
    collections::BTreeSet,
    io,
    sync::atomic::AtomicBool,
};

use bstr::BString;

use crate::{
    fetch::response::{Acknowledgement, ShallowUpdate, WantedRef},
    handshake::Ref,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[allow(dead_code)] // Used by response/packfile.rs and async_io.rs.
pub(crate) const MAX_SIDEBAND_DATA_BYTES: usize = 65_515;

/// A parsed feature line from a protocol V2 request header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    /// The feature name, e.g. `agent`.
    pub name: BString,
    /// An optional feature value, e.g. `git/2.48.0`.
    pub value: Option<BString>,
}

/// A capability line to advertise in protocol V2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// The capability name, like `ls-refs` or `fetch`.
    pub name: BString,
    /// Optional values associated with `name`, separated by spaces when rendered.
    pub values: Vec<BString>,
}

/// Server-side configuration for upload-pack capability validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// The object hash algorithm this server supports.
    pub object_hash: gix_hash::Kind,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            object_hash: gix_hash::Kind::Sha1,
        }
    }
}

/// A parsed protocol V2 request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Request header features that accompany the command.
    pub features: Vec<Feature>,
    /// The upload-pack command payload.
    pub command: Command,
}

/// Parsed upload-pack command variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// A `ls-refs` command.
    LsRefs(LsRefs),
    /// A `fetch` command.
    Fetch(Fetch),
}

/// Parsed `ls-refs` command arguments.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LsRefs {
    /// If true, include symbolic reference targets in output.
    pub symrefs: bool,
    /// If true, include peeled object IDs where available.
    pub peel: bool,
    /// If true, include unborn refs in output.
    pub unborn: bool,
    /// Prefix filters to apply to advertised refs.
    pub ref_prefixes: Vec<BString>,
    /// Unknown arguments preserved for higher-level handling.
    pub extra_arguments: Vec<BString>,
}

/// Parsed `fetch` command arguments.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Fetch {
    /// Requested object IDs.
    pub wants: Vec<gix_hash::ObjectId>,
    /// Object IDs already present on the client.
    pub haves: Vec<gix_hash::ObjectId>,
    /// Requested refs through `want-ref`.
    pub want_refs: Vec<BString>,
    /// Shallow boundary commits sent by the client.
    pub shallow: Vec<gix_hash::ObjectId>,
    /// Optional depth requested by the client through `deepen <depth>`.
    pub deepen: Option<u32>,
    /// Optional depth timestamp requested by the client through `deepen-since <timestamp>`.
    pub deepen_since: Option<gix_date::SecondsSinceUnixEpoch>,
    /// Ref exclusions requested by the client through `deepen-not <ref>`.
    pub deepen_not: Vec<BString>,
    /// If true, client requests `deepen-relative`.
    pub deepen_relative: bool,
    /// Filter specifications requested by the client through `filter <spec>`.
    pub filters: Vec<BString>,
    /// Protocols requested by the client through `packfile-uris <protocols>`.
    pub packfile_uris: Vec<BString>,
    /// If true, client requests thin-pack behavior.
    pub thin_pack: bool,
    /// If true, client requests `no-progress`.
    pub no_progress: bool,
    /// If true, client requests `ofs-delta`.
    pub ofs_delta: bool,
    /// If true, client requests `include-tag`.
    pub include_tag: bool,
    /// If true, client requests `sideband-all`.
    pub sideband_all: bool,
    /// If true, client requests `wait-for-done`.
    pub wait_for_done: bool,
    /// If true, client completed negotiation with `done`.
    pub done: bool,
    /// Unknown arguments preserved for higher-level handling.
    pub extra_arguments: Vec<BString>,
}
/// The result of negotiating an upload-pack `fetch` request against repository data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchNegotiation {
    /// Acknowledgements to send in the `acknowledgments` section.
    pub acknowledgements: Vec<Acknowledgement>,
    /// Requested refs that could be resolved to object IDs and returned in `wanted-refs`.
    pub wanted_refs: Vec<WantedRef>,
    /// `want` object IDs that are present in the repository.
    pub known_wants: Vec<gix_hash::ObjectId>,
    /// `want` object IDs that are absent in the repository.
    pub missing_wants: Vec<gix_hash::ObjectId>,
    /// `have` object IDs that are present in the repository.
    pub common_haves: Vec<gix_hash::ObjectId>,
    /// `want-ref` names that could not be resolved to a reference.
    pub unresolved_want_refs: Vec<BString>,
}

impl FetchNegotiation {
    /// Convert this negotiation result into a [`FetchOutput`] without pack data.
    pub fn into_output(self) -> FetchOutput {
        let mut output = FetchOutput::without_pack();
        output.acknowledgements = self.acknowledgements;
        output.wanted_refs = self.wanted_refs;
        output
    }

    /// Convert this negotiation result into a [`FetchOutput`] and populate repository-backed pack data.
    ///
    /// Pack generation traverses all commits reachable from negotiated wants and from peeled `want-ref`
    /// targets while excluding commits reachable from acknowledged `have` lines.
    ///
    /// When `request.done` is false (ongoing negotiation), pack generation is skipped entirely
    /// and the output contains only metadata (acknowledgements, wanted-refs). Per protocol V2,
    /// pack data is only sent when the client has signaled negotiation is complete.
    pub fn into_output_with_repository_pack<Find>(
        self,
        request: &Fetch,
        object_database: Find,
        object_hash: gix_hash::Kind,
    ) -> Result<FetchOutput, FetchPackGenerationError>
    where
        Find: gix_object::Find + gix_pack::Find + Clone,
    {
        if !request.done {
            return Ok(self.into_output());
        }
        let pack_data = generate_fetch_pack_data_with_repository(request, &self, object_database, object_hash)?;
        let mut output = self.into_output();
        output.pack_data = pack_data.map(|pack| Box::new(io::Cursor::new(pack)) as Box<dyn io::Read + Send + 'static>);
        Ok(output)
    }
}

/// Errors returned while negotiating `fetch` requests against repository state.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum FetchNegotiationError {
    #[error(transparent)]
    OpenPackedRefs(#[from] gix_ref::packed::buffer::open::Error),
    #[error("Could not lookup wanted ref {ref_name:?}")]
    FindWantedRef {
        ref_name: BString,
        #[source]
        source: gix_ref::file::find::existing::Error,
    },
    #[error("Could not resolve wanted ref {ref_name:?} to an object id")]
    ResolveWantedRef {
        ref_name: BString,
        #[source]
        source: gix_ref::peel::to_object::Error,
    },
}

/// Errors returned while building repository-backed pack data for negotiated fetches.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum FetchPackGenerationError {
    #[error(transparent)]
    FindObject(#[from] gix_object::find::Error),
    #[error("Object {id} disappeared while preparing pack data")]
    MissingObject { id: gix_hash::ObjectId },
    #[error(transparent)]
    DecodeTag(#[from] gix_object::decode::Error),
    #[error("Tag cycle detected at {id}")]
    TagCycle { id: gix_hash::ObjectId },
    #[error(transparent)]
    TraverseCommits(#[from] gix_traverse::commit::simple::Error),
    #[error(transparent)]
    CountObjects(#[from] gix_pack::data::output::count::objects::Error),
    #[error(transparent)]
    BuildPackEntry(#[from] gix_pack::data::output::entry::Error),
    #[error(transparent)]
    EncodePack(#[from] gix_pack::data::output::bytes::Error<gix_pack::data::output::entry::Error>),
    #[error("Cannot encode more than u32::MAX objects in a single pack, got {object_count}")]
    TooManyObjects { object_count: usize },
}

fn generate_fetch_pack_data_with_repository<Find>(
    _request: &Fetch,
    negotiation: &FetchNegotiation,
    object_database: Find,
    object_hash: gix_hash::Kind,
) -> Result<Option<Vec<u8>>, FetchPackGenerationError>
where
    Find: gix_object::Find + gix_pack::Find + Clone,
{
    let mut requested_ids = Vec::new();
    let mut seen_requested_ids = BTreeSet::new();
    for object_id in negotiation
        .known_wants
        .iter()
        .chain(negotiation.wanted_refs.iter().map(|wanted| &wanted.id))
    {
        if seen_requested_ids.insert(*object_id) {
            requested_ids.push(*object_id);
        }
    }
    if requested_ids.is_empty() {
        return Ok(None);
    }

    let mut object_buf = Vec::new();
    let wanted_commit_tips = collect_commit_tips(&requested_ids, &object_database, &mut object_buf)?;
    let hidden_commit_tips = collect_commit_tips(&negotiation.common_haves, &object_database, &mut object_buf)?;

    let mut objects_to_pack = Vec::new();
    if !wanted_commit_tips.is_empty() {
        let mut walk = gix_traverse::commit::Simple::new(wanted_commit_tips, object_database.clone());
        if !hidden_commit_tips.is_empty() {
            walk = walk.hide(hidden_commit_tips)?;
        }
        for commit in walk {
            objects_to_pack.push(commit?.id);
        }
    }
    objects_to_pack.extend(requested_ids);
    if objects_to_pack.is_empty() {
        return Ok(None);
    }

    let mut object_ids = objects_to_pack.into_iter().map(Ok::<_, BoxError>);
    let should_interrupt = AtomicBool::new(false);
    let (counts, _) = gix_pack::data::output::count::objects_unthreaded(
        &object_database,
        &mut object_ids,
        &gix_features::progress::Discard,
        &should_interrupt,
        gix_pack::data::output::count::objects::ObjectExpansion::TreeContents,
    )?;
    if counts.is_empty() {
        return Ok(None);
    }

    let object_count = counts.len();
    let num_entries =
        u32::try_from(object_count).map_err(|_| FetchPackGenerationError::TooManyObjects { object_count })?;
    let mut object_buf = Vec::new();
    let mut entries = Vec::with_capacity(object_count);
    for count in &counts {
        let object = gix_pack::Find::try_find(&object_database, count.id.as_ref(), &mut object_buf)?
            .ok_or_else(|| FetchPackGenerationError::MissingObject { id: count.id })?
            .0;
        entries.push(gix_pack::data::output::Entry::from_data(
            count,
            &object,
            gix_zlib::Compression::default(),
        )?);
    }
    let mut writer = gix_pack::data::output::bytes::FromEntriesIter::new(
        std::iter::once(Ok::<_, gix_pack::data::output::entry::Error>(entries)),
        Vec::new(),
        num_entries,
        gix_pack::data::Version::V2,
        object_hash,
    );
    for written in &mut writer {
        written?;
    }
    Ok(Some(writer.into_write()))
}

fn collect_commit_tips<Find>(
    object_ids: &[gix_hash::ObjectId],
    object_database: &Find,
    object_buf: &mut Vec<u8>,
) -> Result<Vec<gix_hash::ObjectId>, FetchPackGenerationError>
where
    Find: gix_object::Find,
{
    let mut tips = Vec::new();
    let mut seen_tips = BTreeSet::new();
    for object_id in object_ids {
        if let Some(commit_id) = peel_to_commit_tip(object_id, object_database, object_buf)? {
            if seen_tips.insert(commit_id) {
                tips.push(commit_id);
            }
        }
    }
    Ok(tips)
}

fn peel_to_commit_tip<Find>(
    object_id: &gix_hash::ObjectId,
    object_database: &Find,
    object_buf: &mut Vec<u8>,
) -> Result<Option<gix_hash::ObjectId>, FetchPackGenerationError>
where
    Find: gix_object::Find,
{
    let mut id = *object_id;
    let mut seen_tags = BTreeSet::new();
    loop {
        let object = object_database
            .try_find(id.as_ref(), object_buf)?
            .ok_or_else(|| FetchPackGenerationError::MissingObject { id })?;
        match object.kind {
            gix_object::Kind::Commit => return Ok(Some(id)),
            gix_object::Kind::Tag => {
                if !seen_tags.insert(id) {
                    return Err(FetchPackGenerationError::TagCycle { id });
                }
                id = gix_object::TagRefIter::from_bytes(object.data, object.object_hash).target_id()?;
            }
            gix_object::Kind::Tree | gix_object::Kind::Blob => return Ok(None),
        }
    }
}

/// Negotiate a `fetch` request using repository refs and object existence checks.
///
/// This resolves:
/// - `have` lines into `ACK` responses for object IDs known by the repository
/// - `want` lines into known/missing object sets
/// - `want-ref` lines into `wanted-refs` response entries when refs can be resolved
///
/// When `request.done` is true (client signals negotiation is complete), the acknowledgements
/// list ends with [`Acknowledgement::Ready`] if common haves exist, or is left empty for
/// fresh clones so that `write_fetch_response` omits the `acknowledgments` section entirely.
///
/// Pack construction is intentionally out of scope of this helper.
pub fn negotiate_fetch_with_repository(
    request: &Fetch,
    refs: &gix_ref::file::Store,
    object_exists: impl FnMut(&gix_hash::oid) -> bool,
) -> Result<FetchNegotiation, FetchNegotiationError> {
    negotiate::NegotiationState::new(request).evaluate(request, refs, object_exists)
}

/// Output payload for a `fetch` response.
pub struct FetchOutput {
    /// Negotiation acknowledgements to return in the `acknowledgments` section.
    pub acknowledgements: Vec<Acknowledgement>,
    /// Optional shallow boundary updates to return in the `shallow-info` section.
    pub shallow_updates: Vec<ShallowUpdate>,
    /// Optional `wanted-refs` section entries.
    pub wanted_refs: Vec<WantedRef>,
    /// If present, pack data streamed as sideband channel 1 in the `packfile` section.
    pub pack_data: Option<Box<dyn io::Read + Send + 'static>>,
}

impl FetchOutput {
    /// Create a response output with `pack_data` and no additional sections.
    pub fn new(pack_data: impl io::Read + Send + 'static) -> Self {
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

/// The outcome of serving a single upload-pack protocol V2 command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// `ls-refs` output was produced.
    LsRefs {
        /// Number of refs sent to the client after applying filters.
        refs_sent: usize,
    },
    /// `fetch` output was produced.
    Fetch {
        /// Number of acknowledgement lines sent.
        acknowledgements_sent: usize,
        /// Number of shallow updates sent.
        shallow_updates_sent: usize,
        /// Number of wanted refs sent.
        wanted_refs_sent: usize,
        /// Number of raw pack bytes sent on sideband channel 1.
        pack_bytes_sent: u64,
    },
}

/// Delegate implementation used by [`serve_v2()`] to obtain repository data.
pub trait Delegate {
    /// Return refs to advertise for the incoming `ls-refs` request.
    fn ls_refs(&mut self, request: &LsRefs) -> Result<Vec<Ref>, BoxError>;
    /// Produce a fetch response for the incoming `fetch` request.
    ///
    /// [`negotiate_fetch_with_repository()`] can be used to obtain repository-backed
    /// acknowledgement and `wanted-refs` data before pack generation is applied.
    fn fetch(&mut self, request: &Fetch) -> Result<FetchOutput, BoxError>;
}

/// Errors returned by upload-pack request parsing and response writing.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Decode(#[from] gix_transport::packetline::decode::Error),
    #[error("Expected text packetline, got {line_type}")]
    NonTextPacketLine { line_type: &'static str },
    #[error("Expected `command=<name>` in request header")]
    MissingCommand,
    #[error("Unsupported upload-pack V2 command {command:?}")]
    UnsupportedCommand { command: BString },
    #[error("Malformed request header line {line:?}")]
    MalformedHeaderLine { line: BString },
    #[error("Malformed {command} argument line {line:?}")]
    MalformedArgument { command: &'static str, line: BString },
    #[error("Could not parse object id in line {line:?}")]
    InvalidObjectId {
        line: BString,
        #[source]
        source: gix_hash::decode::Error,
    },
    #[error("Delegate failed")]
    Delegate(#[source] BoxError),
    #[error("Client requested object-format \"{requested}\" but server supports \"{supported}\"")]
    UnsupportedObjectFormat { requested: BString, supported: BString },
    #[error("Invalid object-format value \"{value}\" (expected \"sha1\" or \"sha256\")")]
    InvalidObjectFormat { value: BString },
    #[error("Object ID hex length {actual} does not match expected {expected} for {hash_kind}")]
    ObjectIdLengthMismatch {
        actual: usize,
        expected: usize,
        hash_kind: gix_hash::Kind,
    },
}

/// Serve one protocol V2 upload-pack request end-to-end.
///
/// The caller owns transport setup/teardown and invokes this function with one complete request payload.
/// The `config` parameter controls capability validation — the client's `object-format` feature
/// (if present) is checked against `config.object_hash`, and OID hex lengths in fetch arguments
/// are enforced to match the configured hash kind.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub fn serve_v2(
    input: impl io::Read,
    mut output: impl io::Write,
    delegate: &mut impl Delegate,
    config: &ServerConfig,
) -> Result<Outcome, Error> {
    match parse_v2_request(input, config)? {
        Request {
            command: Command::LsRefs(request),
            ..
        } => {
            let refs = delegate.ls_refs(&request).map_err(Error::Delegate)?;
            let refs_sent = write_ls_refs_response(&mut output, &request, &refs)?;
            Ok(Outcome::LsRefs { refs_sent })
        }
        Request {
            command: Command::Fetch(request),
            ..
        } => {
            use gix_transport::packetline::blocking_io::encode;
            use state::{v2, Either};

            let parsed = v2::Parsed { request };
            let mut negotiated = parsed.negotiate(delegate)?;

            // Protocol V2 invariant: pack data may only be sent when the ack
            // section either contains `ready` OR is omitted entirely (fresh clone).
            // If the delegate returns pack data with a non-empty ack section that
            // lacks ready (ongoing negotiation), strip it to prevent the client from
            // seeing sections after a non-ready ack response.
            let has_ready = negotiated.output.acknowledgements.iter().any(|a| {
                matches!(a, crate::fetch::response::Acknowledgement::Ready)
            });
            let acks_present_without_ready = !negotiated.output.acknowledgements.is_empty() && !has_ready;
            if acks_present_without_ready {
                negotiated.output.pack_data = None;
            }

            // Capture section counts before consuming the state via resolve().
            let acknowledgements_sent = negotiated.output.acknowledgements.len();
            let shallow_updates_sent = negotiated.output.shallow_updates.len();
            let wanted_refs_sent = negotiated.output.wanted_refs.len();

            // Write metadata sections (acks, shallow-info, wanted-refs).
            write_fetch_metadata_sections(
                &mut output,
                &negotiated.output.acknowledgements,
                &negotiated.output.shallow_updates,
                &negotiated.output.wanted_refs,
                negotiated.output.pack_data.is_some(),
            )?;

            // Resolve and optionally send pack data.
            let pack_bytes_sent = match negotiated.resolve() {
                Either::Left(send_pack) => {
                    let (_, _, bytes) = send_pack.send(&mut output)?;
                    bytes
                }
                Either::Right(_done) => 0,
            };

            encode::flush_to_write(&mut output)?;
            Ok(Outcome::Fetch {
                acknowledgements_sent,
                shallow_updates_sent,
                wanted_refs_sent,
                pack_bytes_sent,
            })
        }
    }
}

// Public write functions are implemented in the response module and re-exported here.
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub use response::{write_v2_capability_advertisement, write_ls_refs_response, write_fetch_response, write_v1_ref_advertisement};
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) use response::write_fetch_metadata_sections;


#[cfg(test)]
mod tests {
    //! Internal unit tests that exercise `pub(crate)` items not reachable from external test crates.
    //! Public-API tests live in `gix-protocol/tests/protocol/upload_pack.rs`.
    use std::collections::BTreeSet;
    use std::io::Cursor;

    use bstr::ByteSlice;

    use super::*;
    use super::parse::{validate_object_format, parse_object_id};

    // Property-based tests for object-format validation
    // Feature: upload-pack-capability-validation, Property 1
    // **Validates: Requirements 2.1, 2.3, 3.1**
    #[cfg(feature = "blocking-server")]
    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_object_format_validation_sha1(
                value in ".*",
            ) {
                let kind = gix_hash::Kind::Sha1;
                let config = ServerConfig { object_hash: kind };
                let features = vec![Feature {
                    name: "object-format".into(),
                    value: Some(value.clone().into()),
                }];
                let result = validate_object_format(&features, &config);

                let known_formats = ["sha1", "sha256"];
                if !known_formats.contains(&value.as_str()) {
                    prop_assert!(
                        matches!(result, Err(Error::InvalidObjectFormat { .. })),
                        "unrecognized value {:?} should produce InvalidObjectFormat, got: {:?}",
                        value, result
                    );
                } else if value == kind.to_string() {
                    prop_assert!(
                        result.is_ok(),
                        "matching value {:?} for kind {:?} should succeed, got: {:?}",
                        value, kind, result
                    );
                } else {
                    prop_assert!(
                        matches!(result, Err(Error::UnsupportedObjectFormat { .. })),
                        "mismatched value {:?} for kind {:?} should produce UnsupportedObjectFormat, got: {:?}",
                        value, kind, result
                    );
                }
            }
        }

        #[cfg(feature = "sha256")]
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_object_format_validation_sha256(
                value in ".*",
            ) {
                let kind = gix_hash::Kind::Sha256;
                let config = ServerConfig { object_hash: kind };
                let features = vec![Feature {
                    name: "object-format".into(),
                    value: Some(value.clone().into()),
                }];
                let result = validate_object_format(&features, &config);

                let known_formats = ["sha1", "sha256"];
                if !known_formats.contains(&value.as_str()) {
                    prop_assert!(
                        matches!(result, Err(Error::InvalidObjectFormat { .. })),
                        "unrecognized value {:?} should produce InvalidObjectFormat, got: {:?}",
                        value, result
                    );
                } else if value == kind.to_string() {
                    prop_assert!(
                        result.is_ok(),
                        "matching value {:?} for kind {:?} should succeed, got: {:?}",
                        value, kind, result
                    );
                } else {
                    prop_assert!(
                        matches!(result, Err(Error::UnsupportedObjectFormat { .. })),
                        "mismatched value {:?} for kind {:?} should produce UnsupportedObjectFormat, got: {:?}",
                        value, kind, result
                    );
                }
            }
        }
    }

    // Property 3: Non-object-format features pass through without rejection
    // **Validates: Requirements 5.1, 5.2, 5.3**
    #[cfg(feature = "blocking-server")]
    mod property_non_object_format_tests {
        use super::*;
        use proptest::prelude::*;

        fn arbitrary_server_config() -> gix_hash::Kind {
            gix_hash::Kind::Sha1
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_non_object_format_feature_pass_through(
                name in "[a-z][a-z0-9-]{0,30}".prop_filter("must not be object-format", |s| s != "object-format"),
                has_value in any::<bool>(),
                value in ".*",
            ) {
                let config = ServerConfig { object_hash: arbitrary_server_config() };
                let feature_value = if has_value { Some(BString::from(value.as_str())) } else { None };
                let features = vec![Feature { name: name.clone().into(), value: feature_value }];

                let result = validate_object_format(&features, &config);
                prop_assert!(result.is_ok(), "non-object-format feature '{}' should not cause validation error, got: {:?}", name, result);
            }
        }

        #[cfg(feature = "sha256")]
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_non_object_format_feature_pass_through_sha256(
                name in "[a-z][a-z0-9-]{0,30}".prop_filter("must not be object-format", |s| s != "object-format"),
                has_value in any::<bool>(),
                value in ".*",
            ) {
                let config = ServerConfig { object_hash: gix_hash::Kind::Sha256 };
                let feature_value = if has_value { Some(BString::from(value.as_str())) } else { None };
                let features = vec![Feature { name: name.clone().into(), value: feature_value }];

                let result = validate_object_format(&features, &config);
                prop_assert!(result.is_ok(), "non-object-format feature '{}' with sha256 config should not cause validation error, got: {:?}", name, result);
            }
        }
    }

    // Property 2: OID length enforcement
    // **Validates: Requirements 4.1, 4.2, 7.4**
    #[cfg(feature = "blocking-server")]
    mod property_tests_oid_length {
        use super::*;
        use proptest::prelude::*;

        fn arb_hash_kind() -> impl Strategy<Value = gix_hash::Kind> {
            #[cfg(feature = "sha256")]
            {
                prop_oneof![
                    Just(gix_hash::Kind::Sha1),
                    Just(gix_hash::Kind::Sha256),
                ].boxed()
            }
            #[cfg(not(feature = "sha256"))]
            {
                Just(gix_hash::Kind::Sha1).boxed()
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_oid_length_enforcement(
                kind in arb_hash_kind(),
                hex_chars in prop::collection::vec(prop::char::range('\0', '\x7f'), 0..128usize),
            ) {
                let hex_string: String = hex_chars.into_iter().collect();
                let line = format!("want {hex_string}");
                let result = parse_object_id(line.as_bytes().as_bstr(), b"want ", "fetch", kind);

                let expected_len = kind.len_in_hex();
                if hex_string.len() != expected_len {
                    prop_assert!(
                        matches!(result, Err(Error::ObjectIdLengthMismatch { .. })),
                        "expected ObjectIdLengthMismatch for len {} != expected {}, got: {:?}",
                        hex_string.len(), expected_len, result,
                    );
                } else if hex_string.bytes().all(|b| b.is_ascii_hexdigit()) {
                    prop_assert!(
                        result.is_ok(),
                        "expected Ok for valid hex of correct length {}, got: {:?}",
                        expected_len, result,
                    );
                } else {
                    prop_assert!(
                        matches!(result, Err(Error::InvalidObjectId { .. })),
                        "expected InvalidObjectId for invalid hex chars at correct length {}, got: {:?}",
                        expected_len, result,
                    );
                }
            }
        }
    }

    // Property 4: Packfile sideband encoding and size bounds
    // **Validates: Requirements 1.5, 1.6, 1.7, 1.8**
    #[cfg(feature = "blocking-server")]
    mod property_packfile_sideband {
        use super::*;
        use proptest::prelude::*;
        use gix_transport::packetline::{PacketLineRef, BandRef, decode};
        use crate::upload_pack::response::PackfileSection;

        fn decode_next_packet(data: &[u8]) -> (PacketLineRef<'_>, usize) {
            match decode::streaming(data).expect("valid packet line data") {
                decode::Stream::Complete { line, bytes_consumed } => (line, bytes_consumed),
                decode::Stream::Incomplete { bytes_needed } => {
                    panic!("incomplete packet line, need {bytes_needed} more bytes");
                }
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_packfile_sideband_encoding(
                data in proptest::collection::vec(any::<u8>(), 0..131072usize),
            ) {
                let mut output = Vec::new();
                let section = PackfileSection;
                let bytes_written = section.write(&mut output, Cursor::new(&data))
                    .expect("PackfileSection write should succeed");

                prop_assert_eq!(bytes_written, data.len() as u64, "reported bytes written should equal input length");

                let mut remaining = output.as_slice();
                let mut reassembled = Vec::new();

                prop_assert!(!remaining.is_empty(), "output must not be empty");
                let (line, consumed) = decode_next_packet(remaining);
                remaining = &remaining[consumed..];
                match line {
                    PacketLineRef::Data(d) => {
                        let text = d.strip_suffix(b"\n").unwrap_or(d);
                        prop_assert_eq!(text, b"packfile" as &[u8], "first packet line should be 'packfile' header");
                    }
                    other => { prop_assert!(false, "expected Data packet for header, got {:?}", other); }
                }

                while !remaining.is_empty() {
                    let (line, consumed) = decode_next_packet(remaining);
                    remaining = &remaining[consumed..];

                    match line {
                        PacketLineRef::Data(d) => {
                            let band = PacketLineRef::Data(d).decode_band()
                                .expect("sideband packet should decode successfully");
                            match band {
                                BandRef::Data(payload) => {
                                    prop_assert!(
                                        payload.len() <= MAX_SIDEBAND_DATA_BYTES,
                                        "each sideband data payload must be <= MAX_SIDEBAND_DATA_BYTES (65515), got {}",
                                        payload.len()
                                    );
                                    reassembled.extend_from_slice(payload);
                                }
                                other => { prop_assert!(false, "expected Data band, got {:?}", other); }
                            }
                        }
                        PacketLineRef::Flush | PacketLineRef::Delimiter | PacketLineRef::ResponseEnd => {
                            prop_assert!(false, "unexpected non-data packet: {:?}", line);
                        }
                    }
                }

                prop_assert_eq!(reassembled, data, "concatenation of sideband payloads must equal original input");
            }
        }
    }

    // Property: Optional section framing and empty-section skipping
    // **Validates: Requirements 1.3, 4.2**
    #[cfg(feature = "blocking-server")]
    mod property_optional_section_framing {
        use super::*;
        use proptest::prelude::*;
        use crate::upload_pack::response::write_fetch_metadata_sections;

        fn arb_oid() -> impl Strategy<Value = gix_hash::ObjectId> {
            proptest::collection::vec(any::<u8>(), 20)
                .prop_map(|bytes| {
                    let mut buf = [0u8; 20];
                    buf.copy_from_slice(&bytes);
                    gix_hash::ObjectId::from_bytes_or_panic(&buf)
                })
        }

        fn arb_shallow_update() -> impl Strategy<Value = ShallowUpdate> {
            (any::<bool>(), arb_oid()).prop_map(|(is_shallow, oid)| {
                if is_shallow { ShallowUpdate::Shallow(oid) } else { ShallowUpdate::Unshallow(oid) }
            })
        }

        fn arb_wanted_ref() -> impl Strategy<Value = WantedRef> {
            (arb_oid(), "refs/[a-z]{1,10}(/[a-z]{1,8}){0,3}")
                .prop_map(|(id, path)| WantedRef { id, path: BString::from(path.as_bytes()) })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn property_optional_section_framing(
                shallow_updates in proptest::collection::vec(arb_shallow_update(), 0..=20),
                wanted_refs in proptest::collection::vec(arb_wanted_ref(), 0..=20),
            ) {
                let mut output = Vec::new();
                write_fetch_metadata_sections(
                    &mut output,
                    &[],
                    &shallow_updates,
                    &wanted_refs,
                    true,
                ).expect("writing metadata sections should not fail");

                if shallow_updates.is_empty() && wanted_refs.is_empty() {
                    prop_assert_eq!(output.len(), 0, "empty sections should produce no output bytes");
                } else {
                    prop_assert!(output.len() > 0, "non-empty sections should produce output bytes");
                }
            }
        }
    }

    // Property: Negotiation acknowledgement correctness
    // **Validates: Requirements 1.4, 4.4**
    #[cfg(feature = "blocking-server")]
    mod property_negotiation_ack_correctness {
        use super::*;
        use proptest::prelude::*;
        use crate::upload_pack::negotiate::NegotiationState;

        fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
            proptest::collection::vec(
                prop::num::u8::ANY.prop_map(|b| b"0123456789abcdef"[(b & 0x0f) as usize]),
                40,
            )
            .prop_map(|hex_bytes| {
                gix_hash::ObjectId::from_hex(&hex_bytes).expect("valid hex for arb_object_id")
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn property_negotiation_ack_correctness(
                done in any::<bool>(),
                common_haves in proptest::collection::vec(arb_object_id(), 0..=20),
            ) {
                let request = Fetch { done, ..Default::default() };
                let mut state = NegotiationState::new(&request);

                for id in &common_haves {
                    state.acknowledge_have(*id);
                }

                let acks = state.acknowledgements();

                let mut seen = BTreeSet::new();
                let unique_haves: Vec<_> = common_haves
                    .iter()
                    .filter(|id| seen.insert(**id))
                    .copied()
                    .collect();

                if done {
                    // Per spec: when done=true, acknowledgments MUST be omitted entirely.
                    prop_assert!(
                        acks.is_empty(),
                        "done=true must always produce empty acknowledgements (section omitted per spec), got {:?}",
                        acks
                    );
                } else {
                    if unique_haves.is_empty() {
                        prop_assert_eq!(acks, vec![Acknowledgement::Nak]);
                    } else {
                        prop_assert_eq!(acks.len(), unique_haves.len());
                        for (entry, expected_id) in acks.iter().zip(unique_haves.iter()) {
                            prop_assert_eq!(*entry, Acknowledgement::Common(*expected_id));
                        }
                        prop_assert!(!acks.contains(&Acknowledgement::Ready));
                        prop_assert!(!acks.contains(&Acknowledgement::Nak));
                    }
                }
            }
        }
    }

    // Property 5: Have deduplication
    // **Validates: Requirements 3.2**
    #[cfg(feature = "blocking-server")]
    mod property_have_deduplication {
        use super::*;
        use super::negotiate::NegotiationState;
        use proptest::prelude::*;

        fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
            proptest::collection::vec(any::<u8>(), 20).prop_map(|bytes| {
                let mut buf = [0u8; 20];
                buf.copy_from_slice(&bytes);
                gix_hash::ObjectId::from_bytes_or_panic(&buf)
            })
        }

        fn arb_oid_list_with_duplicates() -> impl Strategy<Value = Vec<gix_hash::ObjectId>> {
            proptest::collection::vec(arb_object_id(), 2..=8).prop_flat_map(|pool| {
                let pool_len = pool.len();
                proptest::collection::vec(0..pool_len, 4..=30).prop_map(move |indices| {
                    indices.iter().map(|&i| pool[i]).collect()
                })
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn property_have_deduplication(
                oid_list in arb_oid_list_with_duplicates(),
                existence_seed in any::<u64>(),
            ) {
                let request = Fetch { done: false, ..Default::default() };
                let mut state = NegotiationState::new(&request);

                let exists = |id: &gix_hash::ObjectId| -> bool {
                    let bytes = id.as_bytes();
                    let hash = bytes[0] as u64 ^ bytes[1] as u64 ^ existence_seed;
                    hash % 3 != 0
                };

                let mut expected_order: Vec<gix_hash::ObjectId> = Vec::new();
                let mut seen: BTreeSet<gix_hash::ObjectId> = BTreeSet::new();

                for id in &oid_list {
                    if exists(id) {
                        state.acknowledge_have(*id);
                        if seen.insert(*id) {
                            expected_order.push(*id);
                        }
                    }
                }

                let acks = state.acknowledgements();

                if expected_order.is_empty() {
                    prop_assert_eq!(acks, vec![Acknowledgement::Nak]);
                } else {
                    prop_assert_eq!(acks.len(), expected_order.len());
                    for (i, (ack, expected_id)) in acks.iter().zip(expected_order.iter()).enumerate() {
                        prop_assert_eq!(*ack, Acknowledgement::Common(*expected_id),
                            "acknowledgement at index {} must be Common with the OID in first-seen order", i);
                    }
                    let mut output_ids: BTreeSet<gix_hash::ObjectId> = BTreeSet::new();
                    for ack in &acks {
                        if let Acknowledgement::Common(id) = ack {
                            prop_assert!(output_ids.insert(*id), "duplicate OID in output: {:?}", id);
                        }
                    }
                }
            }
        }
    }

    // Property 6: Readiness predicate
    // **Validates: Requirements 3.3, 3.5**
    mod property_readiness_predicate {
        use super::*;
        use super::negotiate::NegotiationState;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn property_readiness_predicate(
                done in any::<bool>(),
                wait_for_done in any::<bool>(),
                common_have_count in 0u8..100,
            ) {
                let request = Fetch { done, wait_for_done, ..Default::default() };
                let mut state = NegotiationState::new(&request);

                for i in 0..common_have_count {
                    let mut bytes = [0u8; 20];
                    bytes[0] = i;
                    bytes[1] = (i as u8).wrapping_mul(37);
                    let id = gix_hash::ObjectId::from_bytes_or_panic(&bytes);
                    state.acknowledge_have(id);
                }

                let ready = state.is_ready();
                prop_assert_eq!(ready, done,
                    "is_ready() must equal `done` regardless of wait_for_done={} or common_have_count={}",
                    wait_for_done, common_have_count);
            }
        }
    }
}
