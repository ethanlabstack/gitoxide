//! Blocking server-side plumbing for `receive-pack` protocol interactions.
//!
//! This module parses incoming push command sections (including negotiated capabilities and
//! optional push-options), exposes the remaining input as pack data to a delegate, and writes
//! report-status responses in plain packet-line or sideband mode.

use std::io::{self, Write as _};

use bstr::{BStr, BString, ByteSlice, ByteVec};
use gix_transport::packetline::{
    Channel, PacketLineRef,
    blocking_io::{Writer, encode},
    decode,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

const MAX_SIDEBAND_DATA_BYTES: usize = 65_515;
const V2_SECTION_REF_UPDATES: &str = "section=ref-updates";
const V2_SECTION_PUSH_OPTIONS: &str = "section=push-options";
const V2_SECTION_REPORT_STATUS: &str = "report-status";
const V2_SECTION_MESSAGES: &str = "messages";

/// Capabilities the server supports for receive-pack V1.
///
/// The `agent` capability is always implicitly accepted from clients but
/// is listed here because it appears in the advertisement.
pub const SERVER_CAPABILITIES: &[&str] = &[
    "report-status",
    "report-status-v2",
    "side-band-64k",
    "delete-refs",
    "push-options",
    "atomic",
    "quiet",
    "no-thin",
    "object-format",
    "agent",
];

/// Server-side configuration for receive-pack capability and protocol validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// The object hash algorithm this server uses.
    pub object_hash: gix_hash::Kind,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            object_hash: gix_hash::Kind::Sha1,
        }
    }
}

/// Build the capability list for a V1 ref advertisement.
///
/// Includes all [`SERVER_CAPABILITIES`] with appropriate values:
/// - `object-format=<hash_name>` from `config.object_hash`
/// - `agent=gix/<version>` with the crate version
///
/// Capabilities that do not carry a value are represented as `(name, None)`.
pub fn server_capability_advertisement(config: &ServerConfig) -> Vec<(&'static str, Option<String>)> {
    SERVER_CAPABILITIES
        .iter()
        .map(|&name| match name {
            "object-format" => (name, Some(config.object_hash.to_string())),
            "agent" => (name, Some(format!("gix/{}", env!("CARGO_PKG_VERSION")))),
            _ => (name, None),
        })
        .collect()
}

/// Validate client capabilities against the server's supported set.
///
/// Returns `Ok(())` if all client capabilities are recognized.
/// Returns an error identifying the first unsupported capability.
///
/// The `agent` capability is always accepted regardless of value.
/// `report-status-v2` requires `side-band-64k` to also be present.
pub fn validate_capabilities(capabilities: &[Capability]) -> Result<(), Error> {
    for cap in capabilities {
        if cap.name.as_bstr() == "agent".as_bytes().as_bstr() {
            continue;
        }
        if !SERVER_CAPABILITIES
            .iter()
            .any(|&known| cap.name.as_bstr() == known.as_bytes().as_bstr())
        {
            return Err(Error::UnsupportedCapability {
                name: cap.name.clone(),
            });
        }
    }

    let has_report_status_v2 = capabilities
        .iter()
        .any(|cap| cap.name.as_bstr() == "report-status-v2".as_bytes().as_bstr());
    let has_sideband_64k = capabilities
        .iter()
        .any(|cap| cap.name.as_bstr() == "side-band-64k".as_bytes().as_bstr());

    if has_report_status_v2 && !has_sideband_64k {
        return Err(Error::ReportStatusV2RequiresSideband);
    }

    Ok(())
}

/// Validate the client's object-format capability against the server config.
///
/// If the client sends `object-format=<algo>`, it must match `config.object_hash`.
/// If absent, `sha1` is assumed as the default.
/// Returns an error if the value is not a recognized algorithm name or if it does
/// not match the server's configured hash.
pub fn validate_object_format(capabilities: &[Capability], config: &ServerConfig) -> Result<(), Error> {
    let object_format_cap = capabilities
        .iter()
        .find(|cap| cap.name.as_bstr() == "object-format".as_bytes().as_bstr());

    let requested_name: BString = match object_format_cap {
        Some(cap) => cap.value.clone().unwrap_or_else(|| "sha1".into()),
        None => "sha1".into(),
    };

    let requested_kind = requested_name
        .to_str()
        .ok()
        .and_then(|s| s.parse::<gix_hash::Kind>().ok())
        .ok_or_else(|| Error::InvalidObjectFormat {
            value: requested_name.clone(),
        })?;

    if requested_kind != config.object_hash {
        return Err(Error::UnsupportedObjectFormat {
            requested: requested_name,
            supported: config.object_hash.to_string().into(),
        });
    }

    Ok(())
}

/// Configuration controlling per-session behavior derived from negotiated capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConfig {
    /// If true, reject thin packs (pass `None` for ODB lookup during ingestion).
    pub no_thin: bool,
    /// If true, use atomic transaction mode (all-or-nothing).
    pub atomic: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            no_thin: false,
            atomic: false,
        }
    }
}

impl SessionConfig {
    /// Derive session configuration from negotiated client capabilities.
    pub fn from_request(request: &Request) -> Self {
        Self {
            no_thin: request.has_capability("no-thin"),
            atomic: request.has_capability("atomic"),
        }
    }
}

/// Async transport integration for receive-pack server plumbing.
#[cfg(feature = "async-client")]
pub mod async_io;

/// Server-side receive-pack handler implementing pack ingestion, connectivity checking,
/// and atomic ref transactions for one push session.
pub mod handler;

/// A parsed receive-pack capability from the first update command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    /// Capability name, like `report-status-v2` or `side-band-64k`.
    pub name: BString,
    /// Optional capability value for key-value capabilities.
    pub value: Option<BString>,
}

/// A parsed feature line from the V2 command header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Feature {
    /// The feature name, like `agent` or `report-status-v2`.
    pub name: BString,
    /// Optional feature value for key-value features.
    pub value: Option<BString>,
}

/// A capability line to advertise in protocol V2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Capability {
    /// Capability name, like `push` or `server-option`.
    pub name: BString,
    /// Optional capability values associated with `name`.
    pub values: Vec<BString>,
}

/// A single requested ref update in a push command list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    /// Expected old object id currently at `ref_name`.
    pub old_id: gix_hash::ObjectId,
    /// New object id to update `ref_name` to.
    pub new_id: gix_hash::ObjectId,
    /// Fully qualified reference name to update.
    pub ref_name: BString,
}

/// Parsed `receive-pack` request metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Parsed capabilities advertised by the client in the first update command.
    pub capabilities: Vec<Capability>,
    /// Parsed update commands from the command section.
    pub updates: Vec<Update>,
    /// Optional push-options section entries (if negotiated and provided).
    pub push_options: Vec<BString>,
}

/// Parsed receive-pack protocol V2 request metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Request {
    /// Header features associated with the command request.
    pub features: Vec<Feature>,
    /// Parsed receive-pack request payload.
    pub request: Request,
    /// If true, additional bytes are present after the argument section and represent pack data.
    pub has_pack: bool,
}

impl Request {
    /// Returns true if the request contains a capability with `name`.
    pub fn has_capability(&self, name: &str) -> bool {
        let name = name.as_bytes().as_bstr();
        self.capabilities
            .iter()
            .any(|capability| capability.name.as_bstr() == name)
    }

    fn uses_sideband(&self) -> bool {
        self.has_capability("side-band") || self.has_capability("side-band-64k")
    }

    fn wants_report_status(&self) -> bool {
        self.has_capability("report-status") || self.has_capability("report-status-v2")
    }
}

/// Status of unpacking the received pack data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnpackStatus {
    /// Pack unpacking succeeded.
    Ok,
    /// Pack unpacking failed with a message.
    Error(BString),
}

/// Per-reference report-status entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefStatus {
    /// Reference update succeeded.
    Ok {
        /// Updated reference name.
        ref_name: BString,
    },
    /// Reference update failed with a message.
    Rejected {
        /// Rejected reference name.
        ref_name: BString,
        /// Rejection reason.
        message: BString,
    },
}

/// Kind of sideband message to send before/after report-status data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SidebandMessageKind {
    /// Sideband progress channel (`2`).
    Progress,
    /// Sideband error channel (`3`).
    Error,
}

/// A sideband message emitted during push processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidebandMessage {
    /// Target sideband channel.
    pub kind: SidebandMessageKind,
    /// Message payload bytes.
    pub text: BString,
}

/// Delegate-provided receive-pack response data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// Overall unpack result.
    pub unpack_status: UnpackStatus,
    /// Per-reference results.
    pub ref_statuses: Vec<RefStatus>,
    /// Optional sideband progress/error messages.
    pub sideband_messages: Vec<SidebandMessage>,
}

impl Default for Response {
    fn default() -> Self {
        Response {
            unpack_status: UnpackStatus::Ok,
            ref_statuses: Vec::new(),
            sideband_messages: Vec::new(),
        }
    }
}

/// Outcome of serving one receive-pack push request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// Number of update commands parsed from the request.
    pub updates_received: usize,
    /// Number of push-options parsed from the optional push-options section.
    pub push_options_received: usize,
    /// Number of per-ref statuses sent.
    pub ref_statuses_sent: usize,
    /// Whether a report-status payload was written.
    pub report_status_sent: bool,
    /// Number of bytes written onto sideband channels.
    pub sideband_bytes_sent: u64,
}

/// Delegate implementation used by [`serve_v1()`] to process received pushes.
pub trait Delegate {
    /// Process a parsed `receive-pack` request and consume pack data from `pack_data`.
    fn receive(&mut self, request: &Request, pack_data: &mut dyn io::Read) -> Result<Response, BoxError>;
}

/// Errors returned while parsing receive-pack requests and writing responses.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Decode(#[from] decode::Error),
    #[error("Expected at least one update command before the command-section flush")]
    MissingUpdateCommands,
    #[error("Expected `command=<name>` in receive-pack V2 request header")]
    MissingV2Command,
    #[error("Unsupported receive-pack V2 command {command:?}")]
    UnsupportedV2Command { command: BString },
    #[error("Malformed receive-pack V2 header line {line:?}")]
    MalformedV2HeaderLine { line: BString },
    #[error("Expected a V2 argument section after the header delimiter")]
    MissingV2ArgumentSection,
    #[error("Expected at least one section in receive-pack V2 request arguments")]
    MissingV2SectionHeader,
    #[error("Expected a `section=ref-updates` section in receive-pack V2 request arguments")]
    MissingV2RefUpdatesSection,
    #[error("Unknown receive-pack V2 section header {section:?}")]
    UnknownV2Section { section: BString },
    #[error("Duplicate receive-pack V2 section header {section:?}")]
    DuplicateV2Section { section: BString },
    #[error("Unexpected packet line type {line_type} in receive-pack request section")]
    UnexpectedPacketLineType { line_type: &'static str },
    #[error("Malformed receive-pack update command line {line:?}")]
    MalformedCommandLine { line: BString },
    #[error("Could not parse object id in command line {line:?}")]
    InvalidObjectId {
        line: BString,
        #[source]
        source: gix_hash::decode::Error,
    },
    #[error("Delegate failed")]
    Delegate(#[source] BoxError),
    #[error("Unsupported capability {name:?} — not in server capability set")]
    UnsupportedCapability {
        /// The capability name that is not supported.
        name: BString,
    },
    #[error("report-status-v2 requires side-band-64k transport")]
    ReportStatusV2RequiresSideband,
    #[error("Invalid object-format value {value:?}")]
    InvalidObjectFormat {
        /// The unrecognized object-format value sent by the client.
        value: BString,
    },
    #[error("Object-format mismatch: client requested {requested:?}, server supports {supported:?}")]
    UnsupportedObjectFormat {
        /// The hash algorithm name requested by the client.
        requested: BString,
        /// The hash algorithm name the server is configured with.
        supported: BString,
    },
}

/// Parse a protocol V1 receive-pack request from `input`, leaving `input` positioned at pack data.
///
/// The parser consumes:
/// - command section (`old new ref` lines) until flush
/// - optional push-options section if negotiated and present
///
/// Remaining bytes in `input` can be interpreted as pack data by the caller.
pub fn parse_v1_request(input: &mut impl io::BufRead) -> Result<Request, Error> {
    let mut command_lines = read_text_packet_lines_until_flush(input)?;
    if command_lines.is_empty() {
        return Err(Error::MissingUpdateCommands);
    }

    let first_line = command_lines.remove(0);
    let (first_command, capabilities) = split_first_command_and_capabilities(first_line.as_bstr());
    let mut updates = Vec::with_capacity(command_lines.len() + 1);
    updates.push(parse_update_command(first_command.as_bstr())?);
    for line in command_lines {
        updates.push(parse_update_command(line.as_bstr())?);
    }

    let push_options = if capabilities
        .iter()
        .any(|capability| capability.name.as_bstr() == "push-options".as_bytes().as_bstr())
    {
        read_optional_push_options(input)?
    } else {
        Vec::new()
    };

    Ok(Request {
        capabilities,
        updates,
        push_options,
    })
}

/// Parse a protocol V2 receive-pack request from `input`, leaving `input` positioned at optional pack data.
///
/// The parser consumes:
/// - command request header lines through delimiter (expects `command=push`)
/// - argument sections encoded as text packet lines:
///   - `section=ref-updates` (required)
///   - `section=push-options` (optional)
///   - each section terminated by delimiter or final flush
///
/// Remaining bytes in `input` can be interpreted as pack data by the caller.
pub fn parse_v2_request(input: &mut impl io::BufRead) -> Result<V2Request, Error> {
    let (header_lines, header_terminator) = read_text_packet_lines_until_delimiter_or_flush(input)?;
    if header_terminator != SectionTerminator::Delimiter {
        return Err(Error::MissingV2ArgumentSection);
    }

    let (command, features) = parse_v2_header_lines(header_lines)?;
    if command.as_bstr() != "push".as_bytes().as_bstr() {
        return Err(Error::UnsupportedV2Command { command });
    }

    let mut updates = None::<Vec<Update>>;
    let mut push_options = None::<Vec<BString>>;
    loop {
        let (section_lines, section_terminator) = read_text_packet_lines_until_delimiter_or_flush(input)?;
        if section_lines.is_empty() {
            return Err(Error::MissingV2SectionHeader);
        }
        let section = section_lines[0].clone();
        let mut payload = section_lines;
        payload.remove(0);

        match section.as_bstr() {
            section_name if section_name == V2_SECTION_REF_UPDATES.as_bytes().as_bstr() => {
                if updates.is_some() {
                    return Err(Error::DuplicateV2Section { section });
                }
                if payload.is_empty() {
                    return Err(Error::MissingUpdateCommands);
                }
                let mut parsed = Vec::with_capacity(payload.len());
                for line in payload {
                    parsed.push(parse_update_command(line.as_bstr())?);
                }
                updates = Some(parsed);
            }
            section_name if section_name == V2_SECTION_PUSH_OPTIONS.as_bytes().as_bstr() => {
                if push_options.is_some() {
                    return Err(Error::DuplicateV2Section { section });
                }
                push_options = Some(payload);
            }
            _ => return Err(Error::UnknownV2Section { section }),
        }

        if section_terminator == SectionTerminator::Flush {
            break;
        }
    }

    let updates = updates.ok_or(Error::MissingV2RefUpdatesSection)?;
    let capabilities = features
        .iter()
        .map(|feature| Capability {
            name: feature.name.clone(),
            value: feature.value.clone(),
        })
        .collect::<Vec<_>>();
    let has_pack = !input.fill_buf()?.is_empty();

    Ok(V2Request {
        features,
        request: Request {
            capabilities,
            updates,
            push_options: push_options.unwrap_or_default(),
        },
        has_pack,
    })
}

/// Serve one protocol V1 receive-pack push request end-to-end.
///
/// The `config` parameter controls object-format validation.
/// Client capabilities are validated against [`SERVER_CAPABILITIES`] before
/// invoking the delegate.
///
/// If the client sends an immediate flush (no update commands), this is treated
/// as a no-op push ("nothing to do") and returns a zero-count outcome without
/// invoking the delegate. This is the standard git client behavior when all
/// refs are already up-to-date.
pub fn serve_v1(
    input: impl io::Read,
    mut output: impl io::Write,
    delegate: &mut impl Delegate,
    config: &ServerConfig,
) -> Result<Outcome, Error> {
    let mut input = io::BufReader::new(input);
    let request = match parse_v1_request(&mut input) {
        Ok(request) => request,
        Err(Error::MissingUpdateCommands) => {
            // Client sent an immediate flush — nothing to push (refs up-to-date).
            // Write a minimal flush response and return a no-op outcome.
            encode::flush_to_write(&mut output)?;
            return Ok(Outcome {
                updates_received: 0,
                push_options_received: 0,
                ref_statuses_sent: 0,
                report_status_sent: false,
                sideband_bytes_sent: 0,
            });
        }
        Err(e) => return Err(e),
    };

    // No-op push: client sent command section but with no actual updates.
    if request.updates.is_empty() {
        encode::flush_to_write(&mut output)?;
        return Ok(Outcome {
            updates_received: 0,
            push_options_received: request.push_options.len(),
            ref_statuses_sent: 0,
            report_status_sent: false,
            sideband_bytes_sent: 0,
        });
    }

    // Validate capabilities before invoking delegate.
    validate_capabilities(&request.capabilities)?;
    validate_object_format(&request.capabilities, config)?;

    let response = delegate.receive(&request, &mut input).map_err(Error::Delegate)?;
    let report_status_sent = request.wants_report_status();
    let sideband_bytes_sent = write_v1_response(&mut output, &request, &response)?;

    Ok(Outcome {
        updates_received: request.updates.len(),
        push_options_received: request.push_options.len(),
        ref_statuses_sent: response.ref_statuses.len(),
        report_status_sent,
        sideband_bytes_sent,
    })
}

/// Write a receive-pack response matching `request` capabilities.
///
/// When `quiet` is negotiated, sideband progress messages (channel 2) are
/// suppressed. Error messages (channel 3) are always transmitted.
///
/// The response format depends on the negotiated capabilities:
/// - When `report-status` or `report-status-v2` is negotiated, the unpack-status
///   and per-ref status lines are written.
/// - When `side-band-64k` is negotiated *and* there is report-status data or
///   sideband messages to send, the payload is wrapped in sideband data frames.
/// - When neither `report-status` nor `report-status-v2` is negotiated, only a
///   flush packet is written regardless of other capabilities.
///
/// Returns the number of payload bytes written through sideband channels.
pub fn write_v1_response(mut output: impl io::Write, request: &Request, response: &Response) -> Result<u64, Error> {
    let mut sideband_bytes_sent = 0u64;
    let wants_report_status = request.wants_report_status();
    let uses_sideband = request.uses_sideband();
    let is_quiet = request.has_capability("quiet");

    // When neither report-status nor report-status-v2 is negotiated, write only a flush.
    if !wants_report_status {
        encode::flush_to_write(&mut output)?;
        return Ok(0);
    }

    let report_status_payload = encode_report_status_payload(response)?;

    if uses_sideband {
        // Write sideband messages, suppressing progress when quiet is negotiated.
        for message in &response.sideband_messages {
            if is_quiet && message.kind == SidebandMessageKind::Progress {
                continue;
            }
            let channel = match message.kind {
                SidebandMessageKind::Progress => Channel::Progress,
                SidebandMessageKind::Error => Channel::Error,
            };
            let payload: &[u8] = message.text.as_ref();
            sideband_bytes_sent += payload.len() as u64;
            encode::band_to_write(channel, payload, &mut output)?;
        }
        // Write report-status payload wrapped in sideband data frames.
        for chunk in report_status_payload.chunks(MAX_SIDEBAND_DATA_BYTES) {
            sideband_bytes_sent += chunk.len() as u64;
            encode::band_to_write(Channel::Data, chunk, &mut output)?;
        }
        encode::flush_to_write(&mut output)?;
        return Ok(sideband_bytes_sent);
    }

    // Non-sideband path: write report-status payload directly.
    output.write_all(&report_status_payload)?;
    Ok(0)
}

/// Serve one protocol V2 receive-pack push request end-to-end.
pub fn serve_v2(
    input: impl io::Read,
    mut output: impl io::Write,
    delegate: &mut impl Delegate,
) -> Result<Outcome, Error> {
    let mut input = io::BufReader::new(input);
    let request = parse_v2_request(&mut input)?;
    let response = if request.has_pack {
        delegate
            .receive(&request.request, &mut input)
            .map_err(Error::Delegate)?
    } else {
        let mut empty = io::empty();
        delegate
            .receive(&request.request, &mut empty)
            .map_err(Error::Delegate)?
    };

    let report_status_sent = request.request.wants_report_status();
    let sideband_bytes_sent = write_v2_response(&mut output, &request.request, &response)?;

    Ok(Outcome {
        updates_received: request.request.updates.len(),
        push_options_received: request.request.push_options.len(),
        ref_statuses_sent: response.ref_statuses.len(),
        report_status_sent,
        sideband_bytes_sent,
    })
}

/// Write a protocol V2 capability advertisement, including the `version 2` line.
pub fn write_v2_capability_advertisement(
    mut output: impl io::Write,
    capabilities: &[V2Capability],
) -> Result<(), Error> {
    let mut writer = Writer::new(&mut output);
    writer.enable_text_mode();
    writer.write_all(b"version 2")?;
    for capability in capabilities {
        let mut line = capability.name.clone();
        if !capability.values.is_empty() {
            line.push_byte(b'=');
            for (idx, value) in capability.values.iter().enumerate() {
                if idx != 0 {
                    line.push_byte(b' ');
                }
                line.push_str(value);
            }
        }
        writer.write_all(line.as_ref())?;
    }
    encode::flush_to_write(writer.inner_mut())?;
    Ok(())
}

/// A single reference to include in a V1 ref advertisement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdvertisedRef {
    /// The object id this ref points to (hex-encoded 40 chars).
    pub oid: gix_hash::ObjectId,
    /// Fully qualified reference name (e.g. `refs/heads/main`).
    pub ref_name: BString,
}

/// Write a protocol V1 Smart HTTP ref advertisement for `receive-pack`.
///
/// This is what a git client expects from `GET /info/refs?service=git-receive-pack`.
/// The output includes the service header, a flush, then ref lines with capabilities
/// on the first line (after a NUL byte), terminated by a final flush.
///
/// For empty repositories (no refs), a zero-id capabilities line is emitted.
///
/// Format:
/// ```text
/// pkt-line: # service=git-receive-pack\n
/// 0000
/// pkt-line: <sha1> <refname>\0 <capabilities>\n   (first ref)
/// pkt-line: <sha1> <refname>\n                    (subsequent refs)
/// 0000
/// ```
pub fn write_v1_ref_advertisement(
    mut output: impl io::Write,
    refs: &[AdvertisedRef],
    capabilities: &[(&str, Option<&str>)],
) -> Result<(), Error> {
    // Build the space-separated capability string
    let cap_string: String = capabilities
        .iter()
        .map(|(name, value)| match value {
            Some(v) => format!("{name}={v}"),
            None => name.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ");

    // Service header
    {
        let mut writer = Writer::new(&mut output);
        writer.enable_text_mode();
        writer.write_all(b"# service=git-receive-pack")?;
    }
    encode::flush_to_write(&mut output)?;

    // Ref lines
    {
        let mut writer = Writer::new(&mut output);
        writer.enable_text_mode();

        if refs.is_empty() {
            // Empty repo: zero-oid with capabilities
            let null_oid = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
            let line = format!("{null_oid} capabilities^{{}}\0{cap_string}");
            writer.write_all(line.as_bytes())?;
        } else {
            // First ref line includes capabilities after NUL byte
            let first = &refs[0];
            let line = format!("{} {}\0{cap_string}", first.oid, first.ref_name);
            writer.write_all(line.as_bytes())?;

            // Subsequent ref lines are plain
            for advertised_ref in &refs[1..] {
                let line = format!("{} {}", advertised_ref.oid, advertised_ref.ref_name);
                writer.write_all(line.as_bytes())?;
            }
        }
    }
    encode::flush_to_write(&mut output)?;

    Ok(())
}

/// Write a receive-pack V2 response with sectioned report-status and optional message sections.
pub fn write_v2_response(mut output: impl io::Write, request: &Request, response: &Response) -> Result<u64, Error> {
    let mut writer = Writer::new(&mut output);
    writer.enable_text_mode();
    let mut wrote_section = false;

    if request.wants_report_status() {
        writer.write_all(V2_SECTION_REPORT_STATUS.as_bytes())?;
        writer.write_all(format_unpack_status_line(&response.unpack_status).as_ref())?;
        for status in &response.ref_statuses {
            writer.write_all(format_ref_status_line(status).as_ref())?;
        }
        wrote_section = true;
    }

    if !response.sideband_messages.is_empty() {
        if wrote_section {
            encode::delim_to_write(writer.inner_mut())?;
        }
        writer.write_all(V2_SECTION_MESSAGES.as_bytes())?;
        for message in &response.sideband_messages {
            writer.write_all(format_v2_message_line(message).as_ref())?;
        }
    }

    encode::flush_to_write(writer.inner_mut())?;
    Ok(0)
}

fn read_optional_push_options(input: &mut impl io::BufRead) -> Result<Vec<BString>, Error> {
    let Some(first_byte) = input.fill_buf()?.first().copied() else {
        return Ok(Vec::new());
    };
    if !first_byte.is_ascii_hexdigit() {
        return Ok(Vec::new());
    }
    read_text_packet_lines_until_flush(input)
}

fn read_text_packet_lines_until_flush(input: &mut impl io::BufRead) -> Result<Vec<BString>, Error> {
    let mut lines = Vec::new();
    loop {
        let mut hex_bytes = [0u8; 4];
        input.read_exact(&mut hex_bytes)?;
        match decode::hex_prefix(&hex_bytes)? {
            decode::PacketLineOrWantedSize::Line(PacketLineRef::Flush) => break,
            decode::PacketLineOrWantedSize::Line(other) => {
                return Err(Error::UnexpectedPacketLineType {
                    line_type: packet_line_kind(&other),
                });
            }
            decode::PacketLineOrWantedSize::Wanted(data_len) => {
                let mut data = vec![0u8; data_len as usize];
                input.read_exact(&mut data)?;
                if data.last() == Some(&b'\n') {
                    data.pop();
                    if data.last() == Some(&b'\r') {
                        data.pop();
                    }
                }
                lines.push(data.into());
            }
        }
    }
    Ok(lines)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SectionTerminator {
    Delimiter,
    Flush,
}

fn read_text_packet_lines_until_delimiter_or_flush(
    input: &mut impl io::BufRead,
) -> Result<(Vec<BString>, SectionTerminator), Error> {
    let mut lines = Vec::new();
    loop {
        let mut hex_bytes = [0u8; 4];
        input.read_exact(&mut hex_bytes)?;
        match decode::hex_prefix(&hex_bytes)? {
            decode::PacketLineOrWantedSize::Line(PacketLineRef::Delimiter) => {
                return Ok((lines, SectionTerminator::Delimiter));
            }
            decode::PacketLineOrWantedSize::Line(PacketLineRef::Flush) => return Ok((lines, SectionTerminator::Flush)),
            decode::PacketLineOrWantedSize::Line(other) => {
                return Err(Error::UnexpectedPacketLineType {
                    line_type: packet_line_kind(&other),
                });
            }
            decode::PacketLineOrWantedSize::Wanted(data_len) => {
                let mut data = vec![0u8; data_len as usize];
                input.read_exact(&mut data)?;
                if data.last() == Some(&b'\n') {
                    data.pop();
                    if data.last() == Some(&b'\r') {
                        data.pop();
                    }
                }
                lines.push(data.into());
            }
        }
    }
}

fn parse_v2_header_lines(lines: Vec<BString>) -> Result<(BString, Vec<Feature>), Error> {
    let mut command = None::<BString>;
    let mut features = Vec::new();
    for line in lines {
        let bytes: &[u8] = line.as_ref();
        if let Some(command_name) = bytes.strip_prefix(b"command=") {
            if command.is_some() || command_name.is_empty() {
                return Err(Error::MalformedV2HeaderLine { line });
            }
            command = Some(command_name.into());
            continue;
        }
        features.push(parse_v2_feature_line(line.as_bstr())?);
    }
    let command = command.ok_or(Error::MissingV2Command)?;
    Ok((command, features))
}

fn parse_v2_feature_line(line: &BStr) -> Result<Feature, Error> {
    if let Some((name, value)) = split_once(line, b'=') {
        if name.is_empty() {
            return Err(Error::MalformedV2HeaderLine { line: line.to_owned() });
        }
        return Ok(Feature {
            name: name.to_owned(),
            value: Some(value.to_owned()),
        });
    }
    if line.is_empty() {
        return Err(Error::MalformedV2HeaderLine { line: line.to_owned() });
    }
    Ok(Feature {
        name: line.to_owned(),
        value: None,
    })
}

fn split_first_command_and_capabilities(line: &BStr) -> (BString, Vec<Capability>) {
    match line.find_byte(0) {
        Some(nul_pos) => {
            let command = line[..nul_pos].as_bstr().to_owned();
            let capabilities = parse_capabilities(line[nul_pos + 1..].as_bstr());
            (command, capabilities)
        }
        None => (line.to_owned(), Vec::new()),
    }
}

fn parse_capabilities(raw: &BStr) -> Vec<Capability> {
    raw.split(|byte| *byte == b' ')
        .filter(|token| !token.is_empty())
        .map(|token| {
            let token = token.as_bstr();
            if let Some((name, value)) = split_once(token, b'=') {
                Capability {
                    name: name.to_owned(),
                    value: Some(value.to_owned()),
                }
            } else {
                Capability {
                    name: token.to_owned(),
                    value: None,
                }
            }
        })
        .collect()
}

fn parse_update_command(line: &BStr) -> Result<Update, Error> {
    if line.find_byte(0).is_some() {
        return Err(Error::MalformedCommandLine { line: line.to_owned() });
    }

    let mut tokens = line.splitn(3, |byte| *byte == b' ');
    let old_hex = tokens
        .next()
        .ok_or_else(|| Error::MalformedCommandLine { line: line.to_owned() })?;
    let new_hex = tokens
        .next()
        .ok_or_else(|| Error::MalformedCommandLine { line: line.to_owned() })?;
    let ref_name = tokens
        .next()
        .ok_or_else(|| Error::MalformedCommandLine { line: line.to_owned() })?;
    if old_hex.is_empty() || new_hex.is_empty() || ref_name.is_empty() {
        return Err(Error::MalformedCommandLine { line: line.to_owned() });
    }

    let old_id = gix_hash::ObjectId::from_hex(old_hex).map_err(|source| Error::InvalidObjectId {
        line: line.to_owned(),
        source,
    })?;
    let new_id = gix_hash::ObjectId::from_hex(new_hex).map_err(|source| Error::InvalidObjectId {
        line: line.to_owned(),
        source,
    })?;

    Ok(Update {
        old_id,
        new_id,
        ref_name: ref_name.as_bstr().to_owned(),
    })
}

fn encode_report_status_payload(response: &Response) -> Result<Vec<u8>, Error> {
    let mut payload = Vec::new();
    let mut writer = Writer::new(&mut payload);
    writer.enable_text_mode();
    writer.write_all(format_unpack_status_line(&response.unpack_status).as_ref())?;
    for status in &response.ref_statuses {
        writer.write_all(format_ref_status_line(status).as_ref())?;
    }
    encode::flush_to_write(writer.inner_mut())?;
    Ok(payload)
}

fn format_unpack_status_line(status: &UnpackStatus) -> BString {
    match status {
        UnpackStatus::Ok => "unpack ok".into(),
        UnpackStatus::Error(message) => {
            let mut line = BString::from("unpack ");
            line.push_str(message);
            line
        }
    }
}

fn format_ref_status_line(status: &RefStatus) -> BString {
    match status {
        RefStatus::Ok { ref_name } => {
            let mut line = BString::from("ok ");
            line.push_str(ref_name);
            line
        }
        RefStatus::Rejected { ref_name, message } => {
            let mut line = BString::from("ng ");
            line.push_str(ref_name);
            line.push_byte(b' ');
            line.push_str(message);
            line
        }
    }
}

fn format_v2_message_line(message: &SidebandMessage) -> BString {
    match message.kind {
        SidebandMessageKind::Progress => {
            let mut line = BString::from("progress ");
            line.push_str(&message.text);
            line
        }
        SidebandMessageKind::Error => {
            let mut line = BString::from("error ");
            line.push_str(&message.text);
            line
        }
    }
}

fn split_once(line: &BStr, separator: u8) -> Option<(&BStr, &BStr)> {
    let idx = line.find_byte(separator)?;
    Some((line[..idx].as_bstr(), line[idx + 1..].as_bstr()))
}

fn packet_line_kind(line: &PacketLineRef<'_>) -> &'static str {
    match line {
        PacketLineRef::Data(_) => "data",
        PacketLineRef::Flush => "flush",
        PacketLineRef::Delimiter => "delimiter",
        PacketLineRef::ResponseEnd => "response-end",
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use gix_transport::packetline::{BandRef, PacketLineRef, blocking_io::StreamingPeekableIter};

    use super::*;

    #[derive(Default)]
    struct MockDelegate {
        response: Response,
        seen_request: Option<Request>,
        seen_pack_prefix: Option<[u8; 4]>,
    }

    impl Delegate for MockDelegate {
        fn receive(&mut self, request: &Request, pack_data: &mut dyn io::Read) -> Result<Response, BoxError> {
            self.seen_request = Some(request.clone());
            let mut prefix = [0u8; 4];
            pack_data.read_exact(&mut prefix)?;
            self.seen_pack_prefix = Some(prefix);
            Ok(self.response.clone())
        }
    }

    #[test]
    fn serve_v1_parses_commands_capabilities_and_writes_sideband_report_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = request_bytes(
            &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
            &[
                "report-status-v2",
                "side-band-64k",
                "object-format=sha1",
                "agent=git/gitplane",
            ],
            &[],
            b"PACK\0\0\0\x02",
        )?;
        let mut output = Vec::new();
        let mut delegate = MockDelegate {
            response: Response {
                unpack_status: UnpackStatus::Ok,
                ref_statuses: vec![RefStatus::Ok {
                    ref_name: "refs/heads/main".into(),
                }],
                sideband_messages: Vec::new(),
            },
            ..Default::default()
        };

        let outcome = serve_v1(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
        assert_eq!(
            outcome,
            Outcome {
                updates_received: 1,
                push_options_received: 0,
                ref_statuses_sent: 1,
                report_status_sent: true,
                sideband_bytes_sent: 41,
            }
        );

        let seen = delegate
            .seen_request
            .as_ref()
            .expect("request should be visible to delegate");
        assert_eq!(seen.updates.len(), 1);
        assert_eq!(
            seen.updates[0].ref_name.as_bstr(),
            "refs/heads/main".as_bytes().as_bstr()
        );
        assert!(seen.has_capability("report-status-v2"));
        assert!(seen.has_capability("side-band-64k"));
        assert_eq!(
            delegate.seen_pack_prefix,
            Some(*b"PACK"),
            "delegate should receive pack data at the current read position"
        );

        let mut outer_reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let mut report_payload = Vec::<u8>::new();
        while let Some(line) = outer_reader.read_line() {
            let line = line??;
            match line.decode_band()? {
                BandRef::Data(data) => report_payload.extend_from_slice(data),
                BandRef::Progress(_) | BandRef::Error(_) => {}
            }
        }
        assert_eq!(outer_reader.stopped_at(), Some(PacketLineRef::Flush));

        let mut inner_reader = StreamingPeekableIter::new(report_payload.as_slice(), &[PacketLineRef::Flush], false);
        assert_eq!(
            next_text_line(&mut inner_reader)?.as_bstr(),
            "unpack ok".as_bytes().as_bstr()
        );
        assert_eq!(
            next_text_line(&mut inner_reader)?.as_bstr(),
            "ok refs/heads/main".as_bytes().as_bstr()
        );
        assert!(inner_reader.read_line().is_none());
        assert_eq!(inner_reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn serve_v1_parses_push_options_section_when_negotiated() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_bytes(
            &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
            &["report-status", "push-options"],
            &["ci.skip", "trace=1"],
            b"PACK\0\0\0\x02",
        )?;
        let mut output = Vec::new();
        let mut delegate = MockDelegate {
            response: Response {
                unpack_status: UnpackStatus::Ok,
                ref_statuses: vec![RefStatus::Ok {
                    ref_name: "refs/heads/main".into(),
                }],
                sideband_messages: Vec::new(),
            },
            ..Default::default()
        };

        let outcome = serve_v1(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
        assert_eq!(outcome.push_options_received, 2);

        let seen = delegate
            .seen_request
            .as_ref()
            .expect("request should be visible to delegate");
        assert_eq!(
            seen.push_options,
            vec![BString::from("ci.skip"), BString::from("trace=1")]
        );
        assert_eq!(delegate.seen_pack_prefix, Some(*b"PACK"));

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        assert_eq!(next_text_line(&mut reader)?.as_bstr(), "unpack ok".as_bytes().as_bstr());
        assert_eq!(
            next_text_line(&mut reader)?.as_bstr(),
            "ok refs/heads/main".as_bytes().as_bstr()
        );
        assert!(reader.read_line().is_none());
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn parse_v1_request_rejects_malformed_update_line() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_bytes(&["not-an-update-line"], &[], &[], b"PACK\0\0\0\x02")?;
        let mut input = std::io::BufReader::new(Cursor::new(request));
        let err = parse_v1_request(&mut input).expect_err("malformed command line should fail");
        assert!(matches!(err, Error::MalformedCommandLine { .. }));
        Ok(())
    }

    #[test]
    fn parse_v2_request_parses_sections_and_leaves_pack_data() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_bytes_v2(
            &["report-status-v2", "push-options", "agent=git/gitplane"],
            &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
            &["ci.skip", "trace=1"],
            b"PACK\0\0\0\x02",
        )?;
        let mut input = std::io::BufReader::new(Cursor::new(request));
        let parsed = parse_v2_request(&mut input)?;

        assert_eq!(
            parsed.features,
            vec![
                Feature {
                    name: "report-status-v2".into(),
                    value: None,
                },
                Feature {
                    name: "push-options".into(),
                    value: None,
                },
                Feature {
                    name: "agent".into(),
                    value: Some("git/gitplane".into()),
                },
            ]
        );
        assert!(parsed.has_pack);
        assert_eq!(parsed.request.updates.len(), 1);
        assert_eq!(
            parsed.request.push_options,
            vec![BString::from("ci.skip"), BString::from("trace=1")]
        );
        assert!(parsed.request.has_capability("report-status-v2"));
        assert!(parsed.request.has_capability("push-options"));
        assert_eq!(
            parsed.request.updates[0].ref_name.as_bstr(),
            "refs/heads/main".as_bytes().as_bstr()
        );

        let mut prefix = [0u8; 4];
        std::io::Read::read_exact(&mut input, &mut prefix)?;
        assert_eq!(prefix, *b"PACK");
        Ok(())
    }

    #[test]
    fn parse_v2_request_rejects_unknown_section() -> Result<(), Box<dyn std::error::Error>> {
        let mut out = Vec::new();
        {
            let mut writer = Writer::new(&mut out);
            writer.enable_text_mode();
            writer.write_all(b"command=push")?;
            encode::delim_to_write(writer.inner_mut())?;
            writer.write_all(b"section=unknown")?;
            writer.write_all(
                b"0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main",
            )?;
            encode::flush_to_write(writer.inner_mut())?;
        }
        let mut input = std::io::BufReader::new(Cursor::new(out));
        let err = parse_v2_request(&mut input).expect_err("unknown V2 section should fail");
        assert!(matches!(err, Error::UnknownV2Section { .. }));
        Ok(())
    }

    #[test]
    fn serve_v2_parses_sections_and_writes_report_status() -> Result<(), Box<dyn std::error::Error>> {
        let request = request_bytes_v2(
            &["report-status-v2", "push-options"],
            &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
            &["trace=1"],
            b"PACK\0\0\0\x02",
        )?;
        let mut output = Vec::new();
        let mut delegate = MockDelegate {
            response: Response {
                unpack_status: UnpackStatus::Ok,
                ref_statuses: vec![RefStatus::Ok {
                    ref_name: "refs/heads/main".into(),
                }],
                sideband_messages: Vec::new(),
            },
            ..Default::default()
        };

        let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate)?;
        assert_eq!(
            outcome,
            Outcome {
                updates_received: 1,
                push_options_received: 1,
                ref_statuses_sent: 1,
                report_status_sent: true,
                sideband_bytes_sent: 0,
            }
        );
        assert_eq!(delegate.seen_pack_prefix, Some(*b"PACK"));
        let seen = delegate
            .seen_request
            .as_ref()
            .expect("request should be visible to delegate");
        assert_eq!(seen.push_options, vec![BString::from("trace=1")]);
        assert!(seen.has_capability("report-status-v2"));

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        assert_eq!(
            next_text_line(&mut reader)?.as_bstr(),
            V2_SECTION_REPORT_STATUS.as_bytes().as_bstr()
        );
        assert_eq!(next_text_line(&mut reader)?.as_bstr(), "unpack ok".as_bytes().as_bstr());
        assert_eq!(
            next_text_line(&mut reader)?.as_bstr(),
            "ok refs/heads/main".as_bytes().as_bstr()
        );
        assert!(reader.read_line().is_none());
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn write_v2_capability_advertisement_includes_version_and_values() -> Result<(), Box<dyn std::error::Error>> {
        let mut output = Vec::new();
        write_v2_capability_advertisement(
            &mut output,
            &[
                V2Capability {
                    name: "push".into(),
                    values: vec!["report-status-v2".into(), "push-options".into()],
                },
                V2Capability {
                    name: "object-format".into(),
                    values: vec!["sha1".into()],
                },
            ],
        )?;

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        assert_eq!(next_text_line(&mut reader)?.as_bstr(), "version 2".as_bytes().as_bstr());
        assert_eq!(
            next_text_line(&mut reader)?.as_bstr(),
            "push=report-status-v2 push-options".as_bytes().as_bstr()
        );
        assert_eq!(
            next_text_line(&mut reader)?.as_bstr(),
            "object-format=sha1".as_bytes().as_bstr()
        );
        assert!(reader.read_line().is_none());
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    fn request_bytes(
        updates: &[&str],
        capabilities: &[&str],
        push_options: &[&str],
        pack_data: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        assert!(!updates.is_empty(), "at least one update command is required");
        let mut out = Vec::new();
        {
            let mut writer = Writer::new(&mut out);
            writer.enable_text_mode();
            let first = if capabilities.is_empty() {
                updates[0].to_owned()
            } else {
                format!("{}\0 {}", updates[0], capabilities.join(" "))
            };
            writer.write_all(first.as_bytes())?;
            for update in &updates[1..] {
                writer.write_all(update.as_bytes())?;
            }
            encode::flush_to_write(writer.inner_mut())?;

            if !push_options.is_empty() {
                for option in push_options {
                    writer.write_all(option.as_bytes())?;
                }
                encode::flush_to_write(writer.inner_mut())?;
            }
        }
        out.extend_from_slice(pack_data);
        Ok(out)
    }

    fn request_bytes_v2(
        features: &[&str],
        updates: &[&str],
        push_options: &[&str],
        pack_data: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        assert!(!updates.is_empty(), "at least one update command is required");
        let mut out = Vec::new();
        {
            let mut writer = Writer::new(&mut out);
            writer.enable_text_mode();
            writer.write_all(b"command=push")?;
            for feature in features {
                writer.write_all(feature.as_bytes())?;
            }
            encode::delim_to_write(writer.inner_mut())?;
            writer.write_all(V2_SECTION_REF_UPDATES.as_bytes())?;
            for update in updates {
                writer.write_all(update.as_bytes())?;
            }
            if push_options.is_empty() {
                encode::flush_to_write(writer.inner_mut())?;
            } else {
                encode::delim_to_write(writer.inner_mut())?;
                writer.write_all(V2_SECTION_PUSH_OPTIONS.as_bytes())?;
                for option in push_options {
                    writer.write_all(option.as_bytes())?;
                }
                encode::flush_to_write(writer.inner_mut())?;
            }
        }
        out.extend_from_slice(pack_data);
        Ok(out)
    }

    fn next_text_line(reader: &mut StreamingPeekableIter<&[u8]>) -> Result<BString, Box<dyn std::error::Error>> {
        let line = reader
            .read_line()
            .expect("expected packetline")
            .expect("read should succeed")
            .expect("decode should succeed");
        Ok(line.as_text().expect("expected text packetline").as_bstr().to_owned())
    }

    // Feature: receive-pack-v1-support, Property 1: Ref advertisement includes all server capabilities with correct values
    mod advertisement_completeness_property {
        use super::*;
        use proptest::prelude::*;

        /// Generate a random object id as 20 raw bytes for sha1.
        fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
            any::<[u8; 20]>().prop_map(|bytes| gix_hash::ObjectId::from_bytes_or_panic(&bytes))
        }

        /// Generate a random ref name that is a valid fully-qualified ref path.
        fn arb_ref_name() -> impl Strategy<Value = BString> {
            "[a-z][a-z0-9_]{1,10}".prop_map(|suffix| BString::from(format!("refs/heads/{suffix}")))
        }

        /// Generate a random AdvertisedRef.
        fn arb_advertised_ref() -> impl Strategy<Value = AdvertisedRef> {
            (arb_object_id(), arb_ref_name()).prop_map(|(oid, ref_name)| AdvertisedRef { oid, ref_name })
        }

        /// Generate a random Vec of AdvertisedRef (0 to 10 entries).
        fn arb_advertised_refs() -> impl Strategy<Value = Vec<AdvertisedRef>> {
            proptest::collection::vec(arb_advertised_ref(), 0..10)
        }

        /// Generate a random ServerConfig (only sha1 without the sha256 feature).
        fn arb_server_config() -> impl Strategy<Value = ServerConfig> {
            Just(ServerConfig {
                object_hash: gix_hash::Kind::Sha1,
            })
        }

        /// Parse capabilities from the NUL-separated portion of the first ref line.
        fn parse_capability_string(cap_str: &str) -> Vec<(&str, Option<&str>)> {
            cap_str
                .split(' ')
                .filter(|s| !s.is_empty())
                .map(|token| match token.split_once('=') {
                    Some((name, value)) => (name, Some(value)),
                    None => (token, None),
                })
                .collect()
        }

        /// Extract the capability string from the raw packet-line encoded output.
        ///
        /// The output format is:
        ///   pkt-line: # service=git-receive-pack\n
        ///   0000
        ///   pkt-line: <oid> <ref_or_capabilities^{}>\0<cap_string>\n
        ///   ...
        ///   0000
        ///
        /// We find the NUL byte in the output (which separates ref from caps on
        /// the first ref line) and extract the cap string up to the trailing newline.
        fn extract_capability_string(output: &[u8]) -> &str {
            let nul_pos = output
                .iter()
                .position(|&b| b == 0)
                .expect("output should contain a NUL byte separating ref from capabilities");
            // Capabilities run from after the NUL to the next newline (text-mode pkt-line)
            let after_nul = &output[nul_pos + 1..];
            // The pkt-line text ends at \n which is stripped by the writer,
            // but the raw data may contain it before the next pkt-line length prefix.
            // Find end of this packet's data: look for next 4-byte hex length prefix pattern
            // by finding the \n that terminates this line in the packet.
            let end = after_nul
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(after_nul.len());
            std::str::from_utf8(&after_nul[..end])
                .expect("capability string should be valid UTF-8")
        }

        // **Validates: Requirements 1.2, 1.3, 1.4**
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]
            #[test]
            fn ref_advertisement_includes_all_server_capabilities(
                refs in arb_advertised_refs(),
                config in arb_server_config(),
            ) {
                let caps = server_capability_advertisement(&config);
                let caps_borrowed: Vec<(&str, Option<&str>)> = caps
                    .iter()
                    .map(|(name, value)| (*name, value.as_deref()))
                    .collect();

                let mut output = Vec::new();
                write_v1_ref_advertisement(&mut output, &refs, &caps_borrowed)
                    .expect("write_v1_ref_advertisement should succeed");

                let cap_str = extract_capability_string(&output);
                let parsed_caps = parse_capability_string(cap_str);

                // Verify all SERVER_CAPABILITIES are present
                for &expected_cap in SERVER_CAPABILITIES {
                    let found = parsed_caps.iter().any(|(name, _)| *name == expected_cap);
                    prop_assert!(
                        found,
                        "capability {:?} should be present in advertisement, got: {:?}",
                        expected_cap,
                        cap_str
                    );
                }

                // Verify object-format value matches config
                let object_format_entry = parsed_caps
                    .iter()
                    .find(|(name, _)| *name == "object-format")
                    .expect("object-format capability should be present");
                let expected_hash_name = config.object_hash.to_string();
                prop_assert_eq!(
                    object_format_entry.1,
                    Some(expected_hash_name.as_str()),
                    "object-format value should match server's configured hash algorithm"
                );

                // Verify agent value starts with "gix/"
                let agent_entry = parsed_caps
                    .iter()
                    .find(|(name, _)| *name == "agent")
                    .expect("agent capability should be present");
                let agent_value = agent_entry.1
                    .expect("agent capability should have a value");
                prop_assert!(
                    agent_value.starts_with("gix/"),
                    "agent value {:?} should start with 'gix/'",
                    agent_value
                );
            }
        }
    }

    // Feature: receive-pack-v1-support, Property 2: Capability validation rejects unknown capabilities and accepts valid ones
    mod capability_validation_property {
        use super::*;
        use proptest::prelude::*;

        /// The set of valid capability names (excluding "agent" which is tested separately).
        const VALID_NON_AGENT_CAPABILITIES: &[&str] = &[
            "report-status",
            "report-status-v2",
            "side-band-64k",
            "delete-refs",
            "push-options",
            "atomic",
            "quiet",
            "no-thin",
            "object-format",
        ];

        /// Generate a capability with a name drawn from the valid set.
        /// Includes `side-band-64k` dependency handling for `report-status-v2`.
        fn arb_valid_capability() -> impl Strategy<Value = Capability> {
            proptest::sample::select(VALID_NON_AGENT_CAPABILITIES)
                .prop_map(|name| Capability {
                    name: BString::from(name),
                    value: if name == "object-format" {
                        Some(BString::from("sha1"))
                    } else {
                        None
                    },
                })
        }

        /// Generate a valid subset of capabilities that satisfies the dependency constraint
        /// (report-status-v2 requires side-band-64k).
        fn arb_valid_capability_set() -> impl Strategy<Value = Vec<Capability>> {
            proptest::collection::vec(arb_valid_capability(), 0..6)
                .prop_map(|mut caps| {
                    // Deduplicate by name
                    let mut seen = std::collections::HashSet::new();
                    caps.retain(|cap| seen.insert(cap.name.clone()));
                    // Enforce dependency: if report-status-v2 is present, ensure side-band-64k is too
                    let has_rsv2 = caps.iter().any(|c| c.name.as_bstr() == "report-status-v2".as_bytes().as_bstr());
                    let has_sb64k = caps.iter().any(|c| c.name.as_bstr() == "side-band-64k".as_bytes().as_bstr());
                    if has_rsv2 && !has_sb64k {
                        caps.push(Capability {
                            name: BString::from("side-band-64k"),
                            value: None,
                        });
                    }
                    caps
                })
        }

        /// Generate an invalid capability name that is not in SERVER_CAPABILITIES.
        fn arb_invalid_capability_name() -> impl Strategy<Value = BString> {
            "[a-z][a-z0-9\\-]{2,15}"
                .prop_filter("must not be a valid capability name", |name| {
                    !SERVER_CAPABILITIES.iter().any(|&known| known == name.as_str())
                })
                .prop_map(BString::from)
        }

        /// Generate an "agent" capability with an arbitrary value.
        fn arb_agent_capability() -> impl Strategy<Value = Capability> {
            "[a-zA-Z0-9/_\\-\\.]{1,30}".prop_map(|value| Capability {
                name: BString::from("agent"),
                value: Some(BString::from(value)),
            })
        }

        // **Validates: Requirements 2.2, 2.3, 2.6**
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            /// Valid subsets of SERVER_CAPABILITIES (with dependency constraints satisfied) pass validation.
            #[test]
            fn valid_capability_subsets_pass_validation(
                caps in arb_valid_capability_set(),
            ) {
                let result = validate_capabilities(&caps);
                prop_assert!(
                    result.is_ok(),
                    "valid capability set should pass validation, got error: {:?} for caps: {:?}",
                    result,
                    caps
                );
            }

            /// Any capability not in SERVER_CAPABILITIES (and not "agent") triggers an error.
            #[test]
            fn unknown_capability_is_rejected(
                valid_caps in arb_valid_capability_set(),
                invalid_name in arb_invalid_capability_name(),
            ) {
                let mut caps = valid_caps;
                caps.push(Capability {
                    name: invalid_name.clone(),
                    value: None,
                });
                let result = validate_capabilities(&caps);
                match result {
                    Err(Error::UnsupportedCapability { name }) => {
                        prop_assert_eq!(
                            name, invalid_name,
                            "error should identify the unsupported capability name"
                        );
                    }
                    other => {
                        prop_assert!(
                            false,
                            "expected UnsupportedCapability error for {:?}, got: {:?}",
                            invalid_name,
                            other
                        );
                    }
                }
            }

            /// The "agent" capability with any value always passes validation.
            #[test]
            fn agent_capability_always_accepted(
                valid_caps in arb_valid_capability_set(),
                agent_cap in arb_agent_capability(),
            ) {
                let mut caps = valid_caps;
                caps.push(agent_cap.clone());
                let result = validate_capabilities(&caps);
                prop_assert!(
                    result.is_ok(),
                    "agent capability with value {:?} should always be accepted, got: {:?}",
                    agent_cap.value,
                    result
                );
            }
        }
    }

    // Feature: receive-pack-v1-support, Property 7: Quiet suppresses progress but preserves errors
    mod quiet_suppression_property {
        use super::*;
        use proptest::prelude::*;

        /// Generate a random sideband message text (printable ASCII, 1–60 bytes).
        fn arb_message_text() -> impl Strategy<Value = BString> {
            "[a-zA-Z0-9 _./:]{1,60}".prop_map(BString::from)
        }

        /// Generate a random Vec of progress messages (0–10).
        fn arb_progress_messages(count: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = Vec<SidebandMessage>> {
            proptest::collection::vec(arb_message_text(), count).prop_map(|texts| {
                texts
                    .into_iter()
                    .map(|text| SidebandMessage {
                        kind: SidebandMessageKind::Progress,
                        text,
                    })
                    .collect()
            })
        }

        /// Generate a random Vec of error messages (0–10).
        fn arb_error_messages(count: std::ops::RangeInclusive<usize>) -> impl Strategy<Value = Vec<SidebandMessage>> {
            proptest::collection::vec(arb_message_text(), count).prop_map(|texts| {
                texts
                    .into_iter()
                    .map(|text| SidebandMessage {
                        kind: SidebandMessageKind::Error,
                        text,
                    })
                    .collect()
            })
        }

        /// Build a Request with report-status + side-band-64k and optionally quiet.
        fn make_request(quiet: bool) -> Request {
            let mut capabilities = vec![
                Capability {
                    name: "report-status".into(),
                    value: None,
                },
                Capability {
                    name: "side-band-64k".into(),
                    value: None,
                },
            ];
            if quiet {
                capabilities.push(Capability {
                    name: "quiet".into(),
                    value: None,
                });
            }
            Request {
                capabilities,
                updates: vec![Update {
                    old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
                    new_id: gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")
                        .expect("valid hex"),
                    ref_name: "refs/heads/main".into(),
                }],
                push_options: Vec::new(),
            }
        }

        /// Parse sideband output and count progress vs error messages.
        /// Returns (progress_count, error_count, error_payloads).
        fn count_sideband_messages(output: &[u8]) -> (usize, usize, Vec<Vec<u8>>) {
            let mut progress_count = 0usize;
            let mut error_count = 0usize;
            let mut error_payloads = Vec::new();

            let mut reader =
                StreamingPeekableIter::new(output, &[PacketLineRef::Flush], false);
            while let Some(line) = reader.read_line() {
                let line = match line {
                    Ok(Ok(l)) => l,
                    _ => break,
                };
                match line.decode_band() {
                    Ok(BandRef::Progress(data)) => {
                        progress_count += 1;
                        let _ = data;
                    }
                    Ok(BandRef::Error(data)) => {
                        error_count += 1;
                        error_payloads.push(data.to_vec());
                    }
                    Ok(BandRef::Data(_)) => {
                        // report-status data channel — skip
                    }
                    Err(_) => break,
                }
            }
            (progress_count, error_count, error_payloads)
        }

        // **Validates: Requirements 6.1, 6.2, 6.3**
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            /// With quiet: 0 progress messages in output, all error messages preserved.
            /// Without quiet: all progress + all error messages present.
            #[test]
            fn quiet_suppresses_progress_preserves_errors(
                progress_msgs in arb_progress_messages(1..=10),
                error_msgs in arb_error_messages(1..=10),
            ) {
                let n_progress = progress_msgs.len();
                let n_errors = error_msgs.len();

                // Build response with interleaved progress and error messages
                let mut sideband_messages = Vec::with_capacity(n_progress + n_errors);
                let mut progress_iter = progress_msgs.into_iter();
                let mut error_iter = error_msgs.into_iter();
                loop {
                    let p = progress_iter.next();
                    let e = error_iter.next();
                    if p.is_none() && e.is_none() {
                        break;
                    }
                    if let Some(msg) = p {
                        sideband_messages.push(msg);
                    }
                    if let Some(msg) = e {
                        sideband_messages.push(msg);
                    }
                }

                let response = Response {
                    unpack_status: UnpackStatus::Ok,
                    ref_statuses: vec![RefStatus::Ok {
                        ref_name: "refs/heads/main".into(),
                    }],
                    sideband_messages,
                };

                // With quiet: progress suppressed, errors preserved
                let request_quiet = make_request(true);
                let mut output_quiet = Vec::new();
                write_v1_response(&mut output_quiet, &request_quiet, &response)
                    .expect("write_v1_response should succeed with quiet");
                let (quiet_progress, quiet_errors, _) = count_sideband_messages(&output_quiet);
                prop_assert_eq!(
                    quiet_progress, 0,
                    "quiet mode should suppress all progress messages, but found {}",
                    quiet_progress
                );
                prop_assert_eq!(
                    quiet_errors, n_errors,
                    "quiet mode should preserve all {} error messages, but found {}",
                    n_errors, quiet_errors
                );

                // Without quiet: all messages present
                let request_noisy = make_request(false);
                let mut output_noisy = Vec::new();
                write_v1_response(&mut output_noisy, &request_noisy, &response)
                    .expect("write_v1_response should succeed without quiet");
                let (noisy_progress, noisy_errors, _) = count_sideband_messages(&output_noisy);
                prop_assert_eq!(
                    noisy_progress, n_progress,
                    "non-quiet mode should include all {} progress messages, but found {}",
                    n_progress, noisy_progress
                );
                prop_assert_eq!(
                    noisy_errors, n_errors,
                    "non-quiet mode should include all {} error messages, but found {}",
                    n_errors, noisy_errors
                );
            }
        }
    }

    // Feature: receive-pack-v1-support, Property 9: Response format matches negotiated capabilities
    mod response_format_property {
        use super::*;
        use proptest::prelude::*;

        /// Generate a random UnpackStatus.
        fn arb_unpack_status() -> impl Strategy<Value = UnpackStatus> {
            prop_oneof![
                Just(UnpackStatus::Ok),
                "[a-z ]{1,20}".prop_map(|msg| UnpackStatus::Error(BString::from(msg))),
            ]
        }

        /// Generate a random ref name.
        fn arb_ref_name() -> impl Strategy<Value = BString> {
            "[a-z][a-z0-9_]{1,10}".prop_map(|suffix| BString::from(format!("refs/heads/{suffix}")))
        }

        /// Generate a random RefStatus.
        fn arb_ref_status() -> impl Strategy<Value = RefStatus> {
            arb_ref_name().prop_flat_map(|ref_name| {
                prop_oneof![
                    Just(RefStatus::Ok {
                        ref_name: ref_name.clone(),
                    }),
                    "[a-z ]{1,20}".prop_map(move |msg| RefStatus::Rejected {
                        ref_name: ref_name.clone(),
                        message: BString::from(msg),
                    }),
                ]
            })
        }

        /// Generate a random SidebandMessage.
        fn arb_sideband_message() -> impl Strategy<Value = SidebandMessage> {
            ("[a-z0-9 ]{1,30}", prop_oneof![Just(SidebandMessageKind::Progress), Just(SidebandMessageKind::Error)])
                .prop_map(|(text, kind)| SidebandMessage {
                    kind,
                    text: BString::from(text),
                })
        }

        /// Generate a random Response.
        fn arb_response() -> impl Strategy<Value = Response> {
            (
                arb_unpack_status(),
                proptest::collection::vec(arb_ref_status(), 1..5),
                proptest::collection::vec(arb_sideband_message(), 0..4),
            )
                .prop_map(|(unpack_status, ref_statuses, sideband_messages)| Response {
                    unpack_status,
                    ref_statuses,
                    sideband_messages,
                })
        }

        /// Generate a random Request with a specific capability combination.
        /// The `report_status` flag controls whether `report-status` is included.
        /// The `sideband` flag controls whether `side-band-64k` is included.
        fn arb_request_with_caps(report_status: bool, sideband: bool) -> impl Strategy<Value = Request> {
            arb_ref_name().prop_map(move |ref_name| {
                let mut capabilities = Vec::new();
                if report_status {
                    capabilities.push(Capability {
                        name: BString::from("report-status"),
                        value: None,
                    });
                }
                if sideband {
                    capabilities.push(Capability {
                        name: BString::from("side-band-64k"),
                        value: None,
                    });
                }
                Request {
                    capabilities,
                    updates: vec![Update {
                        old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
                        new_id: gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")
                            .expect("valid hex"),
                        ref_name,
                    }],
                    push_options: Vec::new(),
                }
            })
        }

        /// Check if the raw output is flush-only (just `0000`).
        fn is_flush_only(output: &[u8]) -> bool {
            output == b"0000"
        }

        /// Check if the raw output contains sideband framing by looking for band packet structure.
        /// Sideband packets have the format: XXXX\x01... (data channel) or XXXX\x02... (progress)
        /// or XXXX\x03... (error). The band byte is the first payload byte after the 4-hex length.
        fn contains_sideband_framing(output: &[u8]) -> bool {
            // Parse packet lines from output and check for band bytes
            let mut pos = 0;
            while pos + 4 <= output.len() {
                let hex = &output[pos..pos + 4];
                if hex == b"0000" {
                    // flush packet
                    break;
                }
                let len_str = std::str::from_utf8(hex).unwrap_or("");
                let len = u16::from_str_radix(len_str, 16).unwrap_or(0) as usize;
                if len < 5 || pos + len > output.len() {
                    break;
                }
                // The first byte after the 4-byte hex prefix is the band indicator
                let band_byte = output[pos + 4];
                if band_byte == 1 || band_byte == 2 || band_byte == 3 {
                    return true;
                }
                pos += len;
            }
            false
        }

        /// Check if the output contains report-status lines (unpack status + ref status).
        /// For sideband output, we need to extract the data channel content first.
        /// For plain output, we look for pkt-line encoded "unpack ok" or "unpack <error>".
        fn contains_report_status(output: &[u8]) -> bool {
            // For sideband: extract data channel and check for report-status content
            if contains_sideband_framing(output) {
                let mut data_payload = Vec::new();
                let mut pos = 0;
                while pos + 4 <= output.len() {
                    let hex = &output[pos..pos + 4];
                    if hex == b"0000" {
                        break;
                    }
                    let len_str = std::str::from_utf8(hex).unwrap_or("");
                    let len = u16::from_str_radix(len_str, 16).unwrap_or(0) as usize;
                    if len < 5 || pos + len > output.len() {
                        break;
                    }
                    let band_byte = output[pos + 4];
                    if band_byte == 1 {
                        // Data channel — accumulate payload
                        data_payload.extend_from_slice(&output[pos + 5..pos + len]);
                    }
                    pos += len;
                }
                // The data payload is itself pkt-line encoded report-status
                return data_payload.windows(9).any(|w| w == b"unpack ok")
                    || data_payload.windows(7).any(|w| w == b"unpack ");
            }
            // Plain (non-sideband): look for "unpack" in pkt-line text
            output.windows(9).any(|w| w == b"unpack ok") || output.windows(7).any(|w| w == b"unpack ")
        }

        // **Validates: Requirements 9.1, 9.2, 9.3, 9.4**
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            /// When report-status is negotiated with side-band-64k, output contains
            /// sideband-framed report-status data.
            #[test]
            fn report_status_with_sideband_produces_framed_output(
                request in arb_request_with_caps(true, true),
                response in arb_response(),
            ) {
                let mut output = Vec::new();
                write_v1_response(&mut output, &request, &response)
                    .expect("write_v1_response should succeed");

                prop_assert!(
                    !is_flush_only(&output),
                    "report-status + side-band-64k should produce more than just a flush"
                );
                prop_assert!(
                    contains_sideband_framing(&output),
                    "output should use sideband framing when side-band-64k is negotiated"
                );
                prop_assert!(
                    contains_report_status(&output),
                    "output should contain report-status lines when report-status is negotiated"
                );
            }

            /// When report-status is negotiated without side-band-64k, output contains
            /// plain report-status lines (no sideband framing).
            #[test]
            fn report_status_without_sideband_produces_plain_output(
                request in arb_request_with_caps(true, false),
                response in arb_response(),
            ) {
                let mut output = Vec::new();
                write_v1_response(&mut output, &request, &response)
                    .expect("write_v1_response should succeed");

                prop_assert!(
                    !is_flush_only(&output),
                    "report-status without sideband should produce more than just a flush"
                );
                prop_assert!(
                    !contains_sideband_framing(&output),
                    "output should NOT use sideband framing when side-band-64k is not negotiated"
                );
                prop_assert!(
                    contains_report_status(&output),
                    "output should contain report-status lines when report-status is negotiated"
                );
            }

            /// When neither report-status nor report-status-v2 is negotiated, output is flush-only
            /// regardless of whether side-band-64k is present.
            #[test]
            fn no_report_status_produces_flush_only(
                sideband in any::<bool>(),
                response in arb_response(),
                ref_name in arb_ref_name(),
            ) {
                let mut capabilities = Vec::new();
                if sideband {
                    capabilities.push(Capability {
                        name: BString::from("side-band-64k"),
                        value: None,
                    });
                }
                let request = Request {
                    capabilities,
                    updates: vec![Update {
                        old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
                        new_id: gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")
                            .expect("valid hex"),
                        ref_name,
                    }],
                    push_options: Vec::new(),
                };
                let mut output = Vec::new();
                write_v1_response(&mut output, &request, &response)
                    .expect("write_v1_response should succeed");

                prop_assert!(
                    is_flush_only(&output),
                    "without report-status negotiated, output should be flush-only, got {} bytes: {:?}",
                    output.len(),
                    String::from_utf8_lossy(&output[..output.len().min(100)])
                );
            }

            /// Side-band-64k without report-status produces flush-only (Req 9.4).
            #[test]
            fn sideband_without_report_status_produces_flush_only(
                request in arb_request_with_caps(false, true),
                response in arb_response(),
            ) {
                let mut output = Vec::new();
                write_v1_response(&mut output, &request, &response)
                    .expect("write_v1_response should succeed");

                prop_assert!(
                    is_flush_only(&output),
                    "side-band-64k without report-status should produce flush-only (Req 9.4), got {} bytes: {:?}",
                    output.len(),
                    String::from_utf8_lossy(&output[..output.len().min(100)])
                );
            }
        }
    }

    // Feature: receive-pack-v1-support, Property 3: Object-format validation rejects mismatches and prevents delegate invocation
    mod object_format_validation_property {
        use super::*;
        use proptest::prelude::*;

        /// Generate a ServerConfig (sha1 only without sha256 feature).
        fn arb_server_config() -> impl Strategy<Value = ServerConfig> {
            Just(ServerConfig {
                object_hash: gix_hash::Kind::Sha1,
            })
        }

        /// Generate an invalid algorithm name that cannot be parsed by gix_hash::Kind.
        fn arb_invalid_format() -> impl Strategy<Value = String> {
            "[a-z][a-z0-9]{2,12}".prop_filter("must not be a parseable hash kind", |s| {
                s.parse::<gix_hash::Kind>().is_err()
            })
        }

        // **Validates: Requirements 3.1, 3.2, 3.4**
        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            /// Matching (client_algo, server_algo) pairs pass validation.
            #[test]
            fn matching_object_format_passes(
                config in arb_server_config(),
            ) {
                // Client sends the same algorithm as the server
                let server_format = config.object_hash.to_string();
                let caps = vec![Capability {
                    name: BString::from("object-format"),
                    value: Some(BString::from(server_format.as_str())),
                }];
                let result = validate_object_format(&caps, &config);
                prop_assert!(
                    result.is_ok(),
                    "matching object-format {:?} should pass validation, got: {:?}",
                    server_format,
                    result
                );
            }

            /// When the client sends a value that parses to a different Kind than the server's,
            /// the result is UnsupportedObjectFormat. When the sha256 feature is not enabled,
            /// "sha256" is not parseable and produces InvalidObjectFormat instead — which is
            /// still a rejection, satisfying Requirement 3.1.
            #[test]
            fn non_matching_object_format_rejected(
                config in arb_server_config(),
            ) {
                // "sha256" is not the server's format (sha1); it should be rejected.
                // The specific error variant depends on whether sha256 feature is compiled.
                let caps = vec![Capability {
                    name: BString::from("object-format"),
                    value: Some(BString::from("sha256")),
                }];
                let result = validate_object_format(&caps, &config);
                prop_assert!(
                    result.is_err(),
                    "non-matching object-format 'sha256' with sha1 server should be rejected, got: {:?}",
                    result
                );
                // Verify it's one of the two valid rejection error types
                match &result {
                    Err(Error::UnsupportedObjectFormat { .. }) | Err(Error::InvalidObjectFormat { .. }) => {}
                    other => {
                        prop_assert!(
                            false,
                            "expected UnsupportedObjectFormat or InvalidObjectFormat, got: {:?}",
                            other
                        );
                    }
                }
            }

            /// Absent object-format capability defaults to sha1 and validates against the server.
            #[test]
            fn absent_object_format_defaults_to_sha1(
                config in arb_server_config(),
            ) {
                // No object-format capability present — default is sha1
                let caps: Vec<Capability> = vec![
                    Capability {
                        name: BString::from("report-status"),
                        value: None,
                    },
                ];
                let result = validate_object_format(&caps, &config);
                // Server uses sha1, default is sha1, so it should pass
                if config.object_hash == gix_hash::Kind::Sha1 {
                    prop_assert!(
                        result.is_ok(),
                        "absent object-format with sha1 server should default to sha1 and pass, got: {:?}",
                        result
                    );
                } else {
                    prop_assert!(
                        matches!(result, Err(Error::UnsupportedObjectFormat { .. })),
                        "absent object-format with non-sha1 server should be rejected, got: {:?}",
                        result
                    );
                }
            }

            /// Invalid (unrecognized) algorithm names return InvalidObjectFormat error.
            #[test]
            fn invalid_algorithm_name_returns_error(
                invalid_name in arb_invalid_format(),
                config in arb_server_config(),
            ) {
                let caps = vec![Capability {
                    name: BString::from("object-format"),
                    value: Some(BString::from(invalid_name.as_str())),
                }];
                let result = validate_object_format(&caps, &config);
                match result {
                    Err(Error::InvalidObjectFormat { value }) => {
                        prop_assert_eq!(
                            value.to_string(),
                            invalid_name,
                            "error should contain the invalid format name"
                        );
                    }
                    other => {
                        prop_assert!(
                            false,
                            "expected InvalidObjectFormat error for {:?}, got: {:?}",
                            invalid_name,
                            other
                        );
                    }
                }
            }

            /// Non-object-format capabilities do not affect object-format validation.
            #[test]
            fn non_object_format_caps_do_not_interfere(
                cap_name in "[a-z][a-z0-9\\-]{2,15}".prop_filter(
                    "must not be object-format",
                    |s| s != "object-format"
                ),
                config in arb_server_config(),
            ) {
                let caps = vec![Capability {
                    name: BString::from(cap_name.as_str()),
                    value: None,
                }];
                // With only non-object-format caps, the default (sha1) is used.
                // For a sha1 server, this should pass.
                let result = validate_object_format(&caps, &config);
                if config.object_hash == gix_hash::Kind::Sha1 {
                    prop_assert!(
                        result.is_ok(),
                        "non-object-format cap {:?} should not interfere with validation (sha1 server), got: {:?}",
                        cap_name,
                        result
                    );
                }
            }
        }
    }
}
