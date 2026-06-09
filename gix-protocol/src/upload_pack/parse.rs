//! Protocol V2 upload-pack request parsing.
//!
//! This module extracts and validates the command, features, and arguments
//! from a pkt-line encoded V2 request stream.

use std::io;

use bstr::{BStr, BString, ByteSlice};
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
use gix_transport::packetline::blocking_io::StreamingPeekableIter;
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
use gix_transport::packetline::PacketLineRef;

use super::{Command, Error, Feature, Fetch, LsRefs, Request, ServerConfig};

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
const HEADER_DELIMITERS: &[PacketLineRef<'static>] = &[PacketLineRef::Delimiter, PacketLineRef::Flush];
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
const ARGUMENT_DELIMITERS: &[PacketLineRef<'static>] = &[PacketLineRef::Flush];

/// Known `object-format` values — recognized regardless of compile-time hash features.
/// This ensures a sha256 request against a sha1-only build reports "unsupported" not "invalid".
#[cfg(any(feature = "blocking-server", feature = "async-server"))]
const KNOWN_OBJECT_FORMATS: &[&str] = &["sha1", "sha256"];

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
/// Parse a single protocol V2 upload-pack request from `input`.
///
/// The `config` parameter controls capability validation — the client's `object-format`
/// feature (if present) is checked against `config.object_hash`, and OID hex lengths in
/// fetch arguments are enforced to match the configured hash kind.
pub fn parse_v2_request(input: impl io::Read, config: &ServerConfig) -> Result<Request, Error> {
    let mut reader = StreamingPeekableIter::new(input, HEADER_DELIMITERS, false);
    let header_lines = read_text_lines(&mut reader)?;
    let has_argument_section = reader.stopped_at() == Some(PacketLineRef::Delimiter);
    let argument_lines = if has_argument_section {
        reader.reset_with(ARGUMENT_DELIMITERS);
        read_text_lines(&mut reader)?
    } else {
        Vec::new()
    };

    let (command, features) = parse_header_lines(header_lines)?;
    validate_object_format(&features, config)?;
    let command: &[u8] = command.as_ref();
    let command = match command {
        b"ls-refs" => Command::LsRefs(parse_ls_refs_arguments(argument_lines)),
        b"fetch" => Command::Fetch(parse_fetch_arguments(argument_lines, config.object_hash)?),
        other => {
            return Err(Error::UnsupportedCommand { command: other.into() });
        }
    };
    Ok(Request { features, command })
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_header_lines(lines: Vec<BString>) -> Result<(BString, Vec<Feature>), Error> {
    let mut command = None::<BString>;
    let mut features = Vec::new();

    for line in lines {
        let bytes: &[u8] = line.as_ref();
        if let Some(command_name) = bytes.strip_prefix(b"command=") {
            if command.is_some() || command_name.is_empty() {
                return Err(Error::MalformedHeaderLine { line });
            }
            command = Some(command_name.into());
            continue;
        }
        features.push(parse_feature_line(line.as_bstr())?);
    }

    let command = command.ok_or(Error::MissingCommand)?;
    Ok((command, features))
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_feature_line(line: &BStr) -> Result<Feature, Error> {
    if let Some((name, value)) = split_once(line, b'=') {
        if name.is_empty() {
            return Err(Error::MalformedHeaderLine { line: line.to_owned() });
        }
        return Ok(Feature {
            name: name.to_owned(),
            value: Some(value.to_owned()),
        });
    }
    if line.is_empty() {
        return Err(Error::MalformedHeaderLine { line: line.to_owned() });
    }
    Ok(Feature {
        name: line.to_owned(),
        value: None,
    })
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_ls_refs_arguments(arguments: Vec<BString>) -> LsRefs {
    let mut parsed = LsRefs::default();
    for line in arguments {
        let bytes: &[u8] = line.as_ref();
        match bytes {
            b"symrefs" => parsed.symrefs = true,
            b"peel" => parsed.peel = true,
            b"unborn" => parsed.unborn = true,
            _ => {
                if let Some(prefix) = bytes.strip_prefix(b"ref-prefix ") {
                    if !prefix.is_empty() {
                        parsed.ref_prefixes.push(prefix.into());
                    } else {
                        parsed.extra_arguments.push(line);
                    }
                } else {
                    parsed.extra_arguments.push(line);
                }
            }
        }
    }
    parsed
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_fetch_arguments(arguments: Vec<BString>, object_hash: gix_hash::Kind) -> Result<Fetch, Error> {
    let mut parsed = Fetch::default();
    for line in arguments {
        let bytes: &[u8] = line.as_ref();
        match bytes {
            b"thin-pack" => parsed.thin_pack = true,
            b"no-progress" => parsed.no_progress = true,
            b"ofs-delta" => parsed.ofs_delta = true,
            b"include-tag" => parsed.include_tag = true,
            b"sideband-all" => parsed.sideband_all = true,
            b"deepen-relative" => parsed.deepen_relative = true,
            b"wait-for-done" => parsed.wait_for_done = true,
            b"done" => parsed.done = true,
            _ => {
                if bytes.starts_with(b"want ") {
                    parsed
                        .wants
                        .push(parse_object_id(line.as_bstr(), b"want ", "fetch", object_hash)?);
                } else if bytes.starts_with(b"have ") {
                    parsed
                        .haves
                        .push(parse_object_id(line.as_bstr(), b"have ", "fetch", object_hash)?);
                } else if bytes.starts_with(b"shallow ") {
                    parsed
                        .shallow
                        .push(parse_object_id(line.as_bstr(), b"shallow ", "fetch", object_hash)?);
                } else if let Some(value) = bytes.strip_prefix(b"deepen ") {
                    parsed.deepen = Some(parse_u32_argument(line.as_bstr(), value, "fetch", false)?);
                } else if let Some(value) = bytes.strip_prefix(b"deepen-since ") {
                    parsed.deepen_since = Some(parse_i64_argument(line.as_bstr(), value, "fetch")?);
                } else if let Some(value) = bytes.strip_prefix(b"deepen-not ") {
                    if value.is_empty() {
                        return Err(Error::MalformedArgument { command: "fetch", line });
                    }
                    parsed.deepen_not.push(value.into());
                } else if let Some(value) = bytes.strip_prefix(b"filter ") {
                    if value.is_empty() {
                        return Err(Error::MalformedArgument { command: "fetch", line });
                    }
                    parsed.filters.push(value.into());
                } else if let Some(value) = bytes.strip_prefix(b"want-ref ") {
                    if value.is_empty() {
                        return Err(Error::MalformedArgument { command: "fetch", line });
                    }
                    parsed.want_refs.push(value.into());
                } else if let Some(value) = bytes.strip_prefix(b"packfile-uris ") {
                    parsed
                        .packfile_uris
                        .extend(parse_comma_separated_values(line.as_bstr(), value, "fetch")?);
                } else {
                    parsed.extra_arguments.push(line);
                }
            }
        }
    }
    Ok(parsed)
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn validate_object_format(features: &[Feature], config: &ServerConfig) -> Result<(), Error> {
    for feature in features {
        if feature.name == "object-format" {
            let value: &BStr = feature
                .value
                .as_ref()
                .map_or(b"".as_bstr(), |v| v.as_bstr());
            let value_str = match value.to_str() {
                Ok(s) => s,
                Err(_) => return Err(Error::InvalidObjectFormat { value: value.to_owned() }),
            };
            // Check if the value is a recognized hash name (independent of compile-time features)
            if !KNOWN_OBJECT_FORMATS.contains(&value_str) {
                return Err(Error::InvalidObjectFormat { value: value.to_owned() });
            }
            // Check if it matches the server's configured hash
            if value_str == config.object_hash.to_string().as_str() {
                return Ok(());
            }
            return Err(Error::UnsupportedObjectFormat {
                requested: value.to_owned(),
                supported: config.object_hash.to_string().into(),
            });
        }
    }
    // No object-format feature: assume server's hash — OK
    Ok(())
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_object_id(
    line: &BStr,
    prefix: &[u8],
    command: &'static str,
    object_hash: gix_hash::Kind,
) -> Result<gix_hash::ObjectId, Error> {
    let hex = line
        .as_bytes()
        .strip_prefix(prefix)
        .ok_or_else(|| Error::MalformedArgument {
            command,
            line: line.to_owned(),
        })?;

    let expected_len = object_hash.len_in_hex();
    if hex.len() != expected_len {
        return Err(Error::ObjectIdLengthMismatch {
            actual: hex.len(),
            expected: expected_len,
            hash_kind: object_hash,
        });
    }

    gix_hash::ObjectId::from_hex(hex).map_err(|source| Error::InvalidObjectId {
        line: line.to_owned(),
        source,
    })
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_u32_argument(line: &BStr, value: &[u8], command: &'static str, allow_zero: bool) -> Result<u32, Error> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|parsed| allow_zero || *parsed != 0)
        .ok_or_else(|| Error::MalformedArgument {
            command,
            line: line.to_owned(),
        })
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_i64_argument(line: &BStr, value: &[u8], command: &'static str) -> Result<i64, Error> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| Error::MalformedArgument {
            command,
            line: line.to_owned(),
        })
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn parse_comma_separated_values(line: &BStr, value: &[u8], command: &'static str) -> Result<Vec<BString>, Error> {
    if value.is_empty() {
        return Err(Error::MalformedArgument {
            command,
            line: line.to_owned(),
        });
    }

    let values = value
        .split(|byte| *byte == b',')
        .map(|value| value.as_bstr().to_owned())
        .collect::<Vec<_>>();
    if values.iter().any(|value| value.is_empty()) {
        return Err(Error::MalformedArgument {
            command,
            line: line.to_owned(),
        });
    }
    Ok(values)
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn read_text_lines<T: io::Read>(reader: &mut StreamingPeekableIter<T>) -> Result<Vec<BString>, Error> {
    let mut out = Vec::new();
    while let Some(line) = reader.read_line() {
        let line = line?;
        let line = line?;
        let text = line.as_text().ok_or_else(|| Error::NonTextPacketLine {
            line_type: packet_line_kind(&line),
        })?;
        out.push(text.as_bstr().to_owned());
    }
    Ok(out)
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn packet_line_kind(line: &PacketLineRef<'_>) -> &'static str {
    match line {
        PacketLineRef::Data(_) => "data",
        PacketLineRef::Flush => "flush",
        PacketLineRef::Delimiter => "delimiter",
        PacketLineRef::ResponseEnd => "response-end",
    }
}

#[cfg(any(feature = "blocking-server", feature = "async-server"))]
pub(crate) fn split_once(line: &BStr, separator: u8) -> Option<(&BStr, &BStr)> {
    let idx = line.find_byte(separator)?;
    Some((line[..idx].as_bstr(), line[idx + 1..].as_bstr()))
}
