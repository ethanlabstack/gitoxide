//! Composable section writers for protocol V2 upload-pack responses.
//!
//! Each section (acknowledgments, shallow-info, wanted-refs, packfile) implements
//! the [`SectionWriter`] trait, allowing the response pipeline to compose sections
//! dynamically, skip empty ones, and share logic between blocking and async paths.

pub(crate) mod ack;
pub(crate) mod packfile;
pub(crate) mod shallow;
pub(crate) mod wanted_refs;

use std::io::{self, Write as _};

use bstr::BString;
use bstr::ByteVec;
use gix_transport::packetline::blocking_io::{Writer, encode};

use crate::fetch::response::{Acknowledgement, ShallowUpdate, WantedRef};
use crate::handshake::Ref;

use super::{Capability, Error, FetchOutput, LsRefs};

pub(crate) use ack::AckSection;
pub(crate) use shallow::ShallowSection;
pub(crate) use wanted_refs::WantedRefsSection;
pub(crate) use packfile::PackfileSection;

/// A section writer that can emit its content into a pkt-line stream.
///
/// Implementations handle their own section header and content.
/// The caller is responsible for the terminating delimiter or flush
/// based on protocol rules.
pub(crate) trait SectionWriter {
    /// The data this section needs to produce output.
    type Input: ?Sized;

    /// Returns true if this section has content to write.
    /// When false, the pipeline skips this section entirely (no header, no delimiter).
    fn has_content(input: &Self::Input) -> bool;

    /// Write the section content (header + entries) to the output.
    /// Does NOT write the terminating delimiter/flush — that's the pipeline's job.
    fn write(&self, output: &mut dyn io::Write, input: &Self::Input) -> Result<(), Error>;
}

/// Write a protocol V2 capability advertisement, including the `version 2` line.
pub fn write_v2_capability_advertisement(mut output: impl io::Write, capabilities: &[Capability]) -> Result<(), Error> {
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

/// Write a `ls-refs` response body according to `request`.
///
/// Returns the number of refs written.
pub fn write_ls_refs_response(mut output: impl io::Write, request: &LsRefs, refs: &[Ref]) -> Result<usize, Error> {
    let mut writer = Writer::new(&mut output);
    writer.enable_text_mode();
    let mut refs_sent = 0usize;

    for line in refs
        .iter()
        .filter(|reference| matches_ref_prefixes(reference, &request.ref_prefixes))
        .filter_map(|reference| format_ls_ref_line(reference, request))
    {
        writer.write_all(line.as_ref())?;
        refs_sent += 1;
    }
    encode::flush_to_write(writer.inner_mut())?;
    Ok(refs_sent)
}

/// Write the non-pack metadata sections (acknowledgments, shallow-info, wanted-refs) of a fetch response.
///
/// This is shared between the blocking [`write_fetch_response`] and the async variant
/// in [`super::async_io`] to avoid duplicating the section-framing logic.
///
/// When `has_pack_data` is true, each section is terminated with a delimiter packet (`0001`)
/// to signal that another section follows. When false, the last section omits the trailing
/// delimiter — the caller's final flush packet (`0000`) terminates the response instead.
/// This matches the V2 protocol framing expected by clients.
pub(crate) fn write_fetch_metadata_sections(
    output: impl io::Write,
    acknowledgements: &[Acknowledgement],
    shallow_updates: &[ShallowUpdate],
    wanted_refs: &[WantedRef],
    has_pack_data: bool,
) -> Result<(), Error> {
    // Use section writers internally for the actual content.
    // We still manage delimiter/flush framing at this level since
    // it depends on cross-section awareness (which section is last, etc.).
    let mut output = output;

    let sections: [(&[u8], bool); 3] = [
        (b"acknowledgments" as &[u8], !acknowledgements.is_empty()),
        (b"shallow-info", !shallow_updates.is_empty()),
        (b"wanted-refs", !wanted_refs.is_empty()),
    ];
    let last_active_idx = sections.iter().rposition(|(_, active)| *active);

    if AckSection::has_content(acknowledgements) {
        AckSection.write(&mut output, acknowledgements)?;
        let is_last = last_active_idx == Some(0);
        if has_pack_data || !is_last {
            encode::delim_to_write(&mut output)?;
        }
    }

    if ShallowSection::has_content(shallow_updates) {
        ShallowSection.write(&mut output, shallow_updates)?;
        let is_last = last_active_idx == Some(1);
        if has_pack_data || !is_last {
            encode::delim_to_write(&mut output)?;
        }
    }

    if WantedRefsSection::has_content(wanted_refs) {
        WantedRefsSection.write(&mut output, wanted_refs)?;
        let is_last = last_active_idx == Some(2);
        if has_pack_data || !is_last {
            encode::delim_to_write(&mut output)?;
        }
    }

    Ok(())
}

/// Write a V2 `fetch` response, including optional sections and optional pack stream.
///
/// Returns the number of raw pack bytes sent on sideband channel `1`.
pub fn write_fetch_response(mut output: impl io::Write, response: &mut FetchOutput) -> Result<u64, Error> {
    write_fetch_metadata_sections(
        &mut output,
        &response.acknowledgements,
        &response.shallow_updates,
        &response.wanted_refs,
        response.pack_data.is_some(),
    )?;

    let pack_bytes_sent = match response.pack_data.as_mut() {
        Some(pack_data) => PackfileSection.write(&mut output, &mut **pack_data)?,
        None => 0,
    };

    encode::flush_to_write(&mut output)?;
    Ok(pack_bytes_sent)
}

/// Write a V1 ref advertisement.
///
/// Format (per git protocol V1 spec):
/// ```text
/// <hex-oid> <refname>\0<capabilities>\n   # first line has capabilities after NUL byte
/// <hex-oid> <refname>\n                    # subsequent lines
/// 0000                                     # flush terminates
/// ```
///
/// - `Ref::Direct` emits `<object> <full_ref_name>`
/// - `Ref::Peeled` emits `<tag> <full_ref_name>` followed by `<object> <full_ref_name>^{}`
/// - `Ref::Symbolic` emits `<object> <full_ref_name>` (symref targets are communicated via capabilities in V1)
/// - `Ref::Unborn` is skipped (V1 doesn't support unborn refs)
///
/// Returns the number of ref lines written (counting peeled `^{}` lines separately).
pub fn write_v1_ref_advertisement(
    mut output: impl io::Write,
    refs: &[Ref],
    capabilities: &str,
) -> Result<usize, Error> {
    let mut writer = Writer::new(&mut output);
    writer.enable_text_mode();
    let mut lines_written = 0usize;
    let mut is_first = true;

    for reference in refs {
        match reference {
            Ref::Direct { full_ref_name, object } => {
                let mut line = BString::default();
                line.push_str(object.to_string());
                line.push_byte(b' ');
                line.push_str(full_ref_name);
                if is_first {
                    line.push_byte(0);
                    line.push_str(capabilities);
                    is_first = false;
                }
                writer.write_all(line.as_ref())?;
                lines_written += 1;
            }
            Ref::Peeled {
                full_ref_name,
                tag,
                object,
            } => {
                let mut line = BString::default();
                line.push_str(tag.to_string());
                line.push_byte(b' ');
                line.push_str(full_ref_name);
                if is_first {
                    line.push_byte(0);
                    line.push_str(capabilities);
                    is_first = false;
                }
                writer.write_all(line.as_ref())?;
                lines_written += 1;

                // Peeled tags get an additional ^{} line showing the target object.
                let mut peeled_line = BString::default();
                peeled_line.push_str(object.to_string());
                peeled_line.push_byte(b' ');
                peeled_line.push_str(full_ref_name);
                peeled_line.push_str("^{}");
                writer.write_all(peeled_line.as_ref())?;
                lines_written += 1;
            }
            Ref::Symbolic {
                full_ref_name,
                object,
                ..
            } => {
                let mut line = BString::default();
                line.push_str(object.to_string());
                line.push_byte(b' ');
                line.push_str(full_ref_name);
                if is_first {
                    line.push_byte(0);
                    line.push_str(capabilities);
                    is_first = false;
                }
                writer.write_all(line.as_ref())?;
                lines_written += 1;
            }
            Ref::Unborn { .. } => {
                // V1 doesn't support unborn refs, skip.
            }
        }
    }

    encode::flush_to_write(writer.inner_mut())?;
    Ok(lines_written)
}

/// Check whether a ref matches any of the given prefix filters.
///
/// If `prefixes` is empty, all refs match (no filter applied).
pub(crate) fn matches_ref_prefixes(reference: &Ref, prefixes: &[BString]) -> bool {
    if prefixes.is_empty() {
        return true;
    }
    let full_ref_name = match reference {
        Ref::Peeled { full_ref_name, .. }
        | Ref::Direct { full_ref_name, .. }
        | Ref::Symbolic { full_ref_name, .. }
        | Ref::Unborn { full_ref_name, .. } => full_ref_name,
    };
    prefixes.iter().any(|prefix| {
        let full_ref_name: &[u8] = full_ref_name.as_ref();
        let prefix: &[u8] = prefix.as_ref();
        full_ref_name.starts_with(prefix)
    })
}

/// Format a single ref as a `ls-refs` response line.
///
/// Returns `None` if the ref should be excluded (e.g., unborn ref when `request.unborn` is false).
pub(crate) fn format_ls_ref_line(reference: &Ref, request: &LsRefs) -> Option<BString> {
    let mut line = BString::default();
    match reference {
        Ref::Direct { full_ref_name, object } => {
            line.push_str(object.to_string());
            line.push_byte(b' ');
            line.push_str(full_ref_name);
        }
        Ref::Peeled {
            full_ref_name,
            tag,
            object,
        } => {
            line.push_str(tag.to_string());
            line.push_byte(b' ');
            line.push_str(full_ref_name);
            if request.peel {
                line.push_str(" peeled:");
                line.push_str(object.to_string());
            }
        }
        Ref::Symbolic {
            full_ref_name,
            target,
            tag,
            object,
        } => {
            let advertised_id = tag.as_ref().unwrap_or(object);
            line.push_str(advertised_id.to_string());
            line.push_byte(b' ');
            line.push_str(full_ref_name);

            if request.symrefs {
                line.push_str(" symref-target:");
                line.push_str(target);
            }
            if request.peel {
                if let Some(tag) = tag {
                    line.push_str(" peeled:");
                    line.push_str(object.to_string());
                    if tag == object {
                        return Some(line);
                    }
                }
            }
        }
        Ref::Unborn { full_ref_name, target } => {
            if !request.unborn {
                return None;
            }
            line.push_str("unborn ");
            line.push_str(full_ref_name);
            line.push_str(" symref-target:");
            line.push_str(target);
        }
    }
    Some(line)
}
