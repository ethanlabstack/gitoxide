//! Wanted-refs section writer for protocol V2 fetch responses.
//!
//! Handles the `wanted-refs` section which reports resolved `want-ref`
//! entries back to the client with their object IDs.

use std::io::Write as _;

use bstr::BString;
use bstr::ByteVec;
use gix_transport::packetline::blocking_io::Writer;

use crate::fetch::response::WantedRef;

use super::{Error, SectionWriter};

/// Format a single wanted-ref entry as a protocol line.
pub(crate) fn format_wanted_ref_line(wanted: &WantedRef) -> BString {
    let mut line = BString::default();
    line.push_str(wanted.id.to_string());
    line.push_byte(b' ');
    let path: &[u8] = wanted.path.as_ref();
    line.push_str(path);
    line
}

/// Wanted-refs section writer.
///
/// Writes the `wanted-refs` section header followed by `<oid> <refname>` lines
/// for each ref that was requested via `want-ref` and successfully resolved.
pub(crate) struct WantedRefsSection;

impl SectionWriter for WantedRefsSection {
    type Input = [WantedRef];

    fn has_content(input: &Self::Input) -> bool {
        !input.is_empty()
    }

    fn write(&self, output: &mut dyn std::io::Write, wanted_refs: &Self::Input) -> Result<(), Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"wanted-refs")?;
        for wanted in wanted_refs {
            writer.write_all(format_wanted_ref_line(wanted).as_ref())?;
        }
        Ok(())
    }
}
