//! Shallow-info section writer for protocol V2 fetch responses.
//!
//! Handles the `shallow-info` section which reports shallow boundary
//! updates (shallow/unshallow) to the client.

use std::io::Write as _;

use bstr::BString;
use gix_transport::packetline::blocking_io::Writer;

use crate::fetch::response::ShallowUpdate;

use super::{Error, SectionWriter};

/// Format a single shallow update entry as a protocol line.
pub(crate) fn format_shallow_update_line(update: &ShallowUpdate) -> BString {
    match update {
        ShallowUpdate::Shallow(id) => format!("shallow {id}").into(),
        ShallowUpdate::Unshallow(id) => format!("unshallow {id}").into(),
    }
}

/// Shallow-info section writer.
///
/// Writes the `shallow-info` section header followed by `shallow <id>`
/// and `unshallow <id>` lines for boundary updates.
pub(crate) struct ShallowSection;

impl SectionWriter for ShallowSection {
    type Input = [ShallowUpdate];

    fn has_content(input: &Self::Input) -> bool {
        !input.is_empty()
    }

    fn write(&self, output: &mut dyn std::io::Write, updates: &Self::Input) -> Result<(), Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"shallow-info")?;
        for update in updates {
            writer.write_all(format_shallow_update_line(update).as_ref())?;
        }
        Ok(())
    }
}
