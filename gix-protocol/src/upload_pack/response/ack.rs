//! Acknowledgments section writer for protocol V2 fetch responses.
//!
//! Handles the `acknowledgments` section which reports common objects
//! and readiness state back to the client during negotiation.

use std::io::Write as _;

use bstr::BString;
use gix_transport::packetline::blocking_io::Writer;

use crate::fetch::response::Acknowledgement;

use super::{Error, SectionWriter};

/// Format a single acknowledgement entry as a protocol line.
pub(crate) fn format_acknowledgement_line(ack: Acknowledgement) -> BString {
    match ack {
        Acknowledgement::Common(id) => format!("ACK {id} common").into(),
        Acknowledgement::Ready => "ready".into(),
        Acknowledgement::Nak => "NAK".into(),
    }
}

/// Acknowledgments section writer.
///
/// Writes the `acknowledgments` section header followed by `ACK <id> common` lines
/// and optionally a `ready` line when negotiation is complete.
pub(crate) struct AckSection;

impl SectionWriter for AckSection {
    type Input = [Acknowledgement];

    fn has_content(input: &Self::Input) -> bool {
        !input.is_empty()
    }

    fn write(&self, output: &mut dyn std::io::Write, acks: &Self::Input) -> Result<(), Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"acknowledgments")?;
        for ack in acks {
            writer.write_all(format_acknowledgement_line(*ack).as_ref())?;
        }
        Ok(())
    }
}
