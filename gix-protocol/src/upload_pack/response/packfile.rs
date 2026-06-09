//! Packfile sideband section writer for protocol V2 fetch responses.
//!
//! Handles the `packfile` section which streams pack data as sideband
//! channel 1 packets to the client.

use std::io::{Read, Write as _};

use gix_transport::packetline::blocking_io::{Writer, encode};
use gix_transport::packetline::Channel;

use super::Error;

/// Packfile sideband section writer.
///
/// Writes the `packfile` section header followed by pack data encoded
/// as sideband channel 1 packets, each bounded by `MAX_SIDEBAND_DATA_BYTES`.
pub(crate) struct PackfileSection;

impl PackfileSection {
    /// Write pack data as sideband channel 1 packets.
    ///
    /// Buffer size is bounded by [`super::super::MAX_SIDEBAND_DATA_BYTES`] (65515).
    /// Returns total raw pack bytes written.
    pub fn write(
        &self,
        output: &mut dyn std::io::Write,
        mut pack_data: impl Read,
    ) -> Result<u64, Error> {
        let mut writer = Writer::new(output);
        writer.enable_text_mode();
        writer.write_all(b"packfile")?;

        let mut buffer = [0u8; super::super::MAX_SIDEBAND_DATA_BYTES];
        let mut total = 0u64;
        loop {
            let n = pack_data.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            encode::band_to_write(Channel::Data, &buffer[..n], writer.inner_mut())?;
        }
        Ok(total)
    }
}
