//! Receive-pack protocol-contract tests backed by real `git push` client captures.
//!
//! On the currently tested git client (`git/2.39.5`), push requests are framed as V1 command
//! sections (including optional push-options) even when protocol v2 is requested globally.
use std::io::{BufRead as _, BufReader, Cursor, Read as _};

use bstr::ByteSlice;
use gix_protocol::receive_pack::{self, RefStatus, Request, Response, UnpackStatus};
use gix_transport::packetline::{BandRef, PacketLineRef, blocking_io::StreamingPeekableIter};

#[derive(Default)]
struct RecordingDelegate {
    response: Response,
    seen_request: Option<Request>,
    seen_pack_prefix: Option<[u8; 4]>,
}

impl receive_pack::Delegate for RecordingDelegate {
    fn receive(
        &mut self,
        request: &Request,
        pack_data: &mut dyn std::io::Read,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.seen_request = Some(request.clone());

        let mut prefix = [0u8; 4];
        match pack_data.read_exact(&mut prefix) {
            Ok(()) => self.seen_pack_prefix = Some(prefix),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => self.seen_pack_prefix = None,
            Err(err) => return Err(err.into()),
        }

        Ok(self.response.clone())
    }
}

#[test]
fn parse_v1_request_from_real_push_basic_transcript() -> crate::Result {
    let mut input = BufReader::new(Cursor::new(fixture_request("push-basic.request")));
    let request = receive_pack::parse_v1_request(&mut input)?;

    assert_eq!(request.updates.len(), 1);
    assert_eq!(request.push_options.len(), 0);
    assert!(request.has_capability("report-status-v2"));
    assert!(request.has_capability("side-band-64k"));
    assert!(
        request.capabilities.iter().any(
            |capability| capability.name.as_bstr() == "object-format".as_bytes().as_bstr()
                && capability.value.as_ref().map(|value| value.as_bstr()) == Some("sha1".as_bytes().as_bstr())
        ),
        "real client transcript should include object-format=sha1"
    );
    assert!(
        request
            .capabilities
            .iter()
            .any(|capability| capability.name.as_bstr() == "agent".as_bytes().as_bstr()
                && capability
                    .value
                    .as_ref()
                    .is_some_and(|value| value.as_bstr().as_bytes().starts_with(b"git/"))),
        "real client transcript should include agent=git/<version>"
    );
    assert_eq!(
        request.updates[0].old_id.to_string(),
        "0000000000000000000000000000000000000000"
    );
    assert_eq!(
        request.updates[0].new_id.to_string(),
        "477eab3a52feaff241c8797fde5454349f125087"
    );
    assert_eq!(
        request.updates[0].ref_name.as_bstr(),
        "refs/heads/main".as_bytes().as_bstr()
    );

    let mut pack_prefix = [0u8; 4];
    input.read_exact(&mut pack_prefix)?;
    assert_eq!(pack_prefix, *b"PACK", "pack bytes should follow the command section");
    Ok(())
}

#[test]
fn parse_v1_request_from_real_push_with_option_transcript() -> crate::Result {
    let mut input = BufReader::new(Cursor::new(fixture_request("push-with-option.request")));
    let request = receive_pack::parse_v1_request(&mut input)?;

    assert_eq!(request.updates.len(), 1);
    assert!(request.has_capability("push-options"));
    assert_eq!(
        request.push_options,
        vec![bstr::BString::from("trace=1")],
        "client-sent push-options section should be preserved"
    );
    assert_eq!(
        request.updates[0].old_id.to_string(),
        "477eab3a52feaff241c8797fde5454349f125087"
    );
    assert_eq!(
        request.updates[0].new_id.to_string(),
        "2f189a1d2c8b843619bcd29aafaa95aa0bfeb4bd"
    );

    let mut pack_prefix = [0u8; 4];
    input.read_exact(&mut pack_prefix)?;
    assert_eq!(pack_prefix, *b"PACK");
    Ok(())
}

#[test]
fn parse_v1_request_from_real_delete_transcript_has_no_pack() -> crate::Result {
    let mut input = BufReader::new(Cursor::new(fixture_request("delete-main.request")));
    let request = receive_pack::parse_v1_request(&mut input)?;

    assert_eq!(request.updates.len(), 1);
    assert_eq!(request.push_options.len(), 0);
    assert_eq!(
        request.updates[0].old_id.to_string(),
        "2f189a1d2c8b843619bcd29aafaa95aa0bfeb4bd"
    );
    assert_eq!(
        request.updates[0].new_id.to_string(),
        "0000000000000000000000000000000000000000"
    );
    assert!(
        input.fill_buf()?.is_empty(),
        "delete transcript should not contain pack data"
    );
    Ok(())
}

#[test]
fn serve_v1_from_real_push_transcript_writes_sideband_report_status() -> crate::Result {
    let request = fixture_request("push-basic.request");
    let mut output = Vec::new();
    let mut delegate = RecordingDelegate {
        response: Response {
            unpack_status: UnpackStatus::Ok,
            ref_statuses: vec![RefStatus::Ok {
                ref_name: "refs/heads/main".into(),
            }],
            sideband_messages: Vec::new(),
        },
        ..Default::default()
    };

    let outcome = receive_pack::serve_v1(request.as_slice(), &mut output, &mut delegate, &receive_pack::ServerConfig::default())?;
    assert_eq!(outcome.updates_received, 1);
    assert_eq!(outcome.push_options_received, 0);
    assert_eq!(outcome.ref_statuses_sent, 1);
    assert!(outcome.report_status_sent);
    assert!(outcome.sideband_bytes_sent > 0);
    assert_eq!(delegate.seen_pack_prefix, Some(*b"PACK"));

    let mut sideband_reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    let mut report_status_payload = Vec::<u8>::new();
    while let Some(line) = sideband_reader.read_line() {
        let line = line??;
        match line.decode_band()? {
            BandRef::Data(data) => report_status_payload.extend_from_slice(data),
            BandRef::Progress(_) | BandRef::Error(_) => {}
        }
    }
    assert_eq!(sideband_reader.stopped_at(), Some(PacketLineRef::Flush));

    let mut report_status_reader =
        StreamingPeekableIter::new(report_status_payload.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(
        next_text_line(&mut report_status_reader)?.as_bstr(),
        "unpack ok".as_bytes().as_bstr()
    );
    assert_eq!(
        next_text_line(&mut report_status_reader)?.as_bstr(),
        "ok refs/heads/main".as_bytes().as_bstr()
    );
    assert!(report_status_reader.read_line().is_none());
    assert_eq!(report_status_reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

fn fixture_request(name: &str) -> Vec<u8> {
    crate::fixture_bytes(&format!("receive-pack/v1/{name}"))
}

fn next_text_line(reader: &mut StreamingPeekableIter<&[u8]>) -> Result<bstr::BString, Box<dyn std::error::Error>> {
    let line = reader
        .read_line()
        .expect("expected packetline")
        .expect("read should succeed")
        .expect("decode should succeed");
    Ok(line.as_text().expect("expected text packetline").as_bstr().to_owned())
}

/// Build a raw V1 receive-pack request as wire bytes.
///
/// `updates` is a list of `"old new refname"` strings.
/// `capabilities` is appended NUL-separated on the first command line.
/// `push_options` generates the optional push-options section.
/// `pack_data` is appended raw after the command section.
fn request_bytes(
    updates: &[&str],
    capabilities: &[&str],
    push_options: &[&str],
    pack_data: &[u8],
) -> Vec<u8> {
    use gix_transport::packetline::blocking_io::{Writer, encode};
    use std::io::Write as _;

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
        writer
            .write_all(first.as_bytes())
            .expect("writing to vec never fails");
        for update in &updates[1..] {
            writer
                .write_all(update.as_bytes())
                .expect("writing to vec never fails");
        }
        encode::flush_to_write(writer.inner_mut()).expect("flush to vec never fails");

        if !push_options.is_empty() {
            for option in push_options {
                writer
                    .write_all(option.as_bytes())
                    .expect("writing to vec never fails");
            }
            encode::flush_to_write(writer.inner_mut()).expect("flush to vec never fails");
        }
    }
    out.extend_from_slice(pack_data);
    out
}

/// Build a raw flush-only packet (no-op / empty push).
fn flush_only_bytes() -> Vec<u8> {
    use gix_transport::packetline::blocking_io::encode;
    let mut out = Vec::new();
    encode::flush_to_write(&mut out).expect("flush to vec never fails");
    out
}

// ——————————————————————————————————————————————————————————————————————————
// Integration tests exercising end-to-end `serve_v1` validation flows.
// ——————————————————————————————————————————————————————————————————————————

/// A delegate that records whether it was invoked, for testing that
/// validation short-circuits before delegate invocation.
#[derive(Default)]
struct TrackingDelegate {
    invoked: bool,
    response: Response,
}

impl receive_pack::Delegate for TrackingDelegate {
    fn receive(
        &mut self,
        _request: &Request,
        _pack_data: &mut dyn std::io::Read,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.invoked = true;
        Ok(self.response.clone())
    }
}

#[test]
fn serve_v1_full_push_with_valid_capabilities_succeeds_end_to_end() -> crate::Result {
    let input = request_bytes(
        &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
        &[
            "report-status",
            "side-band-64k",
            "object-format=sha1",
            "agent=git/test-client",
        ],
        &[],
        b"PACK\x00\x00\x00\x02",
    );
    let mut output = Vec::new();
    let mut delegate = RecordingDelegate {
        response: Response {
            unpack_status: UnpackStatus::Ok,
            ref_statuses: vec![RefStatus::Ok {
                ref_name: "refs/heads/main".into(),
            }],
            sideband_messages: Vec::new(),
        },
        ..Default::default()
    };

    let config = receive_pack::ServerConfig::default();
    let outcome = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config)?;

    assert_eq!(outcome.updates_received, 1, "single update command parsed");
    assert_eq!(outcome.ref_statuses_sent, 1, "one ref status sent back");
    assert!(outcome.report_status_sent, "report-status was negotiated");
    assert!(outcome.sideband_bytes_sent > 0, "sideband framing was used");
    assert!(
        delegate.seen_request.is_some(),
        "delegate must be invoked for valid pushes"
    );
    assert_eq!(
        delegate.seen_pack_prefix,
        Some(*b"PACK"),
        "delegate should see pack data"
    );
    Ok(())
}

#[test]
fn serve_v1_unknown_capability_returns_error_before_delegate_invocation() -> crate::Result {
    let input = request_bytes(
        &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
        &[
            "report-status",
            "side-band-64k",
            "ofs-delta",
            "object-format=sha1",
        ],
        &[],
        b"PACK\x00\x00\x00\x02",
    );
    let mut output = Vec::new();
    let mut delegate = TrackingDelegate::default();

    let config = receive_pack::ServerConfig::default();
    let result = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config);

    assert!(result.is_err(), "unknown capability should produce an error");
    let err = result.expect_err("should have errored");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("ofs-delta"),
        "error message should identify the unsupported capability, got: {err_msg}"
    );
    assert!(
        !delegate.invoked,
        "delegate must NOT be invoked when capability validation fails"
    );
    Ok(())
}

#[test]
fn serve_v1_sha256_mismatch_returns_early_rejection() -> crate::Result {
    let input = request_bytes(
        &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
        &[
            "report-status",
            "side-band-64k",
            "object-format=sha256",
            "agent=git/test-client",
        ],
        &[],
        b"PACK\x00\x00\x00\x02",
    );
    let mut output = Vec::new();
    let mut delegate = TrackingDelegate::default();

    // Server is configured for sha1 (default), but client requests sha256.
    // When compiled without the `sha256` feature on gix-hash, the value is
    // unrecognized (InvalidObjectFormat). With the feature enabled it would
    // be a mismatch (UnsupportedObjectFormat). Either way the push is rejected
    // before delegate invocation.
    let config = receive_pack::ServerConfig::default();
    let result = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config);

    assert!(result.is_err(), "sha256 vs sha1 server should produce an error");
    let err = result.expect_err("should have errored");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("sha256"),
        "error should reference the requested format, got: {err_msg}"
    );
    assert!(
        !delegate.invoked,
        "delegate must NOT be invoked when object-format validation fails"
    );
    Ok(())
}

#[test]
fn serve_v1_noop_push_empty_flush_returns_flush_and_noop_outcome() -> crate::Result {
    let input = flush_only_bytes();
    let mut output = Vec::new();
    let mut delegate = TrackingDelegate::default();

    let config = receive_pack::ServerConfig::default();
    let outcome = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config)?;

    assert_eq!(outcome.updates_received, 0, "no updates in a no-op push");
    assert_eq!(outcome.push_options_received, 0, "no push-options in a no-op push");
    assert_eq!(outcome.ref_statuses_sent, 0, "no ref statuses for no-op");
    assert!(!outcome.report_status_sent, "no report-status for no-op");
    assert_eq!(outcome.sideband_bytes_sent, 0, "no sideband data for no-op");
    assert!(
        !delegate.invoked,
        "delegate must NOT be invoked for no-op pushes"
    );
    // The output should be exactly a flush packet (4 bytes: "0000").
    assert_eq!(
        output, b"0000",
        "no-op push should produce only a flush packet as acknowledgment"
    );
    Ok(())
}

#[test]
fn serve_v1_delete_only_push_round_trip_processes_deletions_without_pack_data() -> crate::Result {
    // A delete-only push: old_id is non-zero, new_id is all zeros.
    let input = request_bytes(
        &[
            "808e50d724f604f69ab93c6da2919c014667bedb 0000000000000000000000000000000000000000 refs/heads/feature",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0000000000000000000000000000000000000000 refs/heads/old-branch",
        ],
        &[
            "report-status",
            "side-band-64k",
            "delete-refs",
            "object-format=sha1",
            "agent=git/test-client",
        ],
        &[],
        // No pack data follows a delete-only push.
        b"",
    );
    let mut output = Vec::new();

    /// A delegate that verifies pack_data is empty (no pack expected for deletions)
    /// and returns per-ref statuses for the deletions.
    struct DeleteDelegate {
        pack_bytes_read: usize,
    }

    impl receive_pack::Delegate for DeleteDelegate {
        fn receive(
            &mut self,
            request: &Request,
            pack_data: &mut dyn std::io::Read,
        ) -> Result<Response, Box<dyn std::error::Error + Send + Sync + 'static>> {
            // Try to read pack data — for a delete-only push, there should be none.
            let mut buf = Vec::new();
            self.pack_bytes_read = pack_data.read_to_end(&mut buf)?;

            let ref_statuses = request
                .updates
                .iter()
                .map(|u| RefStatus::Ok {
                    ref_name: u.ref_name.clone(),
                })
                .collect();

            Ok(Response {
                unpack_status: UnpackStatus::Ok,
                ref_statuses,
                sideband_messages: Vec::new(),
            })
        }
    }

    let mut delegate = DeleteDelegate { pack_bytes_read: 0 };

    let config = receive_pack::ServerConfig::default();
    let outcome = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config)?;

    assert_eq!(outcome.updates_received, 2, "two deletion commands parsed");
    assert_eq!(outcome.ref_statuses_sent, 2, "two ref statuses reported");
    assert!(outcome.report_status_sent, "report-status was negotiated");
    assert_eq!(
        delegate.pack_bytes_read, 0,
        "no pack data should be available for a delete-only push"
    );

    // Verify the response output contains report-status with both refs.
    let mut sideband_reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    let mut report_payload = Vec::<u8>::new();
    while let Some(line) = sideband_reader.read_line() {
        let line = line??;
        match line.decode_band()? {
            BandRef::Data(data) => report_payload.extend_from_slice(data),
            BandRef::Progress(_) | BandRef::Error(_) => {}
        }
    }
    assert_eq!(sideband_reader.stopped_at(), Some(PacketLineRef::Flush));

    let mut report_reader =
        StreamingPeekableIter::new(report_payload.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(
        next_text_line(&mut report_reader)?.as_bstr(),
        "unpack ok".as_bytes().as_bstr()
    );
    assert_eq!(
        next_text_line(&mut report_reader)?.as_bstr(),
        "ok refs/heads/feature".as_bytes().as_bstr()
    );
    assert_eq!(
        next_text_line(&mut report_reader)?.as_bstr(),
        "ok refs/heads/old-branch".as_bytes().as_bstr()
    );
    assert!(report_reader.read_line().is_none());
    assert_eq!(report_reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

#[test]
fn serve_v1_quiet_mode_filters_progress_but_preserves_errors_in_sideband() -> crate::Result {
    use receive_pack::{SidebandMessage, SidebandMessageKind};

    let input = request_bytes(
        &["0000000000000000000000000000000000000000 808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main"],
        &[
            "report-status",
            "side-band-64k",
            "quiet",
            "object-format=sha1",
            "agent=git/test-client",
        ],
        &[],
        b"PACK\x00\x00\x00\x02",
    );
    let mut output = Vec::new();
    let mut delegate = RecordingDelegate {
        response: Response {
            unpack_status: UnpackStatus::Ok,
            ref_statuses: vec![RefStatus::Ok {
                ref_name: "refs/heads/main".into(),
            }],
            sideband_messages: vec![
                SidebandMessage {
                    kind: SidebandMessageKind::Progress,
                    text: "Counting objects: 3, done.\n".into(),
                },
                SidebandMessage {
                    kind: SidebandMessageKind::Progress,
                    text: "Compressing objects: 100%\n".into(),
                },
                SidebandMessage {
                    kind: SidebandMessageKind::Error,
                    text: "warning: hook declined push\n".into(),
                },
                SidebandMessage {
                    kind: SidebandMessageKind::Error,
                    text: "error: ref update failed\n".into(),
                },
            ],
        },
        ..Default::default()
    };

    let config = receive_pack::ServerConfig::default();
    let outcome = receive_pack::serve_v1(input.as_slice(), &mut output, &mut delegate, &config)?;

    assert!(outcome.report_status_sent, "report-status was negotiated");
    assert!(outcome.sideband_bytes_sent > 0, "sideband data should be written");

    // Parse the sideband output and count progress vs error messages.
    let mut sideband_reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    let mut progress_messages = Vec::new();
    let mut error_messages = Vec::new();
    while let Some(line) = sideband_reader.read_line() {
        let line = line??;
        match line.decode_band()? {
            BandRef::Progress(data) => progress_messages.push(data.to_vec()),
            BandRef::Error(data) => error_messages.push(data.to_vec()),
            BandRef::Data(_) => {} // report-status payload
        }
    }
    assert_eq!(sideband_reader.stopped_at(), Some(PacketLineRef::Flush));

    assert_eq!(
        progress_messages.len(),
        0,
        "quiet mode should suppress all progress messages"
    );
    assert_eq!(
        error_messages.len(),
        2,
        "quiet mode should preserve all error messages"
    );
    assert_eq!(
        error_messages[0].as_bstr(),
        "warning: hook declined push\n".as_bytes().as_bstr(),
        "first error message preserved"
    );
    assert_eq!(
        error_messages[1].as_bstr(),
        "error: ref update failed\n".as_bytes().as_bstr(),
        "second error message preserved"
    );
    Ok(())
}
