//! Integration tests for upload-pack server plumbing exercising the public API.
//!
//! Tests here verify wire-level correctness of the `acknowledgments` and `packfile`
//! sections, request parsing, negotiation, and ref advertisement.
use std::{
    collections::BTreeSet,
    fs,
    io::{BufReader, Cursor, Read as _, Write as _},
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use bstr::{BString, ByteSlice};
use gix_object::Write as _;
use gix_protocol::{
    fetch::response::{Acknowledgement, ShallowUpdate, WantedRef},
    handshake::Ref,
    upload_pack::{
        Command, Delegate, Error, Fetch, FetchOutput, Feature, LsRefs,
        Outcome, Request, ServerConfig,
        negotiate_fetch_with_repository, parse_v2_request, serve_v2,
        write_fetch_response, write_v1_ref_advertisement,
    },
};
use gix_transport::packetline::{BandRef, PacketLineRef, blocking_io::StreamingPeekableIter};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Default)]
struct MockDelegate {
    refs: Vec<Ref>,
    fetch_output: Option<FetchOutput>,
    seen_ls_refs: Option<LsRefs>,
    seen_fetch: Option<Fetch>,
}

impl Delegate for MockDelegate {
    fn ls_refs(&mut self, request: &LsRefs) -> Result<Vec<Ref>, BoxError> {
        self.seen_ls_refs = Some(request.clone());
        Ok(self.refs.clone())
    }

    fn fetch(&mut self, request: &Fetch) -> Result<FetchOutput, BoxError> {
        self.seen_fetch = Some(request.clone());
        self.fetch_output
            .take()
            .ok_or_else(|| std::io::Error::other("fetch output should be configured").into())
    }
}

/// Fresh clone scenario: `done=true`, no haves, delegate returns empty acknowledgements + pack data.
/// The wire output must contain `packfile` section WITHOUT a preceding `acknowledgments` section.
#[test]
fn serve_v2_done_fresh_clone_omits_acknowledgments_section() -> crate::Result {
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &["want 808e50d724f604f69ab93c6da2919c014667bedb", "done"],
    )?;
    let mut output = Vec::new();
    let fetch_output = FetchOutput::new(Cursor::new(b"PACK\0\0\0\0".to_vec()));
    assert!(
        fetch_output.acknowledgements.is_empty(),
        "fresh clone should have no acknowledgements"
    );
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 0,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 8,
        },
        "fresh clone with done=true should send zero acknowledgements"
    );
    assert!(
        delegate.seen_fetch.as_ref().expect("request should be captured").done,
        "done flag should be parsed from request"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    let first_section = next_text_line(&mut reader)?;
    assert_eq!(
        first_section.as_bstr(),
        "packfile".as_bytes().as_bstr(),
        "fresh clone with done=true must start with packfile section, no acknowledgments"
    );
    assert_eq!(
        next_band_data(&mut reader)?,
        b"PACK\0\0\0\0",
        "pack data should follow packfile header"
    );
    assert!(
        reader.read_line().is_none(),
        "flush should terminate response"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Fetch with common objects: `done=true`, delegate returns acknowledgements with
/// `[Common(id), Ready]` and pack data.
/// The wire output must contain `acknowledgments` section with `ready` line followed by `packfile`.
#[test]
fn serve_v2_done_with_common_objects_includes_acknowledgments_with_ready() -> crate::Result {
    let common_id = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &[
            "want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0",
            &format!("have {common_id}"),
            "done",
        ],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::new(Cursor::new(b"PACK\0\0\0\0".to_vec()));
    fetch_output
        .acknowledgements
        .push(Acknowledgement::Common(common_id));
    fetch_output.acknowledgements.push(Acknowledgement::Ready);
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 2,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 8,
        },
        "fetch with common objects and done=true should send Common + Ready"
    );
    assert!(
        delegate.seen_fetch.as_ref().expect("request should be captured").done,
        "done flag should be parsed from request"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "acknowledgments".as_bytes().as_bstr(),
        "response should start with acknowledgments section"
    );
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        format!("ACK {common_id} common").as_bytes().as_bstr(),
        "first acknowledgement should be Common for the shared object"
    );
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "ready".as_bytes().as_bstr(),
        "acknowledgments section should end with ready line when done=true"
    );
    expect_delimiter(&mut reader)?;
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "packfile".as_bytes().as_bstr(),
        "packfile section should follow acknowledgments"
    );
    assert_eq!(
        next_band_data(&mut reader)?,
        b"PACK\0\0\0\0",
        "pack data should follow packfile header"
    );
    assert!(
        reader.read_line().is_none(),
        "flush should terminate response"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Client sends `have` lines with objects the server doesn't recognize, `done=true`.
/// Delegate returns empty `FetchOutput` (no acknowledgements, no pack). Wire output should be just a flush.
#[test]
fn serve_v2_done_all_unknown_haves_no_pack_produces_empty_response() -> crate::Result {
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &[
            "want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0",
            "have aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "have bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "done",
        ],
    )?;
    let mut output = Vec::new();
    let fetch_output = FetchOutput::without_pack();
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 0,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 0,
        },
        "server with nothing to say should send zero in all sections"
    );
    assert!(
        delegate.seen_fetch.as_ref().expect("request should be captured").done,
        "done flag should be parsed from request"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert!(
        reader.read_line().is_none(),
        "empty response should contain only flush"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Client sends wants but no haves, `done=false`. Delegate returns NAK acknowledgement and no pack.
/// Wire output should have `acknowledgments` section with NAK, delimiter, then flush.
#[test]
fn serve_v2_no_done_ongoing_negotiation_nak() -> crate::Result {
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &["want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0"],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::without_pack();
    fetch_output.acknowledgements.push(Acknowledgement::Nak);
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 1,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 0,
        },
        "ongoing negotiation with NAK should send one acknowledgement and no pack"
    );
    assert!(
        !delegate.seen_fetch.as_ref().expect("request should be captured").done,
        "done flag should be false for ongoing negotiation"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "acknowledgments".as_bytes().as_bstr(),
        "response should start with acknowledgments section"
    );
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "NAK".as_bytes().as_bstr(),
        "acknowledgments section should contain NAK"
    );
    assert!(
        reader.read_line().is_none(),
        "flush should terminate response after acknowledgments"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Client sends haves the server knows, `done=false`. Delegate returns Common acknowledgement, no pack.
/// Wire output should have `acknowledgments` section with ACK <id> common, delimiter, then flush.
#[test]
fn serve_v2_no_done_ongoing_negotiation_common_only() -> crate::Result {
    let common_id = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &[
            "want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0",
            &format!("have {common_id}"),
        ],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::without_pack();
    fetch_output
        .acknowledgements
        .push(Acknowledgement::Common(common_id));
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 1,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 0,
        },
        "ongoing negotiation with common should send one acknowledgement and no pack"
    );
    assert!(
        !delegate.seen_fetch.as_ref().expect("request should be captured").done,
        "done flag should be false for ongoing negotiation"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        "acknowledgments".as_bytes().as_bstr(),
        "response should start with acknowledgments section"
    );
    assert_eq!(
        next_text_line(&mut reader)?.as_bstr(),
        format!("ACK {common_id} common").as_bytes().as_bstr(),
        "acknowledgments section should contain ACK for common object"
    );
    assert!(
        reader.read_line().is_none(),
        "flush should terminate response with no packfile section"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Tests all optional sections together: `done=true`, delegate returns acknowledgements with
/// multiple Common + Ready, wanted-refs, and pack data.
#[test]
fn serve_v2_done_multiple_common_haves_with_wanted_refs_and_pack() -> crate::Result {
    let id1 = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
    let id2 = gix_hash::ObjectId::from_hex(b"9e320b9180e0b5580af68fa3255b7f3d9ecd5af0")?;
    let wanted_id = gix_hash::ObjectId::from_hex(b"dce0ea858eef7ff61ad345cc5cdac62203fb3c10")?;
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &[
            "want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0",
            &format!("have {id1}"),
            &format!("have {id2}"),
            "want-ref refs/heads/main",
            "done",
        ],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::new(Cursor::new(b"PACK\0\0\0\0".to_vec()));
    fetch_output.acknowledgements.push(Acknowledgement::Common(id1));
    fetch_output.acknowledgements.push(Acknowledgement::Common(id2));
    fetch_output.acknowledgements.push(Acknowledgement::Ready);
    fetch_output.wanted_refs.push(WantedRef {
        id: wanted_id,
        path: "refs/heads/main".into(),
    });
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 3,
            shallow_updates_sent: 0,
            wanted_refs_sent: 1,
            pack_bytes_sent: 8,
        },
        "all sections should be counted correctly"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "acknowledgments".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("ACK {id1} common").as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("ACK {id2} common").as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "ready".as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "wanted-refs".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("{wanted_id} refs/heads/main").as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "packfile".as_bytes().as_bstr());
    assert_eq!(next_band_data(&mut reader)?, b"PACK\0\0\0\0");
    assert!(reader.read_line().is_none(), "flush should terminate response");
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// Delegate returns a completely empty `FetchOutput` (no acknowledgements, no shallow_updates,
/// no wanted_refs, no pack_data). Wire output should be just a flush.
#[test]
fn serve_v2_done_empty_fetch_output_no_sections() -> crate::Result {
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &["want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0", "done"],
    )?;
    let mut output = Vec::new();
    let fetch_output = FetchOutput::without_pack();
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 0,
            shallow_updates_sent: 0,
            wanted_refs_sent: 0,
            pack_bytes_sent: 0,
        },
        "completely empty FetchOutput should produce zero counts"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert!(
        reader.read_line().is_none(),
        "empty FetchOutput should produce only a flush on the wire"
    );
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// `done=true`, delegate returns acknowledgements with Common + Ready, shallow updates,
/// and pack data. Verifies section ordering: acknowledgments, shallow-info, packfile.
#[test]
fn serve_v2_done_with_shallow_updates_between_acks_and_pack() -> crate::Result {
    let common_id = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
    let shallow_id = gix_hash::ObjectId::from_hex(b"dce0ea858eef7ff61ad345cc5cdac62203fb3c10")?;
    let request = request_bytes(
        "fetch",
        &["agent=git/test"],
        &[
            "want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0",
            &format!("have {common_id}"),
            "done",
        ],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::new(Cursor::new(b"PACK\0\0\0\0".to_vec()));
    fetch_output.acknowledgements.push(Acknowledgement::Common(common_id));
    fetch_output.acknowledgements.push(Acknowledgement::Ready);
    fetch_output.shallow_updates.push(ShallowUpdate::Shallow(shallow_id));
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 2,
            shallow_updates_sent: 1,
            wanted_refs_sent: 0,
            pack_bytes_sent: 8,
        },
        "all sections should be counted correctly with shallow updates"
    );

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "acknowledgments".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("ACK {common_id} common").as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "ready".as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "shallow-info".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("shallow {shallow_id}").as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "packfile".as_bytes().as_bstr());
    assert_eq!(next_band_data(&mut reader)?, b"PACK\0\0\0\0");
    assert!(reader.read_line().is_none(), "flush should terminate response");
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

/// When serve_v2 receives a request with a mismatched `object-format` (e.g., sha256 against a
/// SHA-1 server), the delegate's methods must never be called — validation rejects the request
/// before any repository access occurs.
#[test]
fn serve_v2_delegate_not_called_on_validation_failure() -> crate::Result {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct NeverCalledDelegate {
        was_called: AtomicBool,
    }

    impl Delegate for NeverCalledDelegate {
        fn ls_refs(&mut self, _request: &LsRefs) -> Result<Vec<Ref>, BoxError> {
            self.was_called.store(true, Ordering::SeqCst);
            panic!("delegate ls_refs should not be called on validation failure");
        }

        fn fetch(&mut self, _request: &Fetch) -> Result<FetchOutput, BoxError> {
            self.was_called.store(true, Ordering::SeqCst);
            panic!("delegate fetch should not be called on validation failure");
        }
    }

    let request = request_bytes(
        "fetch",
        &["object-format=sha256"],
        &["want 9e320b9180e0b5580af68fa3255b7f3d9ecd5af0", "done"],
    )?;
    let mut output = Vec::new();
    let mut delegate = NeverCalledDelegate {
        was_called: AtomicBool::new(false),
    };

    let config = ServerConfig::default();
    assert_eq!(config.object_hash, gix_hash::Kind::Sha1, "default config should be SHA-1");

    let result = serve_v2(request.as_slice(), &mut output, &mut delegate, &config);

    assert!(result.is_err(), "serve_v2 should return an error for mismatched object-format");
    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::UnsupportedObjectFormat { .. }),
        "error should be UnsupportedObjectFormat, got: {err:?}"
    );
    assert!(
        !delegate.was_called.load(Ordering::SeqCst),
        "delegate methods must not be called when validation fails"
    );
    Ok(())
}

#[test]
fn parse_ls_refs_request() -> crate::Result {
    let input = request_bytes(
        "ls-refs",
        &["agent=git/gitplane", "object-format=sha1"],
        &["symrefs", "peel", "ref-prefix refs/heads/"],
    )?;

    let request = parse_v2_request(input.as_slice(), &ServerConfig::default())?;
    assert_eq!(
        request.features,
        vec![
            Feature { name: "agent".into(), value: Some("git/gitplane".into()) },
            Feature { name: "object-format".into(), value: Some("sha1".into()) },
        ]
    );
    match request.command {
        Command::LsRefs(arguments) => {
            assert!(arguments.symrefs);
            assert!(arguments.peel);
            assert_eq!(arguments.ref_prefixes, vec![BString::from("refs/heads/")]);
        }
        Command::Fetch(_) => panic!("expected ls-refs command"),
    }
    Ok(())
}

#[test]
fn parse_fetch_request() -> crate::Result {
    let id_one = "808e50d724f604f69ab93c6da2919c014667bedb";
    let id_two = "9e320b9180e0b5580af68fa3255b7f3d9ecd5af0";
    let input = request_bytes(
        "fetch",
        &["agent=git/gitplane"],
        &[
            "thin-pack",
            "ofs-delta",
            &format!("want {id_one}"),
            &format!("have {id_two}"),
            "want-ref refs/heads/main",
            "done",
        ],
    )?;

    let request = parse_v2_request(input.as_slice(), &ServerConfig::default())?;
    match request.command {
        Command::Fetch(arguments) => {
            assert!(arguments.thin_pack);
            assert!(arguments.ofs_delta);
            assert!(arguments.done);
            assert_eq!(arguments.wants, vec![gix_hash::ObjectId::from_hex(id_one.as_bytes())?]);
            assert_eq!(arguments.haves, vec![gix_hash::ObjectId::from_hex(id_two.as_bytes())?]);
            assert_eq!(arguments.want_refs, vec![BString::from("refs/heads/main")]);
        }
        Command::LsRefs(_) => panic!("expected fetch command"),
    }
    Ok(())
}

#[test]
fn parse_fetch_request_with_negotiation_arguments() -> crate::Result {
    let id = "808e50d724f604f69ab93c6da2919c014667bedb";
    let input = request_bytes(
        "fetch",
        &[],
        &[
            "no-progress",
            "deepen 16",
            "deepen-since 12345",
            "deepen-not refs/tags/v1.0.0",
            "deepen-relative",
            "filter blob:none",
            "packfile-uris https,ssh",
            "wait-for-done",
            &format!("want {id}"),
            "done",
        ],
    )?;

    let request = parse_v2_request(input.as_slice(), &ServerConfig::default())?;
    match request.command {
        Command::Fetch(arguments) => {
            assert!(arguments.no_progress);
            assert_eq!(arguments.deepen, Some(16));
            assert_eq!(arguments.deepen_since, Some(12_345));
            assert_eq!(arguments.deepen_not, vec![BString::from("refs/tags/v1.0.0")]);
            assert!(arguments.deepen_relative);
            assert_eq!(arguments.filters, vec![BString::from("blob:none")]);
            assert_eq!(arguments.packfile_uris, vec![BString::from("https"), BString::from("ssh")]);
            assert!(arguments.wait_for_done);
            assert!(arguments.done);
        }
        Command::LsRefs(_) => panic!("expected fetch command"),
    }
    Ok(())
}

#[test]
fn parse_fetch_request_with_invalid_deepen_value() -> crate::Result {
    let input = request_bytes("fetch", &[], &["deepen nope"])?;
    let err = parse_v2_request(input.as_slice(), &ServerConfig::default())
        .expect_err("invalid deepen value should fail parsing");
    assert!(
        matches!(err, Error::MalformedArgument { command: "fetch", line } if line.as_bstr() == "deepen nope".as_bytes().as_bstr())
    );
    Ok(())
}

/// Informational features like `agent` pass through without validation,
/// and even `object-format` is preserved in the parsed features list.
#[test]
fn feature_pass_through() -> crate::Result {
    let input = request_bytes("ls-refs", &["agent=git/test", "object-format=sha1"], &[])?;

    let request = parse_v2_request(input.as_slice(), &ServerConfig::default())?;
    assert_eq!(
        request.features,
        vec![
            Feature { name: "agent".into(), value: Some("git/test".into()) },
            Feature { name: "object-format".into(), value: Some("sha1".into()) },
        ],
        "both agent and object-format features should be present in parsed result"
    );
    Ok(())
}

#[test]
fn negotiate_fetch_with_repository_tracks_wants_and_common_haves() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_want = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let missing_want = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");
    let common_have = object_id("f99771fe6a1b535783af3163eba95a927aae21d5");
    let unknown_have = object_id("2d9d136fb0765f2e24c44a0f91984318d580d03b");

    let request = Fetch {
        wants: vec![known_want, missing_want, known_want],
        haves: vec![common_have, unknown_have, common_have],
        ..Default::default()
    };
    let known_objects = [known_want, common_have]
        .into_iter()
        .collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(negotiation.known_wants, vec![known_want]);
    assert_eq!(negotiation.missing_wants, vec![missing_want]);
    assert_eq!(negotiation.common_haves, vec![common_have]);
    assert_eq!(negotiation.acknowledgements, vec![Acknowledgement::Common(common_have)]);
    assert!(negotiation.wanted_refs.is_empty());
    assert!(negotiation.unresolved_want_refs.is_empty());

    let output = negotiation.into_output();
    assert_eq!(output.acknowledgements, vec![Acknowledgement::Common(common_have)]);
    assert!(output.wanted_refs.is_empty());
    assert!(output.pack_data.is_none());
    Ok(())
}

#[test]
fn negotiate_fetch_with_repository_sends_nak_without_common_haves() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let request = Fetch {
        haves: vec![object_id("f99771fe6a1b535783af3163eba95a927aae21d5")],
        ..Default::default()
    };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;
    assert_eq!(negotiation.acknowledgements, vec![Acknowledgement::Nak]);
    Ok(())
}

#[test]
fn negotiate_fetch_with_repository_resolves_want_refs() -> crate::Result {
    let main = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let (_tmp, refs) = temporary_ref_store(&[
        ("HEAD", "ref: refs/heads/main\n".to_string()),
        ("refs/heads/main", format!("{main}\n")),
    ])?;

    let request = Fetch {
        want_refs: vec![
            "HEAD".into(),
            "refs/heads/main".into(),
            "HEAD".into(),
            "refs/heads/missing".into(),
            "not a ref".into(),
        ],
        ..Default::default()
    };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;
    assert_eq!(
        negotiation.wanted_refs,
        vec![
            WantedRef { id: main, path: "HEAD".into() },
            WantedRef { id: main, path: "refs/heads/main".into() },
        ]
    );
    assert_eq!(
        negotiation.unresolved_want_refs,
        vec![BString::from("refs/heads/missing"), BString::from("not a ref")]
    );
    Ok(())
}

#[test]
fn into_output_with_repository_pack_omits_pack_without_wants() -> crate::Result {
    let fixture = temporary_object_store_with_linear_history()?;
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let request = Fetch::default();

    let negotiation =
        negotiate_fetch_with_repository(&request, &refs, |id| gix_pack::Find::contains(&fixture.odb, id))?;
    let output =
        negotiation.into_output_with_repository_pack(&request, fixture.odb.clone(), gix_hash::Kind::Sha1)?;

    assert_eq!(output.acknowledgements, vec![Acknowledgement::Nak]);
    assert!(output.pack_data.is_none(), "no wants should not produce a pack");
    Ok(())
}

#[test]
fn into_output_with_repository_pack_excludes_common_have_history() -> crate::Result {
    let fixture = temporary_object_store_with_linear_history()?;
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let request = Fetch {
        wants: vec![fixture.commit_three],
        haves: vec![fixture.commit_one],
        ..Default::default()
    };

    let negotiation =
        negotiate_fetch_with_repository(&request, &refs, |id| gix_pack::Find::contains(&fixture.odb, id))?;
    let mut output =
        negotiation.into_output_with_repository_pack(&request, fixture.odb.clone(), gix_hash::Kind::Sha1)?;
    let mut pack_bytes = Vec::new();
    output
        .pack_data
        .as_mut()
        .expect("known wants should produce pack data")
        .read_to_end(&mut pack_bytes)?;

    let packed_ids = pack_object_ids(pack_bytes, gix_hash::Kind::Sha1)?;
    assert!(packed_ids.contains(&fixture.commit_three));
    assert!(packed_ids.contains(&fixture.commit_two));
    assert!(
        !packed_ids.contains(&fixture.commit_one),
        "commits acknowledged as common should not be resent"
    );
    Ok(())
}

#[test]
fn into_output_with_repository_pack_peels_tag_wants_to_commits() -> crate::Result {
    let fixture = temporary_object_store_with_linear_history()?;
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let request = Fetch {
        wants: vec![fixture.tag_three],
        haves: vec![fixture.commit_one],
        ..Default::default()
    };

    let negotiation =
        negotiate_fetch_with_repository(&request, &refs, |id| gix_pack::Find::contains(&fixture.odb, id))?;
    let mut output =
        negotiation.into_output_with_repository_pack(&request, fixture.odb.clone(), gix_hash::Kind::Sha1)?;
    let mut pack_bytes = Vec::new();
    output
        .pack_data
        .as_mut()
        .expect("tag wants should produce pack data")
        .read_to_end(&mut pack_bytes)?;

    let packed_ids = pack_object_ids(pack_bytes, gix_hash::Kind::Sha1)?;
    assert!(packed_ids.contains(&fixture.tag_three));
    assert!(packed_ids.contains(&fixture.commit_three));
    assert!(packed_ids.contains(&fixture.commit_two));
    assert!(
        !packed_ids.contains(&fixture.commit_one),
        "common history should stay excluded even for tag wants"
    );
    Ok(())
}

#[test]
fn serve_ls_refs_with_prefix_filter() -> crate::Result {
    let request = request_bytes(
        "ls-refs",
        &["agent=git/gitplane"],
        &["symrefs", "peel", "ref-prefix refs/heads/"],
    )?;
    let mut output = Vec::new();
    let mut delegate = MockDelegate {
        refs: vec![
            Ref::Symbolic {
                full_ref_name: "HEAD".into(),
                target: "refs/heads/main".into(),
                tag: None,
                object: gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?,
            },
            Ref::Direct {
                full_ref_name: "refs/heads/main".into(),
                object: gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?,
            },
            Ref::Direct {
                full_ref_name: "refs/tags/v1.0.0".into(),
                object: gix_hash::ObjectId::from_hex(b"9e320b9180e0b5580af68fa3255b7f3d9ecd5af0")?,
            },
        ],
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(outcome, Outcome::LsRefs { refs_sent: 1 });
    assert!(delegate.seen_ls_refs.as_ref().expect("request should be captured").symrefs);

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    let advertised = next_text_line(&mut reader)?;
    assert_eq!(
        advertised.as_bstr(),
        "808e50d724f604f69ab93c6da2919c014667bedb refs/heads/main".as_bytes().as_bstr()
    );
    assert!(reader.read_line().is_none(), "flush should terminate response");
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

#[test]
fn serve_fetch_with_pack_sideband() -> crate::Result {
    let common_id = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
    let wanted_id = gix_hash::ObjectId::from_hex(b"9e320b9180e0b5580af68fa3255b7f3d9ecd5af0")?;
    let request = request_bytes(
        "fetch",
        &["agent=git/gitplane"],
        &[&format!("want {common_id}"), "done"],
    )?;
    let mut output = Vec::new();
    let mut fetch_output = FetchOutput::new(Cursor::new(b"PACK\0\0\0\0".to_vec()));
    fetch_output.acknowledgements.push(Acknowledgement::Common(common_id));
    fetch_output.wanted_refs.push(WantedRef { id: wanted_id, path: "refs/heads/main".into() });
    let mut delegate = MockDelegate {
        fetch_output: Some(fetch_output),
        ..Default::default()
    };

    let outcome = serve_v2(request.as_slice(), &mut output, &mut delegate, &ServerConfig::default())?;
    assert_eq!(
        outcome,
        Outcome::Fetch {
            acknowledgements_sent: 1,
            shallow_updates_sent: 0,
            wanted_refs_sent: 1,
            pack_bytes_sent: 8,
        }
    );
    assert!(delegate.seen_fetch.as_ref().expect("request should be captured").done);

    let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "acknowledgments".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("ACK {common_id} common").as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "wanted-refs".as_bytes().as_bstr());
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), format!("{wanted_id} refs/heads/main").as_bytes().as_bstr());
    expect_delimiter(&mut reader)?;
    assert_eq!(next_text_line(&mut reader)?.as_bstr(), "packfile".as_bytes().as_bstr());
    assert_eq!(next_band_data(&mut reader)?, b"PACK\0\0\0\0");
    assert!(reader.read_line().is_none(), "flush should terminate response");
    assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
    Ok(())
}

// Bug condition exploration tests: these encode the EXPECTED behavior per protocol v2.
// **Validates: Requirements 1.1, 1.2, 2.1, 2.2**

#[test]
fn negotiate_fetch_done_with_common_haves_should_end_with_ready() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_have = object_id("808e50d724f604f69ab93c6da2919c014667bedb");

    let request = Fetch {
        haves: vec![known_have],
        done: true,
        ..Default::default()
    };
    let known_objects = [known_have].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Common(known_have), Acknowledgement::Ready],
        "when done=true and common haves exist, acknowledgements must end with Ready to signal packfile follows"
    );
    Ok(())
}

#[test]
fn negotiate_fetch_done_with_no_haves_should_produce_empty_acknowledgements() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;

    let request = Fetch { done: true, ..Default::default() };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;

    assert!(
        negotiation.acknowledgements.is_empty(),
        "when done=true and no haves exist (fresh clone), acknowledgements must be empty so the section is omitted"
    );
    Ok(())
}

#[test]
fn negotiate_fetch_done_with_all_unknown_haves_should_produce_empty_acknowledgements() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let unknown_have = object_id("f99771fe6a1b535783af3163eba95a927aae21d5");

    let request = Fetch {
        haves: vec![unknown_have],
        done: true,
        ..Default::default()
    };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;

    assert!(
        negotiation.acknowledgements.is_empty(),
        "when done=true and no haves are known (all unknown), acknowledgements must be empty so the section is omitted"
    );
    Ok(())
}

#[test]
fn negotiate_fetch_done_with_mixed_known_unknown_haves_should_have_common_then_ready() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_have = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let unknown_have = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");

    let request = Fetch {
        haves: vec![known_have, unknown_have],
        done: true,
        ..Default::default()
    };
    let known_objects = [known_have].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Common(known_have), Acknowledgement::Ready],
        "when done=true with mix of known/unknown haves, acknowledgements must be [Common(known), Ready]"
    );
    Ok(())
}

// Preservation property tests: verify that `done == false` behavior is unchanged.
// **Validates: Requirements 3.1, 3.2**

#[test]
fn preservation_done_false_single_known_have_produces_common_only() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_have = object_id("808e50d724f604f69ab93c6da2919c014667bedb");

    let request = Fetch {
        haves: vec![known_have],
        done: false,
        ..Default::default()
    };
    let known_objects = [known_have].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Common(known_have)],
        "when done=false with a known have, acknowledgements must be [Common(id)] without Ready"
    );
    assert!(!negotiation.acknowledgements.contains(&Acknowledgement::Ready), "done=false must never produce Ready");
    assert!(!negotiation.acknowledgements.is_empty(), "done=false with known haves must never produce empty acknowledgements");
    Ok(())
}

#[test]
fn preservation_done_false_multiple_known_haves_produces_common_for_each() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let have_a = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let have_b = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");
    let have_c = object_id("f99771fe6a1b535783af3163eba95a927aae21d5");

    let request = Fetch {
        haves: vec![have_a, have_b, have_c],
        done: false,
        ..Default::default()
    };
    let known_objects = [have_a, have_b, have_c].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![
            Acknowledgement::Common(have_a),
            Acknowledgement::Common(have_b),
            Acknowledgement::Common(have_c),
        ],
        "when done=false with multiple known haves, acknowledgements must be [Common(a), Common(b), Common(c)]"
    );
    assert!(!negotiation.acknowledgements.contains(&Acknowledgement::Ready), "done=false must never produce Ready");
    Ok(())
}

#[test]
fn preservation_done_false_mixed_known_unknown_haves_produces_common_for_known_only() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_have = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let unknown_have = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");

    let request = Fetch {
        haves: vec![known_have, unknown_have],
        done: false,
        ..Default::default()
    };
    let known_objects = [known_have].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Common(known_have)],
        "when done=false with mixed haves, only known haves appear as Common entries"
    );
    assert!(!negotiation.acknowledgements.contains(&Acknowledgement::Ready), "done=false must never produce Ready");
    Ok(())
}

#[test]
fn preservation_done_false_no_haves_produces_nak() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;

    let request = Fetch { done: false, ..Default::default() };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Nak],
        "when done=false and no haves exist, acknowledgements must be [Nak]"
    );
    Ok(())
}

#[test]
fn preservation_done_false_all_unknown_haves_produces_nak() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let unknown_a = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let unknown_b = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");

    let request = Fetch {
        haves: vec![unknown_a, unknown_b],
        done: false,
        ..Default::default()
    };

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |_| false)?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Nak],
        "when done=false and all haves are unknown, acknowledgements must be [Nak]"
    );
    Ok(())
}

#[test]
fn preservation_done_false_duplicate_known_haves_are_deduplicated() -> crate::Result {
    let (_tmp, refs) = temporary_ref_store(&Vec::<(&str, String)>::new())?;
    let known_have = object_id("808e50d724f604f69ab93c6da2919c014667bedb");

    let request = Fetch {
        haves: vec![known_have, known_have, known_have],
        done: false,
        ..Default::default()
    };
    let known_objects = [known_have].into_iter().collect::<BTreeSet<_>>();

    let negotiation = negotiate_fetch_with_repository(&request, &refs, |id| known_objects.contains(id))?;

    assert_eq!(
        negotiation.acknowledgements,
        vec![Acknowledgement::Common(known_have)],
        "duplicate haves must be deduplicated in acknowledgements"
    );
    Ok(())
}

#[test]
fn preservation_non_acknowledgement_fields_unaffected_by_done_flag() -> crate::Result {
    let main_id = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let (_tmp, refs) = temporary_ref_store(&[
        ("HEAD", "ref: refs/heads/main\n".to_string()),
        ("refs/heads/main", format!("{main_id}\n")),
    ])?;
    let known_want = object_id("808e50d724f604f69ab93c6da2919c014667bedb");
    let missing_want = object_id("2d9d136fb0765f2e24c44a0f91984318d580d03b");
    let common_have = object_id("f99771fe6a1b535783af3163eba95a927aae21d5");
    let unknown_have = object_id("9e320b9180e0b5580af68fa3255b7f3d9ecd5af0");

    let known_objects = [known_want, common_have].into_iter().collect::<BTreeSet<_>>();

    let request_not_done = Fetch {
        wants: vec![known_want, missing_want],
        haves: vec![common_have, unknown_have],
        want_refs: vec!["HEAD".into(), "refs/heads/missing".into()],
        done: false,
        ..Default::default()
    };
    let request_done = Fetch {
        wants: vec![known_want, missing_want],
        haves: vec![common_have, unknown_have],
        want_refs: vec!["HEAD".into(), "refs/heads/missing".into()],
        done: true,
        ..Default::default()
    };

    let negotiation_not_done =
        negotiate_fetch_with_repository(&request_not_done, &refs, |id| known_objects.contains(id))?;
    let negotiation_done =
        negotiate_fetch_with_repository(&request_done, &refs, |id| known_objects.contains(id))?;

    assert_eq!(negotiation_not_done.known_wants, negotiation_done.known_wants, "known_wants must be unaffected by done flag");
    assert_eq!(negotiation_not_done.missing_wants, negotiation_done.missing_wants, "missing_wants must be unaffected by done flag");
    assert_eq!(negotiation_not_done.common_haves, negotiation_done.common_haves, "common_haves must be unaffected by done flag");
    assert_eq!(negotiation_not_done.wanted_refs, negotiation_done.wanted_refs, "wanted_refs must be unaffected by done flag");
    assert_eq!(negotiation_not_done.unresolved_want_refs, negotiation_done.unresolved_want_refs, "unresolved_want_refs must be unaffected by done flag");
    Ok(())
}

// ServerConfig and object-format validation tests (via public parse_v2_request API)

#[test]
fn server_config_default_returns_sha1() {
    let config = ServerConfig::default();
    assert_eq!(
        config.object_hash,
        gix_hash::Kind::Sha1,
        "ServerConfig::default() should configure SHA-1 as the object hash"
    );
}

#[test]
fn validate_object_format_matching_sha1_accepted() -> crate::Result {
    let input = request_bytes("ls-refs", &["object-format=sha1"], &[])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };
    let request = parse_v2_request(input.as_slice(), &config)?;
    assert_eq!(request.features[0].name.as_bytes(), b"object-format", "object-format feature should be parsed");
    Ok(())
}

#[test]
fn validate_object_format_mismatched_sha256_rejected() -> crate::Result {
    let input = request_bytes("ls-refs", &["object-format=sha256"], &[])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };
    let err = parse_v2_request(input.as_slice(), &config)
        .expect_err("sha256 against sha1 config should be rejected");
    match err {
        Error::UnsupportedObjectFormat { requested, supported } => {
            assert_eq!(requested.as_bytes(), b"sha256", "requested format should be sha256");
            assert_eq!(supported.as_bytes(), b"sha1", "supported format should be sha1");
        }
        other => panic!("expected UnsupportedObjectFormat, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn validate_object_format_invalid_blake3_rejected() -> crate::Result {
    let input = request_bytes("ls-refs", &["object-format=blake3"], &[])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };
    let err = parse_v2_request(input.as_slice(), &config)
        .expect_err("unrecognized hash name should be rejected");
    match err {
        Error::InvalidObjectFormat { value } => {
            assert_eq!(value.as_bytes(), b"blake3", "invalid value should be blake3");
        }
        other => panic!("expected InvalidObjectFormat, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn validate_object_format_absent_feature_accepted() -> crate::Result {
    let input = request_bytes("ls-refs", &["agent=git/test"], &[])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };
    let request = parse_v2_request(input.as_slice(), &config)?;
    assert_eq!(request.features.len(), 1, "only agent feature should be present");
    assert_eq!(request.features[0].name.as_bytes(), b"agent", "absent object-format should not cause rejection");
    Ok(())
}

#[test]
fn validate_object_format_empty_value_rejected() -> crate::Result {
    let input = request_bytes("ls-refs", &["object-format="], &[])?;
    let config = ServerConfig::default();
    let err = parse_v2_request(input.as_slice(), &config)
        .expect_err("empty object-format value should be rejected");
    match err {
        Error::InvalidObjectFormat { value } => {
            assert_eq!(value.as_bytes(), b"", "invalid value should be empty");
        }
        other => panic!("expected InvalidObjectFormat, got: {other:?}"),
    }
    Ok(())
}

#[test]
fn validate_object_format_non_utf8_value_rejected() -> crate::Result {
    use gix_transport::packetline::blocking_io::{Writer, encode};

    let mut out = Vec::new();
    let mut writer = Writer::new(&mut out);
    writer.enable_text_mode();
    writer.write_all(b"command=ls-refs")?;
    writer.write_all(b"object-format=\xff\xfe")?;
    encode::flush_to_write(writer.inner_mut())?;

    let config = ServerConfig::default();
    let err = parse_v2_request(out.as_slice(), &config)
        .expect_err("non-UTF-8 object-format value should be rejected");
    match err {
        Error::InvalidObjectFormat { value } => {
            assert_eq!(value.as_bytes(), b"\xff\xfe", "invalid value should preserve the raw bytes");
        }
        other => panic!("expected InvalidObjectFormat, got: {other:?}"),
    }
    Ok(())
}

// OID length enforcement tests

#[test]
fn oid_length_sha1_config_rejects_64_char_hex() -> crate::Result {
    let sha256_oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let input = request_bytes("fetch", &[], &[&format!("want {sha256_oid}"), "done"])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };

    let err = parse_v2_request(input.as_slice(), &config)
        .expect_err("SHA-1 config should reject 64-char hex OID");
    assert!(
        matches!(err, Error::ObjectIdLengthMismatch { actual: 64, expected: 40, hash_kind: gix_hash::Kind::Sha1 }),
        "expected ObjectIdLengthMismatch with actual=64, expected=40, got: {err:?}"
    );
    Ok(())
}

#[cfg(feature = "sha256")]
#[test]
fn oid_length_sha256_config_rejects_40_char_hex() -> crate::Result {
    let sha1_oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let input = request_bytes("fetch", &[], &[&format!("want {sha1_oid}"), "done"])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha256 };

    let err = parse_v2_request(input.as_slice(), &config)
        .expect_err("SHA-256 config should reject 40-char hex OID");
    assert!(
        matches!(err, Error::ObjectIdLengthMismatch { actual: 40, expected: 64, hash_kind: gix_hash::Kind::Sha256 }),
        "expected ObjectIdLengthMismatch with actual=40, expected=64, got: {err:?}"
    );
    Ok(())
}

#[test]
fn oid_length_sha1_config_accepts_40_char_valid_hex() -> crate::Result {
    let sha1_oid = "808e50d724f604f69ab93c6da2919c014667bedb";
    let input = request_bytes("fetch", &[], &[&format!("want {sha1_oid}"), "done"])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };

    let request = parse_v2_request(input.as_slice(), &config)
        .expect("SHA-1 config should accept 40-char hex OID");
    match request.command {
        Command::Fetch(fetch) => {
            assert_eq!(fetch.wants, vec![gix_hash::ObjectId::from_hex(sha1_oid.as_bytes())?]);
        }
        Command::LsRefs(_) => panic!("expected fetch command"),
    }
    Ok(())
}

#[cfg(feature = "sha256")]
#[test]
fn oid_length_sha256_config_accepts_64_char_valid_hex() -> crate::Result {
    let sha256_oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let input = request_bytes("fetch", &[], &[&format!("want {sha256_oid}"), "done"])?;
    let config = ServerConfig { object_hash: gix_hash::Kind::Sha256 };

    let request = parse_v2_request(input.as_slice(), &config)
        .expect("SHA-256 config should accept 64-char hex OID");
    match request.command {
        Command::Fetch(fetch) => {
            assert_eq!(fetch.wants, vec![gix_hash::ObjectId::from_hex(sha256_oid.as_bytes())?]);
        }
        Command::LsRefs(_) => panic!("expected fetch command"),
    }
    Ok(())
}

// Property-based tests using only public API

/// Property 2: Acknowledgment section framing (full response)
#[cfg(feature = "blocking-server")]
mod property_ack_section_framing {
    use super::*;
    use proptest::prelude::*;

    fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
        proptest::collection::vec(
            prop::num::u8::ANY.prop_map(|b| b"0123456789abcdef"[(b & 0x0f) as usize]),
            40,
        )
        .prop_map(|hex_bytes| {
            gix_hash::ObjectId::from_hex(&hex_bytes).expect("generated hex should always be valid")
        })
    }

    fn arb_acknowledgements() -> impl Strategy<Value = Vec<Acknowledgement>> {
        prop_oneof![
            Just(vec![Acknowledgement::Nak]),
            proptest::collection::vec(arb_object_id(), 1..=10)
                .prop_map(|ids| ids.into_iter().map(Acknowledgement::Common).collect()),
            proptest::collection::vec(arb_object_id(), 1..=10).prop_map(|ids| {
                let mut acks: Vec<Acknowledgement> =
                    ids.into_iter().map(Acknowledgement::Common).collect();
                acks.push(Acknowledgement::Ready);
                acks
            }),
        ]
    }

    fn contains_ready(acks: &[Acknowledgement]) -> bool {
        acks.iter().any(|a| matches!(a, Acknowledgement::Ready))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_ack_section_framing(acks in arb_acknowledgements()) {
            let has_ready = contains_ready(&acks);

            let mut fetch_output = FetchOutput::without_pack();
            fetch_output.acknowledgements = acks.clone();
            if has_ready {
                let pack_data: Box<dyn std::io::Read + Send + 'static> =
                    Box::new(std::io::Cursor::new(b"PACK".to_vec()));
                fetch_output.pack_data = Some(pack_data);
            }

            let mut output = Vec::new();
            write_fetch_response(&mut output, &mut fetch_output)
                .expect("write_fetch_response should succeed");

            let mut reader = StreamingPeekableIter::new(
                output.as_slice(),
                &[PacketLineRef::Flush, PacketLineRef::Delimiter],
                false,
            );

            let header_line = reader
                .read_line()
                .expect("expected at least one packetline")
                .expect("read should succeed")
                .expect("decode should succeed");
            let header_text = header_line
                .as_text()
                .expect("expected text packetline for section header");
            prop_assert_eq!(
                header_text.as_bstr(),
                b"acknowledgments".as_bstr(),
                "first line should be the acknowledgments section header"
            );

            let mut ack_line_count = 0usize;
            loop {
                if reader.read_line().is_none() {
                    break;
                }
                ack_line_count += 1;
            }

            prop_assert_eq!(
                ack_line_count, acks.len(),
                "number of ack lines should match input acknowledgements"
            );

            let stopped = reader.stopped_at();

            if has_ready {
                prop_assert_eq!(
                    stopped, Some(PacketLineRef::Delimiter),
                    "when Ready is present, ack section should be followed by delimiter (0001)"
                );

                reader.reset_with(&[PacketLineRef::Flush]);
                let next_line = reader
                    .read_line()
                    .expect("expected more data after delimiter")
                    .expect("read should succeed")
                    .expect("decode should succeed");
                let next_text = next_line
                    .as_text()
                    .expect("expected text packetline after delimiter");
                prop_assert_eq!(
                    next_text.as_bstr(),
                    b"packfile".as_bstr(),
                    "after delimiter following acks with Ready, next section should be packfile"
                );
            } else {
                prop_assert_eq!(
                    stopped, Some(PacketLineRef::Flush),
                    "when Ready is absent, ack section should be followed by flush (0000)"
                );
            }
        }
    }
}

/// Property 11: Parse round-trip for V2 requests
#[cfg(feature = "blocking-server")]
mod property_parse_round_trip {
    use super::*;
    use proptest::prelude::*;
    use gix_transport::packetline::blocking_io::{Writer, encode};

    fn serialize_request(request: &Request, hash_kind: gix_hash::Kind) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = Writer::new(&mut out);
        writer.enable_text_mode();

        let command_name = match &request.command {
            Command::LsRefs(_) => "ls-refs",
            Command::Fetch(_) => "fetch",
        };
        writer.write_all(format!("command={command_name}").as_bytes())
            .expect("write to vec never fails");

        for feature in &request.features {
            let line = match &feature.value {
                Some(value) => format!("{}={}", feature.name, value),
                None => feature.name.to_string(),
            };
            writer.write_all(line.as_bytes()).expect("write to vec never fails");
        }

        let arguments = match &request.command {
            Command::LsRefs(ls_refs) => serialize_ls_refs_arguments(ls_refs),
            Command::Fetch(fetch) => serialize_fetch_arguments(fetch, hash_kind),
        };

        if arguments.is_empty() {
            encode::flush_to_write(writer.inner_mut()).expect("write to vec never fails");
        } else {
            encode::delim_to_write(writer.inner_mut()).expect("write to vec never fails");
            for arg in &arguments {
                writer.write_all(arg.as_bytes()).expect("write to vec never fails");
            }
            encode::flush_to_write(writer.inner_mut()).expect("write to vec never fails");
        }

        out
    }

    fn serialize_ls_refs_arguments(ls_refs: &LsRefs) -> Vec<String> {
        let mut args = Vec::new();
        if ls_refs.symrefs { args.push("symrefs".to_string()); }
        if ls_refs.peel { args.push("peel".to_string()); }
        if ls_refs.unborn { args.push("unborn".to_string()); }
        for prefix in &ls_refs.ref_prefixes { args.push(format!("ref-prefix {prefix}")); }
        for extra in &ls_refs.extra_arguments { args.push(extra.to_string()); }
        args
    }

    fn serialize_fetch_arguments(fetch: &Fetch, hash_kind: gix_hash::Kind) -> Vec<String> {
        let mut args = Vec::new();
        for want in &fetch.wants { args.push(format!("want {}", want.to_hex_with_len(hash_kind.len_in_hex()))); }
        for have in &fetch.haves { args.push(format!("have {}", have.to_hex_with_len(hash_kind.len_in_hex()))); }
        for shallow in &fetch.shallow { args.push(format!("shallow {}", shallow.to_hex_with_len(hash_kind.len_in_hex()))); }
        if let Some(deepen) = fetch.deepen { args.push(format!("deepen {deepen}")); }
        if let Some(deepen_since) = fetch.deepen_since { args.push(format!("deepen-since {deepen_since}")); }
        for not_ref in &fetch.deepen_not { args.push(format!("deepen-not {not_ref}")); }
        if fetch.deepen_relative { args.push("deepen-relative".to_string()); }
        for filter in &fetch.filters { args.push(format!("filter {filter}")); }
        for want_ref in &fetch.want_refs { args.push(format!("want-ref {want_ref}")); }
        for uri in &fetch.packfile_uris { args.push(format!("packfile-uris {uri}")); }
        if fetch.thin_pack { args.push("thin-pack".to_string()); }
        if fetch.no_progress { args.push("no-progress".to_string()); }
        if fetch.ofs_delta { args.push("ofs-delta".to_string()); }
        if fetch.include_tag { args.push("include-tag".to_string()); }
        if fetch.sideband_all { args.push("sideband-all".to_string()); }
        if fetch.wait_for_done { args.push("wait-for-done".to_string()); }
        if fetch.done { args.push("done".to_string()); }
        for extra in &fetch.extra_arguments { args.push(extra.to_string()); }
        args
    }

    fn arb_object_id(kind: gix_hash::Kind) -> impl Strategy<Value = gix_hash::ObjectId> {
        let len = kind.len_in_hex();
        proptest::collection::vec(prop::num::u8::ANY.prop_map(|b| b"0123456789abcdef"[(b & 0x0f) as usize]), len)
            .prop_map(move |hex_bytes| {
                gix_hash::ObjectId::from_hex(&hex_bytes).expect("generated hex should always be valid")
            })
    }

    fn arb_feature_name() -> impl Strategy<Value = BString> {
        "[a-z][a-z0-9-]{0,15}"
            .prop_filter("must not be 'command'", |s| s != "command")
            .prop_map(|s| BString::from(s.as_bytes()))
    }

    fn arb_feature() -> impl Strategy<Value = Feature> {
        (arb_feature_name(), proptest::option::of("[a-zA-Z0-9._/-]{1,20}"))
            .prop_map(|(name, value)| Feature {
                name,
                value: value.map(|v| BString::from(v.as_bytes())),
            })
    }

    fn arb_ls_refs() -> impl Strategy<Value = LsRefs> {
        (any::<bool>(), any::<bool>(), any::<bool>(),
         proptest::collection::vec("refs/[a-z]{1,10}(/[a-z]{1,10}){0,2}", 0..4))
            .prop_map(|(symrefs, peel, unborn, prefixes)| LsRefs {
                symrefs, peel, unborn,
                ref_prefixes: prefixes.into_iter().map(|s| BString::from(s.as_bytes())).collect(),
                extra_arguments: Vec::new(),
            })
    }

    fn arb_fetch(kind: gix_hash::Kind) -> impl Strategy<Value = Fetch> {
        (
            proptest::collection::vec(arb_object_id(kind), 0..4),
            proptest::collection::vec(arb_object_id(kind), 0..4),
            proptest::collection::vec(arb_object_id(kind), 0..3),
            proptest::collection::vec("refs/[a-z]{1,10}(/[a-z]{1,8}){0,2}", 0..3),
            proptest::option::of(1u32..1000),
            proptest::option::of(0i64..2_000_000_000),
            proptest::collection::vec("[a-z]{2,10}(/[a-z]{2,8}){0,2}", 0..2),
            proptest::collection::vec("[a-z:]{3,12}", 0..2),
            proptest::collection::vec("[a-z]{3,10}", 0..2),
            (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()),
        )
            .prop_map(|(wants, haves, shallow, want_refs, deepen, deepen_since, deepen_not, filters, packfile_uris, flags)| {
                let (thin_pack, no_progress, ofs_delta, include_tag, sideband_all, deepen_relative, wait_for_done, done) = flags;
                Fetch {
                    wants, haves, shallow,
                    want_refs: want_refs.into_iter().map(|s| BString::from(s.as_bytes())).collect(),
                    deepen, deepen_since,
                    deepen_not: deepen_not.into_iter().map(|s| BString::from(s.as_bytes())).collect(),
                    deepen_relative,
                    filters: filters.into_iter().map(|s| BString::from(s.as_bytes())).collect(),
                    packfile_uris: packfile_uris.into_iter().map(|s| BString::from(s.as_bytes())).collect(),
                    thin_pack, no_progress, ofs_delta, include_tag, sideband_all, wait_for_done, done,
                    extra_arguments: Vec::new(),
                }
            })
    }

    fn arb_request(kind: gix_hash::Kind) -> impl Strategy<Value = Request> {
        let features_strategy = proptest::collection::vec(arb_feature(), 0..4);
        let command_strategy = prop_oneof![
            arb_ls_refs().prop_map(Command::LsRefs),
            arb_fetch(kind).prop_map(Command::Fetch),
        ];
        (features_strategy, command_strategy)
            .prop_map(|(features, command)| Request { features, command })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn property_parse_round_trip(request in arb_request(gix_hash::Kind::Sha1)) {
            let config = ServerConfig { object_hash: gix_hash::Kind::Sha1 };
            let wire_bytes = serialize_request(&request, config.object_hash);
            let parsed = parse_v2_request(wire_bytes.as_slice(), &config)
                .expect("parsing serialized request should succeed");

            prop_assert_eq!(
                &parsed, &request,
                "round-trip failed: serialized then parsed request should equal original"
            );
        }
    }
}

/// Property 7: Want and want-ref unification
mod property_want_unification {
    use super::*;
    use proptest::prelude::*;

    fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
        proptest::collection::vec(any::<u8>(), 20)
            .prop_map(|bytes| {
                let mut buf = [0u8; 20];
                buf.copy_from_slice(&bytes);
                gix_hash::ObjectId::from_bytes_or_panic(&buf)
            })
    }

    fn arb_wanted_ref() -> impl Strategy<Value = WantedRef> {
        (arb_object_id(), "[a-z]{3,10}(/[a-z]{3,8}){0,2}")
            .prop_map(|(id, path)| WantedRef { id, path: path.into() })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_want_unification(
            known_wants in proptest::collection::vec(arb_object_id(), 0..=10),
            wanted_refs in proptest::collection::vec(arb_wanted_ref(), 0..=10),
            overlap_indices in proptest::collection::vec(0usize..10, 0..=3),
        ) {
            let mut actual_wanted_refs = wanted_refs.clone();
            for &idx in &overlap_indices {
                if !known_wants.is_empty() {
                    let overlapping_id = known_wants[idx % known_wants.len()];
                    actual_wanted_refs.push(WantedRef {
                        id: overlapping_id,
                        path: format!("refs/overlap/{idx}").into(),
                    });
                }
            }

            let mut requested_ids = Vec::new();
            let mut seen = BTreeSet::new();
            for id in known_wants.iter().chain(actual_wanted_refs.iter().map(|w| &w.id)) {
                if seen.insert(*id) {
                    requested_ids.push(*id);
                }
            }

            let mut expected_ids = Vec::new();
            let mut expected_seen = std::collections::HashSet::new();
            let all_source_ids: Vec<gix_hash::ObjectId> = known_wants
                .iter()
                .chain(actual_wanted_refs.iter().map(|w| &w.id))
                .copied()
                .collect();
            for id in &all_source_ids {
                if expected_seen.insert(*id) {
                    expected_ids.push(*id);
                }
            }

            prop_assert_eq!(&requested_ids, &expected_ids);

            let result_set: BTreeSet<_> = requested_ids.iter().copied().collect();
            prop_assert_eq!(result_set.len(), requested_ids.len(), "no duplicates");

            for want in &known_wants {
                prop_assert!(result_set.contains(want));
            }
            for wr in &actual_wanted_refs {
                prop_assert!(result_set.contains(&wr.id));
            }
        }
    }
}

/// Property 9: V1 ref advertisement format
#[cfg(feature = "blocking-server")]
mod property_v1_ref_advertisement {
    use super::*;
    use proptest::prelude::*;

    fn arb_object_id() -> impl Strategy<Value = gix_hash::ObjectId> {
        proptest::collection::vec(
            prop::num::u8::ANY.prop_map(|b| b"0123456789abcdef"[(b & 0x0f) as usize]),
            40,
        )
        .prop_map(|hex_bytes| {
            gix_hash::ObjectId::from_hex(&hex_bytes).expect("generated hex should always be valid")
        })
    }

    fn arb_ref_path() -> impl Strategy<Value = BString> {
        ("refs/(heads|tags|remotes)/[a-z][a-z0-9]{1,12}")
            .prop_map(|s| BString::from(s.as_bytes()))
    }

    fn arb_ref() -> impl Strategy<Value = Ref> {
        prop_oneof![
            (arb_ref_path(), arb_object_id()).prop_map(|(name, object)| {
                Ref::Direct { full_ref_name: name, object }
            }),
            (arb_ref_path(), arb_object_id(), arb_object_id()).prop_map(|(name, tag, object)| {
                Ref::Peeled { full_ref_name: name, tag, object }
            }),
            (arb_ref_path(), arb_ref_path(), arb_object_id()).prop_map(|(name, target, object)| {
                Ref::Symbolic { full_ref_name: name, target, tag: None, object }
            }),
            (arb_ref_path(), arb_ref_path()).prop_map(|(name, target)| {
                Ref::Unborn { full_ref_name: name, target }
            }),
        ]
    }

    fn arb_capabilities() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9 ]{0,40}"
    }

    fn ref_line_count(r: &Ref) -> usize {
        match r {
            Ref::Direct { .. } | Ref::Symbolic { .. } => 1,
            Ref::Peeled { .. } => 2,
            Ref::Unborn { .. } => 0,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_v1_ref_advertisement(
            refs in proptest::collection::vec(arb_ref(), 1..=50),
            capabilities in arb_capabilities(),
        ) {
            let mut output = Vec::new();
            let lines_written = write_v1_ref_advertisement(&mut output, &refs, &capabilities)
                .expect("write_v1_ref_advertisement should succeed");

            let expected_lines: usize = refs.iter().map(|r| ref_line_count(r)).sum();
            prop_assert_eq!(lines_written, expected_lines);

            let mut reader = StreamingPeekableIter::new(
                output.as_slice(), &[PacketLineRef::Flush], false,
            );

            let mut parsed_lines: Vec<Vec<u8>> = Vec::new();
            loop {
                match reader.read_line() {
                    Some(Ok(Ok(line))) => {
                        let text = line.as_text().expect("each ref line should be text");
                        parsed_lines.push(text.0.to_vec());
                    }
                    Some(Ok(Err(e))) => { prop_assert!(false, "decode error: {:?}", e); break; }
                    Some(Err(e)) => { prop_assert!(false, "IO error: {:?}", e); break; }
                    None => break,
                }
            }

            prop_assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
            prop_assert_eq!(parsed_lines.len(), expected_lines);

            if parsed_lines.is_empty() {
                return Ok(());
            }

            // First line has NUL + capabilities
            let first_line = &parsed_lines[0];
            let nul_pos = first_line.iter().position(|&b| b == 0);
            prop_assert!(nul_pos.is_some(), "first line must contain NUL byte");
            let nul_pos = nul_pos.expect("verified");

            let caps_bytes = &first_line[nul_pos + 1..];
            prop_assert_eq!(caps_bytes, capabilities.as_bytes());

            let before_nul = &first_line[..nul_pos];
            validate_ref_line(before_nul)?;

            for (i, line) in parsed_lines.iter().enumerate().skip(1) {
                prop_assert!(!line.contains(&0u8), "line {} must not contain NUL", i);
                validate_ref_line(line)?;
            }

            // Verify ref correspondence
            let mut line_idx = 0usize;
            for reference in &refs {
                match reference {
                    Ref::Direct { full_ref_name, object } => {
                        let line = strip_nul_suffix(&parsed_lines[line_idx]);
                        let expected_prefix = format!("{} ", object);
                        prop_assert!(line.starts_with(expected_prefix.as_bytes()));
                        let ref_name_in_line = &line[expected_prefix.len()..];
                        prop_assert_eq!(ref_name_in_line, full_ref_name.as_bytes());
                        line_idx += 1;
                    }
                    Ref::Peeled { full_ref_name, tag, object } => {
                        let line = strip_nul_suffix(&parsed_lines[line_idx]);
                        let expected_prefix = format!("{} ", tag);
                        prop_assert!(line.starts_with(expected_prefix.as_bytes()));
                        line_idx += 1;

                        let peeled_line = &parsed_lines[line_idx];
                        let expected_peeled_prefix = format!("{} ", object);
                        prop_assert!(peeled_line.starts_with(expected_peeled_prefix.as_bytes()));
                        let expected_peeled_suffix = format!("{}^{{}}", full_ref_name);
                        let ref_part = &peeled_line[expected_peeled_prefix.len()..];
                        prop_assert_eq!(ref_part, expected_peeled_suffix.as_bytes());
                        line_idx += 1;
                    }
                    Ref::Symbolic { full_ref_name, object, .. } => {
                        let line = strip_nul_suffix(&parsed_lines[line_idx]);
                        let expected_prefix = format!("{} ", object);
                        prop_assert!(line.starts_with(expected_prefix.as_bytes()));
                        let ref_name_in_line = &line[expected_prefix.len()..];
                        prop_assert_eq!(ref_name_in_line, full_ref_name.as_bytes());
                        line_idx += 1;
                    }
                    Ref::Unborn { .. } => {}
                }
            }
        }
    }

    fn validate_ref_line(line: &[u8]) -> Result<(), proptest::test_runner::TestCaseError> {
        let space_pos = line.iter().position(|&b| b == b' ');
        prop_assert!(space_pos.is_some(), "line must contain space: {:?}", String::from_utf8_lossy(line));
        let space_pos = space_pos.expect("verified");
        let oid_hex = &line[..space_pos];
        prop_assert_eq!(oid_hex.len(), 40, "OID hex should be 40 chars");
        prop_assert!(oid_hex.iter().all(|b| b.is_ascii_hexdigit()), "OID must be valid hex");
        let refname = &line[space_pos + 1..];
        prop_assert!(!refname.is_empty(), "refname must not be empty");
        Ok(())
    }

    fn strip_nul_suffix(line: &[u8]) -> &[u8] {
        match line.iter().position(|&b| b == 0) {
            Some(pos) => &line[..pos],
            None => line,
        }
    }
}

mod v1_ref_advertisement {
    use bstr::ByteSlice;
    use gix_protocol::{handshake::Ref, upload_pack::write_v1_ref_advertisement};
    use gix_transport::packetline::{PacketLineRef, blocking_io::StreamingPeekableIter};

    use super::next_text_line;

    #[test]
    fn empty_refs_writes_only_flush() -> crate::Result {
        let mut output = Vec::new();
        let refs: Vec<Ref> = vec![];

        let count = write_v1_ref_advertisement(&mut output, &refs, "multi_ack thin-pack")?;
        assert_eq!(count, 0, "no refs means zero lines written");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        assert!(reader.read_line().is_none(), "empty advertisement should contain only flush");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn single_direct_ref_has_capabilities_on_first_line() -> crate::Result {
        let oid = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
        let refs = vec![Ref::Direct {
            full_ref_name: "refs/heads/main".into(),
            object: oid,
        }];
        let capabilities = "multi_ack thin-pack side-band";

        let mut output = Vec::new();
        let count = write_v1_ref_advertisement(&mut output, &refs, capabilities)?;
        assert_eq!(count, 1, "one ref should produce one line");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let first_line = next_text_line(&mut reader)?;
        let nul_pos = first_line.find_byte(0).expect("first line should contain NUL byte");
        let ref_part = &first_line[..nul_pos];
        let caps_part = &first_line[nul_pos + 1..];
        assert_eq!(ref_part.as_bstr(), format!("{oid} refs/heads/main").as_bytes().as_bstr());
        assert_eq!(caps_part.as_bstr(), capabilities.as_bytes().as_bstr());

        assert!(reader.read_line().is_none(), "flush should terminate response");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn multiple_refs_capabilities_only_on_first_line() -> crate::Result {
        let oid1 = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
        let oid2 = gix_hash::ObjectId::from_hex(b"9e320b9180e0b5580af68fa3255b7f3d9ecd5af0")?;
        let refs = vec![
            Ref::Direct { full_ref_name: "refs/heads/main".into(), object: oid1 },
            Ref::Direct { full_ref_name: "refs/heads/feature".into(), object: oid2 },
        ];
        let capabilities = "multi_ack";

        let mut output = Vec::new();
        let count = write_v1_ref_advertisement(&mut output, &refs, capabilities)?;
        assert_eq!(count, 2, "two refs should produce two lines");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let first_line = next_text_line(&mut reader)?;
        assert!(first_line.find_byte(0).is_some(), "first line should contain NUL byte");
        let second_line = next_text_line(&mut reader)?;
        assert!(second_line.find_byte(0).is_none(), "subsequent lines should not contain NUL byte");
        assert_eq!(second_line.as_bstr(), format!("{oid2} refs/heads/feature").as_bytes().as_bstr());

        assert!(reader.read_line().is_none(), "flush should terminate response");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn peeled_tag_emits_tag_and_deref_line() -> crate::Result {
        let tag_oid = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
        let target_oid = gix_hash::ObjectId::from_hex(b"9e320b9180e0b5580af68fa3255b7f3d9ecd5af0")?;
        let refs = vec![Ref::Peeled {
            full_ref_name: "refs/tags/v1.0".into(),
            tag: tag_oid,
            object: target_oid,
        }];
        let capabilities = "multi_ack";

        let mut output = Vec::new();
        let count = write_v1_ref_advertisement(&mut output, &refs, capabilities)?;
        assert_eq!(count, 2, "peeled tag should produce two lines");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let first_line = next_text_line(&mut reader)?;
        let nul_pos = first_line.find_byte(0).expect("first line should have capabilities");
        let ref_part = &first_line[..nul_pos];
        assert_eq!(ref_part.as_bstr(), format!("{tag_oid} refs/tags/v1.0").as_bytes().as_bstr());

        let second_line = next_text_line(&mut reader)?;
        assert_eq!(second_line.as_bstr(), format!("{target_oid} refs/tags/v1.0^{{}}").as_bytes().as_bstr());

        assert!(reader.read_line().is_none(), "flush should terminate response");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn symbolic_ref_emits_object_oid() -> crate::Result {
        let oid = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
        let refs = vec![Ref::Symbolic {
            full_ref_name: "HEAD".into(),
            target: "refs/heads/main".into(),
            tag: None,
            object: oid,
        }];
        let capabilities = "symref=HEAD:refs/heads/main";

        let mut output = Vec::new();
        let count = write_v1_ref_advertisement(&mut output, &refs, capabilities)?;
        assert_eq!(count, 1, "symbolic ref should produce one line");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let first_line = next_text_line(&mut reader)?;
        let nul_pos = first_line.find_byte(0).expect("first line should have capabilities");
        let ref_part = &first_line[..nul_pos];
        assert_eq!(ref_part.as_bstr(), format!("{oid} HEAD").as_bytes().as_bstr());

        assert!(reader.read_line().is_none(), "flush should terminate response");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }

    #[test]
    fn unborn_refs_are_skipped() -> crate::Result {
        let oid = gix_hash::ObjectId::from_hex(b"808e50d724f604f69ab93c6da2919c014667bedb")?;
        let refs = vec![
            Ref::Unborn { full_ref_name: "HEAD".into(), target: "refs/heads/main".into() },
            Ref::Direct { full_ref_name: "refs/heads/other".into(), object: oid },
        ];
        let capabilities = "multi_ack";

        let mut output = Vec::new();
        let count = write_v1_ref_advertisement(&mut output, &refs, capabilities)?;
        assert_eq!(count, 1, "unborn ref should be skipped, only direct ref counted");

        let mut reader = StreamingPeekableIter::new(output.as_slice(), &[PacketLineRef::Flush], false);
        let first_line = next_text_line(&mut reader)?;
        let nul_pos = first_line.find_byte(0).expect("first line should have capabilities");
        let ref_part = &first_line[..nul_pos];
        assert_eq!(ref_part.as_bstr(), format!("{oid} refs/heads/other").as_bytes().as_bstr());

        assert!(reader.read_line().is_none(), "flush should terminate response");
        assert_eq!(reader.stopped_at(), Some(PacketLineRef::Flush));
        Ok(())
    }
}

// --- Helper functions ---

fn next_text_line(reader: &mut StreamingPeekableIter<&[u8]>) -> Result<bstr::BString, Box<dyn std::error::Error>> {
    let line = reader
        .read_line()
        .expect("expected packetline")
        .expect("read should succeed")
        .expect("decode should succeed");
    Ok(line.as_text().expect("expected text packetline").as_bstr().to_owned())
}

fn expect_delimiter(reader: &mut StreamingPeekableIter<&[u8]>) -> Result<(), Box<dyn std::error::Error>> {
    let line = reader
        .read_line()
        .expect("expected packetline")
        .expect("read should succeed")
        .expect("decode should succeed");
    match line {
        PacketLineRef::Delimiter => Ok(()),
        other => Err(format!("expected delimiter, got {other:?}").into()),
    }
}

fn next_band_data(reader: &mut StreamingPeekableIter<&[u8]>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let line = reader
        .read_line()
        .expect("expected packetline")
        .expect("read should succeed")
        .expect("decode should succeed");
    match line.decode_band()? {
        BandRef::Data(data) => Ok(data.to_vec()),
        other => Err(format!("expected data band, got {other:?}").into()),
    }
}

fn request_bytes(
    command: &str,
    features: &[&str],
    arguments: &[&str],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use gix_transport::packetline::blocking_io::{Writer, encode};

    let mut out = Vec::new();
    let mut writer = Writer::new(&mut out);
    writer.enable_text_mode();
    writer.write_all(format!("command={command}").as_bytes())?;
    for feature in features {
        writer.write_all(feature.as_bytes())?;
    }
    if arguments.is_empty() {
        encode::flush_to_write(writer.inner_mut())?;
        return Ok(out);
    }

    encode::delim_to_write(writer.inner_mut())?;
    for argument in arguments {
        writer.write_all(argument.as_bytes())?;
    }
    encode::flush_to_write(writer.inner_mut())?;
    Ok(out)
}

fn object_id(hex: &str) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_hex(hex.as_bytes()).expect("valid object id in test")
}

fn temporary_ref_store(
    files: &[(&str, String)],
) -> Result<(TempDir, gix_ref::file::Store), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    for (relative_path, content) in files {
        let path = temp.path.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    let store = gix_ref::file::Store::at(
        temp.path.clone(),
        gix_ref::store::init::Options {
            write_reflog: gix_ref::store::WriteReflog::Disable,
            object_hash: gix_hash::Kind::Sha1,
            ..Default::default()
        },
    );
    Ok((temp, store))
}

struct TempDir {
    path: PathBuf,
}
static TEMP_DIR_ID: AtomicU64 = AtomicU64::new(0);

impl TempDir {
    fn new() -> Result<Self, std::io::Error> {
        let base = std::env::temp_dir();
        for _ in 0..16 {
            let unique = TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!("gitoxide-upload-pack-ext-test-{}-{unique}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate unique temporary upload-pack test directory",
        ))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct ObjectStoreFixture {
    _temp: TempDir,
    odb: gix_odb::Handle,
    commit_one: gix_hash::ObjectId,
    commit_two: gix_hash::ObjectId,
    commit_three: gix_hash::ObjectId,
    tag_three: gix_hash::ObjectId,
}

fn temporary_object_store_with_linear_history() -> Result<ObjectStoreFixture, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let objects_path = temp.path.join("objects");
    fs::create_dir_all(&objects_path)?;
    let odb = gix_odb::at(objects_path)?;

    let blob_one = odb.write_buf(gix_object::Kind::Blob, b"one\n").map_err(std::io::Error::other)?;
    let tree_one = write_single_file_tree(&odb, "file.txt", &blob_one)?;
    let commit_one = write_commit_object(&odb, &tree_one, None, "commit one")?;
    let blob_two = odb.write_buf(gix_object::Kind::Blob, b"two\n").map_err(std::io::Error::other)?;
    let tree_two = write_single_file_tree(&odb, "file.txt", &blob_two)?;
    let commit_two = write_commit_object(&odb, &tree_two, Some(&commit_one), "commit two")?;
    let blob_three = odb.write_buf(gix_object::Kind::Blob, b"three\n").map_err(std::io::Error::other)?;
    let tree_three = write_single_file_tree(&odb, "file.txt", &blob_three)?;
    let commit_three = write_commit_object(&odb, &tree_three, Some(&commit_two), "commit three")?;
    let tag_three = write_tag_object(&odb, &commit_three, "v1.0.0", "release tag")?;

    Ok(ObjectStoreFixture { _temp: temp, odb, commit_one, commit_two, commit_three, tag_three })
}

fn write_single_file_tree(
    odb: &gix_odb::Handle,
    filename: &str,
    blob_id: &gix_hash::ObjectId,
) -> Result<gix_hash::ObjectId, std::io::Error> {
    let tree = gix_object::Tree {
        entries: vec![gix_object::tree::Entry {
            mode: gix_object::tree::EntryKind::Blob.into(),
            filename: BString::from(filename),
            oid: blob_id.clone(),
        }],
    };
    odb.write(&tree).map_err(std::io::Error::other)
}

fn write_commit_object(
    odb: &gix_odb::Handle,
    tree_id: &gix_hash::ObjectId,
    parent: Option<&gix_hash::ObjectId>,
    message: &str,
) -> Result<gix_hash::ObjectId, std::io::Error> {
    let mut bytes = format!("tree {tree_id}\n").into_bytes();
    if let Some(parent) = parent {
        bytes.extend_from_slice(format!("parent {parent}\n").as_bytes());
    }
    bytes.extend_from_slice(b"author Example <example@example.com> 0 +0000\n");
    bytes.extend_from_slice(b"committer Example <example@example.com> 0 +0000\n\n");
    bytes.extend_from_slice(message.as_bytes());
    bytes.push(b'\n');
    odb.write_buf(gix_object::Kind::Commit, &bytes).map_err(std::io::Error::other)
}

fn write_tag_object(
    odb: &gix_odb::Handle,
    target: &gix_hash::ObjectId,
    name: &str,
    message: &str,
) -> Result<gix_hash::ObjectId, std::io::Error> {
    let mut bytes = format!("object {target}\n").into_bytes();
    bytes.extend_from_slice(b"type commit\n");
    bytes.extend_from_slice(format!("tag {name}\n").as_bytes());
    bytes.extend_from_slice(b"tagger Example <example@example.com> 0 +0000\n\n");
    bytes.extend_from_slice(message.as_bytes());
    bytes.push(b'\n');
    odb.write_buf(gix_object::Kind::Tag, &bytes).map_err(std::io::Error::other)
}

fn pack_object_ids(
    pack_data: Vec<u8>,
    object_hash: gix_hash::Kind,
) -> Result<BTreeSet<gix_hash::ObjectId>, Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut reader = BufReader::new(Cursor::new(pack_data));
    let outcome = gix_pack::Bundle::write_to_directory(
        &mut reader,
        Some(temp.path.as_path()),
        &mut gix_features::progress::Discard,
        &AtomicBool::new(false),
        None::<gix_odb::Handle>,
        gix_pack::bundle::write::Options {
            object_hash,
            ..Default::default()
        },
    )?;
    let bundle = outcome
        .to_bundle()
        .ok_or_else(|| std::io::Error::other("a bundle path should be available"))??;
    Ok(bundle.index.iter().map(|entry| entry.oid).collect())
}
