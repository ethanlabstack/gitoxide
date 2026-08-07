//! Integration tests for the receive-pack handler.
//!
//! Tests pack ingestion, connectivity checking, ref transactions, state machine
//! behavior, the Delegate trait implementation, and end-to-end push round-trips.

use std::path::{Path, PathBuf};

use gix_protocol::receive_pack::handler::{
    ConnectivityError, OpenError, Options, ReceivePackHandler, SessionState, TransactError,
};
use gix_protocol::receive_pack::Update;

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Create a minimal bare repository structure at the given path.
/// This creates the `objects/` and `objects/pack/` subdirectories
/// and a `HEAD` file, which is the minimum needed for handler construction.
fn create_bare_repo(path: &Path) {
    let objects_dir = path.join("objects");
    std::fs::create_dir_all(objects_dir.join("pack"))
        .expect("should be able to create objects/pack directory");
    std::fs::write(path.join("HEAD"), "ref: refs/heads/main\n")
        .expect("should be able to write HEAD");
}

/// Create a bare repo with `git init --bare` and return the path.
fn git_init_bare(parent: &Path, name: &str) -> PathBuf {
    let repo_path = parent.join(name);
    let output = std::process::Command::new("git")
        .args(["init", "--bare"])
        .arg(&repo_path)
        .output()
        .expect("git should be available");
    assert!(
        output.status.success(),
        "git init --bare should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    repo_path
}

/// Run a git command in the given repo, returning stdout as String.
fn git_in(repo: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(repo)
        .env("GIT_DIR", repo)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("git command should execute");
    assert!(
        output.status.success(),
        "git {:?} should succeed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output should be valid utf-8")
        .trim()
        .to_string()
}

/// Write a blob and return its oid.
fn write_blob(repo: &Path, content: &[u8]) -> String {
    let mut child = std::process::Command::new("git")
        .args(["hash-object", "-w", "--stdin"])
        .current_dir(repo)
        .env("GIT_DIR", repo)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("git hash-object should spawn");
    {
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin available")
            .write_all(content)
            .expect("write to stdin should succeed");
    }
    let output = child.wait_with_output().expect("git hash-object should complete");
    assert!(
        output.status.success(),
        "git hash-object should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("oid should be valid utf-8")
        .trim()
        .to_string()
}

/// Create a commit in a bare repo using low-level git commands.
/// Returns (commit_oid, tree_oid).
fn create_commit(
    repo: &Path,
    blob_content: &[u8],
    filename: &str,
    parent: Option<&str>,
    message: &str,
) -> (String, String) {
    let blob_oid = write_blob(repo, blob_content);

    // Create a tree with the blob
    let tree_input = format!("100644 blob {}\t{}\n", blob_oid, filename);
    let mut child = std::process::Command::new("git")
        .args(["mktree"])
        .current_dir(repo)
        .env("GIT_DIR", repo)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("git mktree should spawn");
    {
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin available")
            .write_all(tree_input.as_bytes())
            .expect("write to mktree stdin should succeed");
    }
    let output = child.wait_with_output().expect("git mktree should complete");
    assert!(
        output.status.success(),
        "git mktree should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree_oid = String::from_utf8(output.stdout)
        .expect("tree oid should be valid utf-8")
        .trim()
        .to_string();

    // Create the commit
    let mut args = vec!["commit-tree", &tree_oid, "-m", message];
    let parent_flag;
    if let Some(p) = parent {
        parent_flag = p.to_string();
        args.push("-p");
        args.push(&parent_flag);
    }
    let commit_oid = git_in(repo, &args);

    (commit_oid, tree_oid)
}

/// Create a pack from a list of OIDs (treated as revisions via --revs)
/// and return the pack bytes.
fn pack_objects(repo: &Path, revs: &[&str]) -> Vec<u8> {
    let input = revs.join("\n") + "\n";
    let mut child = std::process::Command::new("git")
        .args(["pack-objects", "--stdout", "--revs"])
        .current_dir(repo)
        .env("GIT_DIR", repo)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("git pack-objects should spawn");
    {
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin available")
            .write_all(input.as_bytes())
            .expect("write to pack-objects stdin should succeed");
    }
    let output = child.wait_with_output().expect("git pack-objects should complete");
    assert!(
        output.status.success(),
        "git pack-objects --revs should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

/// Create a pack from revisions with exclusions (e.g. "commit\n^excluded\n").
fn pack_objects_with_exclusions(repo: &Path, input_str: &str) -> Vec<u8> {
    let mut child = std::process::Command::new("git")
        .args(["pack-objects", "--stdout", "--revs"])
        .current_dir(repo)
        .env("GIT_DIR", repo)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("git pack-objects should spawn");
    {
        use std::io::Write;
        child
            .stdin
            .take()
            .expect("stdin available")
            .write_all(input_str.as_bytes())
            .expect("write to pack-objects stdin should succeed");
    }
    let output = child.wait_with_output().expect("git pack-objects should complete");
    assert!(
        output.status.success(),
        "git pack-objects --revs should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

// ---------------------------------------------------------------------------
// open_* tests
// ---------------------------------------------------------------------------

#[test]
fn open_with_valid_bare_repo_path_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    create_bare_repo(tmp.path());

    let handler = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default())?;

    assert_eq!(
        handler.objects_dir(),
        tmp.path().join("objects"),
        "objects directory should be at repo_path/objects/"
    );
    assert_eq!(
        handler.state(),
        SessionState::Fresh,
        "newly opened handler should be in Fresh state"
    );
    Ok(())
}

#[test]
fn open_with_non_existent_path_returns_invalid_path_error() {
    let path = PathBuf::from("/nonexistent/path/that/does/not/exist");
    let result = ReceivePackHandler::open(path.clone(), Options::default());

    match result {
        Err(OpenError::InvalidPath { path: err_path }) => {
            assert_eq!(err_path, path, "error should contain the invalid path");
        }
        Err(other) => panic!("expected OpenError::InvalidPath, got different error: {other}"),
        Ok(_) => panic!("expected OpenError::InvalidPath, but open succeeded"),
    }
}

#[test]
fn open_with_path_missing_objects_dir_still_constructs() -> Result<(), Box<dyn std::error::Error>> {
    // gix_odb::Store::at_opts requires the objects directory to exist as a directory.
    // When the objects/ subdirectory does not exist, `open` should return an Odb error.
    let tmp = tempfile::tempdir()?;
    // Create a directory but don't create objects/ inside it.
    // Just create HEAD so it looks like a repo directory without an ODB.
    std::fs::write(tmp.path().join("HEAD"), "ref: refs/heads/main\n")?;

    let result = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default());

    match result {
        Err(OpenError::Odb { path, .. }) => {
            assert_eq!(
                path,
                tmp.path().join("objects"),
                "error should reference the missing objects directory"
            );
        }
        Err(other) => panic!("expected OpenError::Odb for missing objects dir, got different error: {other}"),
        Ok(_) => panic!("expected OpenError::Odb, but open succeeded"),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pack ingestion properties
// ---------------------------------------------------------------------------

mod pack_ingestion_properties {
    use super::*;
    use proptest::prelude::*;

    // Feature: async-receive-pack-handler, Property 2: Malformed pack produces error with no filesystem artifacts
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn malformed_pack_produces_no_artifacts(
            random_bytes in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory for test");
            create_bare_repo(tmp.path());

            let pack_dir = tmp.path().join("objects").join("pack");

            // Snapshot existing files in pack dir before ingestion attempt
            let files_before: std::collections::HashSet<_> = std::fs::read_dir(&pack_dir)
                .expect("pack dir should exist")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();

            let mut handler = ReceivePackHandler::open(
                tmp.path().to_path_buf(),
                Options::default(),
            ).expect("handler should open successfully for a valid bare repo");

            let mut cursor = std::io::Cursor::new(&random_bytes);
            let result = handler.ingest_pack(&mut cursor);

            // Must return an error for random bytes
            prop_assert!(
                result.is_err(),
                "ingest_pack should return an error for random/malformed bytes, got: {:?}",
                result
            );

            // Assert no new .pack, .idx, or .keep files were created
            let files_after: std::collections::HashSet<_> = std::fs::read_dir(&pack_dir)
                .expect("pack dir should still exist")
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();

            let new_files: Vec<_> = files_after.difference(&files_before)
                .filter(|p| {
                    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
                    ext == "pack" || ext == "idx" || ext == "keep"
                })
                .collect();

            prop_assert!(
                new_files.is_empty(),
                "no .pack, .idx, or .keep files should be created after a failed ingestion, found: {:?}",
                new_files
            );
        }
    }

    // Feature: async-receive-pack-handler, Property 4: Non-empty pack ingestion produces complete file set
    #[test]
    fn non_empty_pack_produces_complete_file_set() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo_path = tmp.path().join("test.git");

        let status = std::process::Command::new("git")
            .args(["init", "--bare"])
            .arg(&repo_path)
            .output()
            .expect("git should be available to create test fixtures");
        assert!(
            status.status.success(),
            "git init --bare should succeed: {}",
            String::from_utf8_lossy(&status.stderr)
        );

        // Write a blob object into the bare repo
        let hash_output = std::process::Command::new("git")
            .args(["hash-object", "-w", "--stdin"])
            .current_dir(&repo_path)
            .env("GIT_DIR", &repo_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin should be available")
                    .write_all(b"hello world\n")
                    .expect("write to stdin should succeed");
                child.wait_with_output()
            })
            .expect("git hash-object should succeed");
        assert!(
            hash_output.status.success(),
            "git hash-object should succeed: {}",
            String::from_utf8_lossy(&hash_output.stderr)
        );
        let blob_oid = String::from_utf8(hash_output.stdout)
            .expect("hash output should be valid utf-8")
            .trim()
            .to_string();

        // Create a pack file from that object using git pack-objects
        let pack_output = std::process::Command::new("git")
            .args(["pack-objects", "--stdout"])
            .current_dir(&repo_path)
            .env("GIT_DIR", &repo_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin should be available")
                    .write_all(format!("{}\n", blob_oid).as_bytes())
                    .expect("write oid to stdin should succeed");
                child.wait_with_output()
            })
            .expect("git pack-objects should succeed");
        assert!(
            pack_output.status.success(),
            "git pack-objects should succeed: {}",
            String::from_utf8_lossy(&pack_output.stderr)
        );
        let pack_bytes = pack_output.stdout;
        assert!(
            !pack_bytes.is_empty(),
            "pack-objects should produce non-empty output"
        );

        // Now create a fresh bare repo for ingestion (to avoid the existing loose object)
        let ingest_tmp = tempfile::tempdir()?;
        let ingest_repo = ingest_tmp.path().join("ingest.git");
        let status = std::process::Command::new("git")
            .args(["init", "--bare"])
            .arg(&ingest_repo)
            .output()
            .expect("git should be available");
        assert!(
            status.status.success(),
            "git init --bare for ingest repo should succeed"
        );

        let mut handler = ReceivePackHandler::open(
            ingest_repo.clone(),
            Options::default(),
        )?;

        let mut cursor = std::io::Cursor::new(&pack_bytes);
        let outcome = handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Assert data_path, index_path, and keep_path are all Some
        let data_path = outcome
            .outcome
            .data_path
            .as_ref()
            .expect("data_path should be Some for a non-empty pack");
        let index_path = outcome
            .outcome
            .index_path
            .as_ref()
            .expect("index_path should be Some for a non-empty pack");
        let keep_path = outcome
            .outcome
            .keep_path
            .as_ref()
            .expect("keep_path should be Some for a non-empty pack");

        // Assert all files exist on disk
        assert!(
            data_path.exists(),
            "pack data file should exist on disk at {:?}",
            data_path
        );
        assert!(
            index_path.exists(),
            "pack index file should exist on disk at {:?}",
            index_path
        );
        assert!(
            keep_path.exists(),
            "keep file should exist on disk at {:?}",
            keep_path
        );

        // Verify object count is at least 1
        assert!(
            outcome.object_count >= 1,
            "object count should be at least 1 for a non-empty pack, got {}",
            outcome.object_count
        );

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 3: Thin pack with resolvable bases ingests successfully
    #[test]
    fn thin_pack_with_resolvable_bases_ingests_successfully() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo_path = tmp.path().join("base.git");

        let status = std::process::Command::new("git")
            .args(["init", "--bare"])
            .arg(&repo_path)
            .output()
            .expect("git should be available");
        assert!(
            status.status.success(),
            "git init --bare should succeed: {}",
            String::from_utf8_lossy(&status.stderr)
        );

        // Write a base blob into the repository
        let base_content = b"this is the base content for thin pack testing\n";
        let _base_oid = write_blob(&repo_path, base_content);

        // Write a similar blob that will produce a delta against the base
        let delta_content = b"this is the base content for thin pack testing\nwith an extra line\n";
        let delta_oid = write_blob(&repo_path, delta_content);

        // Create a thin pack containing only the delta object
        let pack_output = std::process::Command::new("git")
            .args(["pack-objects", "--stdout", "--thin"])
            .current_dir(&repo_path)
            .env("GIT_DIR", &repo_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .take()
                    .expect("stdin should be available")
                    .write_all(format!("{}\n", delta_oid).as_bytes())
                    .expect("write oid to stdin should succeed");
                child.wait_with_output()
            })
            .expect("git pack-objects --thin should succeed");
        assert!(
            pack_output.status.success(),
            "git pack-objects --thin should succeed: {}",
            String::from_utf8_lossy(&pack_output.stderr)
        );
        let thin_pack_bytes = pack_output.stdout;
        assert!(
            !thin_pack_bytes.is_empty(),
            "thin pack should produce non-empty output"
        );

        // Open the handler on the SAME repo (which has the base object in its ODB).
        let mut handler = ReceivePackHandler::open(
            repo_path.clone(),
            Options::default(),
        )?;

        let mut cursor = std::io::Cursor::new(&thin_pack_bytes);
        let outcome = handler.ingest_pack(&mut cursor);

        match outcome {
            Ok(ingest_outcome) => {
                assert!(
                    ingest_outcome.object_count >= 1,
                    "ingested pack should have at least 1 object, got {}",
                    ingest_outcome.object_count
                );
            }
            Err(e) => {
                panic!(
                    "ingest_pack should succeed for a thin pack with resolvable bases, got error: {e}"
                );
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Connectivity properties
// ---------------------------------------------------------------------------

mod connectivity_properties {
    use super::*;

    // Feature: async-receive-pack-handler, Property 5: Connectivity check succeeds on complete object graphs
    #[test]
    fn connectivity_check_succeeds_on_complete_object_graphs() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        // Create a source bare repo with a commit
        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _tree_oid) =
            create_commit(&source, b"hello world\n", "file.txt", None, "initial commit");

        // Create a pack containing the commit and all reachable objects
        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        // Create a destination bare repo and ingest
        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Build an update pointing to the new commit
        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let update = Update {
            old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            new_id,
            ref_name: "refs/heads/main".into(),
        };

        let result = handler.check_connectivity(&[update]);
        assert!(
            result.is_ok(),
            "connectivity check should succeed on a complete object graph, got: {:?}",
            result.err()
        );

        let connectivity_result = result.expect("already asserted Ok");
        assert!(
            connectivity_result.new_objects.contains(&new_id),
            "new_objects should contain the pushed commit"
        );

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 7: Deletion updates are excluded from connectivity checking
    #[test]
    fn deletion_updates_are_excluded_from_connectivity_checking() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        // Create a source repo with a commit so we have a valid pack to ingest
        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _tree_oid) =
            create_commit(&source, b"content\n", "a.txt", None, "first");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        // Ingest into a destination repo
        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed");

        // Build updates: one valid creation, and one deletion (new_id = null).
        let valid_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);

        let updates = vec![
            Update {
                old_id: null_id,
                new_id: valid_id,
                ref_name: "refs/heads/main".into(),
            },
            Update {
                old_id: valid_id,
                new_id: null_id,
                ref_name: "refs/heads/to-delete".into(),
            },
            Update {
                old_id: gix_hash::ObjectId::from_hex(
                    b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                )
                .expect("hex should parse"),
                new_id: null_id,
                ref_name: "refs/heads/another-delete".into(),
            },
        ];

        let result = handler.check_connectivity(&updates);
        assert!(
            result.is_ok(),
            "connectivity check should succeed when deletion updates are present, got: {:?}",
            result.err()
        );

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 8: Submodule tree entries do not trigger missing-object errors
    #[test]
    fn submodule_tree_entries_do_not_trigger_missing_object_errors() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let blob_oid = write_blob(&source, b"regular file content\n");

        // Fabricate a submodule commit oid that does NOT exist anywhere
        let fake_submodule_oid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        // Create a tree with both a regular blob and a gitlink (submodule) entry.
        let tree_input = format!(
            "100644 blob {}\tfile.txt\n160000 commit {}\tmy-submodule\n",
            blob_oid, fake_submodule_oid
        );
        let mut child = std::process::Command::new("git")
            .args(["mktree"])
            .current_dir(&source)
            .env("GIT_DIR", &source)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("git mktree should spawn");
        {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("stdin available")
                .write_all(tree_input.as_bytes())
                .expect("write to mktree should succeed");
        }
        let output = child.wait_with_output().expect("git mktree should complete");
        assert!(
            output.status.success(),
            "git mktree should succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let tree_oid = String::from_utf8(output.stdout)
            .expect("tree oid should be valid utf-8")
            .trim()
            .to_string();

        // Create a commit pointing to this tree
        let commit_oid = git_in(&source, &["commit-tree", &tree_oid, "-m", "commit with submodule"]);

        // Pack the commit + tree + blob (but NOT the fake submodule commit)
        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        // Ingest into a destination repo
        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed");

        // Check connectivity — should succeed because the gitlink entry is skipped
        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let update = Update {
            old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            new_id,
            ref_name: "refs/heads/main".into(),
        };

        let result = handler.check_connectivity(&[update]);
        assert!(
            result.is_ok(),
            "connectivity check should succeed even with submodule entries pointing to \
             non-existent commits, got: {:?}",
            result.err()
        );

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 6: Connectivity walk terminates at pre-existing ref tips
    #[test]
    fn connectivity_walk_terminates_at_pre_existing_ref_tips() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo = git_init_bare(tmp.path(), "repo.git");

        // Create a chain: A → B → C
        let (commit_a, _) = create_commit(&repo, b"a\n", "a.txt", None, "commit A");
        let (commit_b, _) = create_commit(&repo, b"b\n", "b.txt", Some(&commit_a), "commit B");
        let (commit_c, _) = create_commit(&repo, b"c\n", "c.txt", Some(&commit_b), "commit C");

        // Point refs/heads/main at C so it becomes a pre-existing tip
        git_in(&repo, &["update-ref", "refs/heads/main", &commit_c]);

        // Create commit D with parent C
        let (commit_d, _) = create_commit(&repo, b"d\n", "d.txt", Some(&commit_c), "commit D");

        // Create a pack containing ONLY commit D (and its tree/blob).
        let rev_input = format!("{}\n^{}\n", commit_d, commit_c);
        let pack_bytes = pack_objects_with_exclusions(&repo, &rev_input);

        // Now open a handler on the same repo (which already has A, B, C)
        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for the incremental pack");

        // Check connectivity for D
        let d_id = gix_hash::ObjectId::from_hex(commit_d.as_bytes())
            .expect("commit D oid should be valid hex");
        let c_id = gix_hash::ObjectId::from_hex(commit_c.as_bytes())
            .expect("commit C oid should be valid hex");
        let b_id = gix_hash::ObjectId::from_hex(commit_b.as_bytes())
            .expect("commit B oid should be valid hex");
        let a_id = gix_hash::ObjectId::from_hex(commit_a.as_bytes())
            .expect("commit A oid should be valid hex");

        let update = Update {
            old_id: c_id,
            new_id: d_id,
            ref_name: "refs/heads/main".into(),
        };

        let result = handler.check_connectivity(&[update]);
        assert!(
            result.is_ok(),
            "connectivity check should succeed, got: {:?}",
            result.err()
        );

        let connectivity_result = result.expect("already asserted Ok");

        assert!(
            connectivity_result.new_objects.contains(&d_id),
            "new_objects should contain commit D"
        );
        assert!(
            !connectivity_result.new_objects.contains(&c_id),
            "new_objects should NOT contain commit C (pre-existing tip)"
        );
        assert!(
            !connectivity_result.new_objects.contains(&b_id),
            "new_objects should NOT contain commit B (ancestor of pre-existing tip)"
        );
        assert!(
            !connectivity_result.new_objects.contains(&a_id),
            "new_objects should NOT contain commit A (ancestor of pre-existing tip)"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transaction properties
// ---------------------------------------------------------------------------

mod transaction_properties {
    use super::*;

    // Feature: async-receive-pack-handler, Property 9: Successful ref transaction removes the .keep file
    #[test]
    fn successful_transaction_removes_keep_file() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _) = create_commit(&source, b"content for prop9\n", "file.txt", None, "test commit");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        handler.disable_reflog();

        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Verify that the .keep file was created
        let keep_path = handler
            .ingest_pack_keep_path()
            .expect("keep_path should be available after successful ingestion")
            .to_path_buf();
        assert!(
            keep_path.exists(),
            ".keep file should exist after pack ingestion at {:?}",
            keep_path
        );

        // Now call transact_refs with a creation update
        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let update = Update {
            old_id: null_id,
            new_id,
            ref_name: "refs/heads/main".into(),
        };

        let result = handler.transact_refs(&[update]);
        assert!(
            result.is_ok(),
            "transact_refs should succeed for a valid creation update, got: {:?}",
            result.err()
        );

        // Assert the .keep file no longer exists
        assert!(
            !keep_path.exists(),
            ".keep file should be removed after successful transact_refs at {:?}",
            keep_path
        );

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 10: abort_pack removes the .keep file
    #[test]
    fn abort_pack_removes_keep_file() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _) = create_commit(&source, b"content for prop10\n", "file.txt", None, "test commit");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Verify that the .keep file was created
        let keep_path = handler
            .ingest_pack_keep_path()
            .expect("keep_path should be available after successful ingestion")
            .to_path_buf();
        assert!(
            keep_path.exists(),
            ".keep file should exist after pack ingestion at {:?}",
            keep_path
        );

        // Call abort_pack
        handler
            .abort_pack()
            .expect("abort_pack should succeed");

        // Assert the .keep file no longer exists
        assert!(
            !keep_path.exists(),
            ".keep file should be removed after abort_pack at {:?}",
            keep_path
        );

        // Verify state transitioned to Aborted
        assert_eq!(
            handler.state(),
            SessionState::Aborted,
            "handler state should be Aborted after abort_pack"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State machine tests
// ---------------------------------------------------------------------------

mod state_machine_tests {
    use super::*;

    #[test]
    fn check_connectivity_before_ingest_returns_not_ingested() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        create_bare_repo(tmp.path());

        let handler = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default())?;

        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let fake_id = gix_hash::ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("hex should parse");
        let updates = vec![Update {
            old_id: null_id,
            new_id: fake_id,
            ref_name: "refs/heads/main".into(),
        }];

        let result = handler.check_connectivity(&updates);
        match result {
            Err(ConnectivityError::NotIngested) => {}
            Err(other) => panic!(
                "expected ConnectivityError::NotIngested, got: {other}"
            ),
            Ok(_) => panic!(
                "expected ConnectivityError::NotIngested, but check_connectivity succeeded"
            ),
        }
        Ok(())
    }

    #[test]
    fn transact_refs_before_ingest_returns_not_ingested() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        create_bare_repo(tmp.path());

        let mut handler = ReceivePackHandler::open(tmp.path().to_path_buf(), Options::default())?;

        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let fake_id = gix_hash::ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("hex should parse");
        let updates = vec![Update {
            old_id: null_id,
            new_id: fake_id,
            ref_name: "refs/heads/main".into(),
        }];

        let result = handler.transact_refs(&updates);
        match result {
            Err(TransactError::NotIngested) => {}
            Err(other) => panic!(
                "expected TransactError::NotIngested, got: {other}"
            ),
            Ok(_) => panic!(
                "expected TransactError::NotIngested, but transact_refs succeeded"
            ),
        }
        Ok(())
    }

    #[test]
    fn abort_pack_after_commit_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _) = create_commit(&source, b"content\n", "file.txt", None, "initial");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        handler.disable_reflog();

        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed");

        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let updates = vec![Update {
            old_id: null_id,
            new_id,
            ref_name: "refs/heads/main".into(),
        }];

        handler
            .transact_refs(&updates)
            .expect("transact_refs should succeed");

        assert_eq!(
            handler.state(),
            SessionState::Committed,
            "handler should be in Committed state after transact_refs"
        );

        // abort_pack after commit should be a no-op returning Ok
        handler
            .abort_pack()
            .expect("abort_pack after commit should return Ok (no-op)");

        assert_eq!(
            handler.state(),
            SessionState::Committed,
            "handler state should remain Committed after abort_pack no-op"
        );

        Ok(())
    }

    #[test]
    fn double_abort_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _) = create_commit(&source, b"content\n", "file.txt", None, "initial");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;

        let mut cursor = std::io::Cursor::new(&pack_bytes);
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed");

        // First abort
        handler
            .abort_pack()
            .expect("first abort_pack should return Ok");

        assert_eq!(
            handler.state(),
            SessionState::Aborted,
            "handler should be in Aborted state after first abort"
        );

        // Second abort should also be Ok (no-op)
        handler
            .abort_pack()
            .expect("second abort_pack should return Ok (no-op)");

        assert_eq!(
            handler.state(),
            SessionState::Aborted,
            "handler state should remain Aborted after second abort"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Delegate properties
// ---------------------------------------------------------------------------

mod delegate_properties {
    use super::*;
    use gix_protocol::receive_pack::{Capability, Delegate, RefStatus, Request, UnpackStatus};

    // Feature: async-receive-pack-handler, Property 12: Pipeline failure yields Error status with all refs rejected
    #[test]
    fn pipeline_failure_yields_error_status_with_all_refs_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo = git_init_bare(tmp.path(), "dest.git");

        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;

        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let fake_id = gix_hash::ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("hex should parse");

        let request = Request {
            capabilities: vec![Capability {
                name: "report-status".into(),
                value: None,
            }],
            updates: vec![
                Update {
                    old_id: null_id,
                    new_id: fake_id,
                    ref_name: "refs/heads/main".into(),
                },
                Update {
                    old_id: null_id,
                    new_id: fake_id,
                    ref_name: "refs/heads/feature".into(),
                },
                Update {
                    old_id: null_id,
                    new_id: fake_id,
                    ref_name: "refs/heads/bugfix".into(),
                },
            ],
            push_options: Vec::new(),
        };

        // Provide malformed pack data
        let malformed_data = b"this is definitely not a valid git pack stream";
        let mut cursor = std::io::Cursor::new(malformed_data.as_slice());

        let response = handler
            .receive(&request, &mut cursor)
            .expect("Delegate::receive should return Ok(Response), not Err");

        // Assert UnpackStatus::Error with a descriptive message
        match &response.unpack_status {
            UnpackStatus::Error(msg) => {
                assert!(
                    !msg.is_empty(),
                    "error message should be descriptive, got empty string"
                );
            }
            UnpackStatus::Ok => {
                panic!("expected UnpackStatus::Error for malformed pack data, got Ok");
            }
        }

        // Assert ALL refs are rejected
        assert_eq!(
            response.ref_statuses.len(),
            request.updates.len(),
            "should have one ref status per update in the request"
        );

        for (i, status) in response.ref_statuses.iter().enumerate() {
            match status {
                RefStatus::Rejected { ref_name, message } => {
                    assert_eq!(
                        ref_name, &request.updates[i].ref_name,
                        "rejected ref name should match the update at index {i}"
                    );
                    assert!(
                        !message.is_empty(),
                        "rejection message should be non-empty for ref at index {i}"
                    );
                }
                RefStatus::Ok { ref_name } => {
                    panic!(
                        "expected RefStatus::Rejected for ref {ref_name}, got Ok (pipeline should have failed)"
                    );
                }
            }
        }

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 13: Successful pipeline yields Ok status for all refs
    #[test]
    fn successful_pipeline_yields_all_ok() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_a, _) =
            create_commit(&source, b"file a content\n", "a.txt", None, "commit A");
        let (commit_b, _) =
            create_commit(&source, b"file b content\n", "b.txt", None, "commit B");

        let pack_bytes = pack_objects(&source, &[&commit_a, &commit_b]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        handler.disable_reflog();

        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let id_a = gix_hash::ObjectId::from_hex(commit_a.as_bytes())
            .expect("commit A oid should be valid hex");
        let id_b = gix_hash::ObjectId::from_hex(commit_b.as_bytes())
            .expect("commit B oid should be valid hex");

        let request = Request {
            capabilities: vec![Capability {
                name: "report-status".into(),
                value: None,
            }],
            updates: vec![
                Update {
                    old_id: null_id,
                    new_id: id_a,
                    ref_name: "refs/heads/main".into(),
                },
                Update {
                    old_id: null_id,
                    new_id: id_b,
                    ref_name: "refs/heads/feature".into(),
                },
            ],
            push_options: Vec::new(),
        };

        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        let response = handler
            .receive(&request, &mut cursor)
            .expect("Delegate::receive should return Ok(Response)");

        assert_eq!(
            response.unpack_status,
            UnpackStatus::Ok,
            "successful pipeline should yield UnpackStatus::Ok"
        );

        assert_eq!(
            response.ref_statuses.len(),
            request.updates.len(),
            "should have exactly one ref status per update"
        );

        for (i, status) in response.ref_statuses.iter().enumerate() {
            match status {
                RefStatus::Ok { ref_name } => {
                    assert_eq!(
                        ref_name, &request.updates[i].ref_name,
                        "RefStatus::Ok ref_name should match the update at index {i}"
                    );
                }
                RefStatus::Rejected { ref_name, message } => {
                    panic!(
                        "expected RefStatus::Ok for ref {ref_name} at index {i}, \
                         got Rejected with message: {message}"
                    );
                }
            }
        }

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 11: Ingested objects are accessible through ODB
    #[test]
    fn ingested_objects_accessible_through_odb() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let blob_oid_str = write_blob(&source, b"odb accessibility test\n");
        let (commit_oid, tree_oid) =
            create_commit(&source, b"odb accessibility test\n", "test.txt", None, "odb test commit");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;

        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Now look up each known object through the ODB
        let odb_handle = handler.odb().to_handle_arc();

        let commit_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");
        let tree_id = gix_hash::ObjectId::from_hex(tree_oid.as_bytes())
            .expect("tree oid should be valid hex");
        let blob_id = gix_hash::ObjectId::from_hex(blob_oid_str.as_bytes())
            .expect("blob oid should be valid hex");

        let mut buf = Vec::new();

        {
            use gix_object::Find;
            let commit_data = odb_handle
                .try_find(&commit_id, &mut buf)
                .expect("ODB lookup should not error")
                .expect("commit object should be accessible in ODB after ingestion");
            assert_eq!(
                commit_data.kind,
                gix_object::Kind::Commit,
                "looked-up commit should have Commit kind"
            );
        }

        {
            use gix_object::Find;
            let tree_data = odb_handle
                .try_find(&tree_id, &mut buf)
                .expect("ODB lookup should not error")
                .expect("tree object should be accessible in ODB after ingestion");
            assert_eq!(
                tree_data.kind,
                gix_object::Kind::Tree,
                "looked-up tree should have Tree kind"
            );
        }

        {
            use gix_object::Find;
            let blob_data = odb_handle
                .try_find(&blob_id, &mut buf)
                .expect("ODB lookup should not error")
                .expect("blob object should be accessible in ODB after ingestion");
            assert_eq!(
                blob_data.kind,
                gix_object::Kind::Blob,
                "looked-up blob should have Blob kind"
            );
        }

        Ok(())
    }

    // Feature: async-receive-pack-handler, Property 14: check_connectivity returns the correct new-object set
    #[test]
    fn check_connectivity_returns_correct_new_object_set() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo = git_init_bare(tmp.path(), "repo.git");

        let (commit_a, tree_a) =
            create_commit(&repo, b"content A\n", "a.txt", None, "commit A");

        // Point refs/heads/main at A so it becomes a pre-existing tip
        git_in(&repo, &["update-ref", "refs/heads/main", &commit_a]);

        let (commit_b, tree_b) =
            create_commit(&repo, b"content B\n", "b.txt", Some(&commit_a), "commit B");

        let blob_b = write_blob(&repo, b"content B\n");

        // Create a pack containing only B's new objects (exclude A)
        let rev_input = format!("{}\n^{}\n", commit_b, commit_a);
        let pack_bytes = pack_objects_with_exclusions(&repo, &rev_input);

        // Open handler on the same repo (which already has A on refs/heads/main)
        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for the incremental pack");

        let a_id = gix_hash::ObjectId::from_hex(commit_a.as_bytes())
            .expect("commit A oid should be valid hex");
        let b_id = gix_hash::ObjectId::from_hex(commit_b.as_bytes())
            .expect("commit B oid should be valid hex");
        let tree_a_id = gix_hash::ObjectId::from_hex(tree_a.as_bytes())
            .expect("tree A oid should be valid hex");
        let tree_b_id = gix_hash::ObjectId::from_hex(tree_b.as_bytes())
            .expect("tree B oid should be valid hex");
        let blob_b_id = gix_hash::ObjectId::from_hex(blob_b.as_bytes())
            .expect("blob B oid should be valid hex");

        let update = Update {
            old_id: a_id,
            new_id: b_id,
            ref_name: "refs/heads/main".into(),
        };

        let result = handler.check_connectivity(&[update])?;

        assert!(
            result.new_objects.contains(&b_id),
            "new_objects should contain commit B"
        );
        assert!(
            result.new_objects.contains(&tree_b_id),
            "new_objects should contain tree B"
        );
        assert!(
            result.new_objects.contains(&blob_b_id),
            "new_objects should contain blob B"
        );
        assert!(
            !result.new_objects.contains(&a_id),
            "new_objects should NOT contain commit A (pre-existing)"
        );
        assert!(
            !result.new_objects.contains(&tree_a_id),
            "new_objects should NOT contain tree A (reachable from pre-existing tip)"
        );

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

mod integration_tests {
    use super::*;
    use gix_protocol::receive_pack::{Capability, Delegate, RefStatus, Request, UnpackStatus};
    use gix_protocol::receive_pack::handler::RefUpdateStatus;

    // Task 10.1: Integration test — full push round-trip via Delegate
    #[test]
    fn integration_full_push_round_trip_via_delegate() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _tree_oid) =
            create_commit(&source, b"integration test content\n", "hello.txt", None, "initial commit");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");

        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        handler.disable_reflog();

        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");

        let request = Request {
            capabilities: vec![Capability {
                name: "report-status".into(),
                value: None,
            }],
            updates: vec![Update {
                old_id: null_id,
                new_id,
                ref_name: "refs/heads/main".into(),
            }],
            push_options: Vec::new(),
        };

        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        let response = handler
            .receive(&request, &mut cursor)
            .expect("Delegate::receive should return Ok(Response)");

        assert_eq!(
            response.unpack_status,
            UnpackStatus::Ok,
            "full push round-trip should yield UnpackStatus::Ok"
        );
        assert_eq!(
            response.ref_statuses.len(),
            1,
            "should have exactly one ref status"
        );
        match &response.ref_statuses[0] {
            RefStatus::Ok { ref_name } => {
                assert_eq!(
                    ref_name.as_ref() as &[u8],
                    b"refs/heads/main",
                    "ref status should be for refs/heads/main"
                );
            }
            RefStatus::Rejected { ref_name, message } => {
                panic!(
                    "expected RefStatus::Ok for {}, got Rejected: {}",
                    ref_name,
                    message
                );
            }
        }

        // Verify the ref was actually created in the destination repo
        let show_ref_output = std::process::Command::new("git")
            .args(["show-ref", "--verify", "refs/heads/main"])
            .current_dir(&dest)
            .env("GIT_DIR", &dest)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("git show-ref should execute");
        assert!(
            show_ref_output.status.success(),
            "refs/heads/main should exist in the destination repo after push, \
             git show-ref failed: {}",
            String::from_utf8_lossy(&show_ref_output.stderr)
        );

        let show_ref_line = String::from_utf8(show_ref_output.stdout)
            .expect("show-ref output should be valid utf-8")
            .trim()
            .to_string();
        assert!(
            show_ref_line.starts_with(&commit_oid),
            "refs/heads/main should point to the pushed commit {}, got: {}",
            commit_oid,
            show_ref_line
        );

        Ok(())
    }

    // Task 10.2: Integration test — thin pack resolution
    #[test]
    fn integration_thin_pack_resolution() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let repo = git_init_bare(tmp.path(), "repo.git");

        let base_content = b"this is the base content that will serve as a delta base for thin pack resolution testing\n";
        let _base_oid = write_blob(&repo, base_content);

        let delta_content = b"this is the base content that will serve as a delta base for thin pack resolution testing\nwith additional line appended\n";
        let delta_oid = write_blob(&repo, delta_content);

        // Create a thin pack containing only the delta object
        let mut child = std::process::Command::new("git")
            .args(["pack-objects", "--stdout", "--thin"])
            .current_dir(&repo)
            .env("GIT_DIR", &repo)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("git pack-objects --thin should spawn");
        {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("stdin available")
                .write_all(format!("{}\n", delta_oid).as_bytes())
                .expect("write oid to pack-objects stdin should succeed");
        }
        let output = child
            .wait_with_output()
            .expect("git pack-objects --thin should complete");
        assert!(
            output.status.success(),
            "git pack-objects --thin should succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let thin_pack_bytes = output.stdout;
        assert!(
            !thin_pack_bytes.is_empty(),
            "thin pack should produce non-empty output"
        );

        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;

        let mut cursor = std::io::Cursor::new(&thin_pack_bytes);
        let ingest_outcome = handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a thin pack with resolvable bases");

        assert!(
            ingest_outcome.object_count >= 1,
            "ingested pack should have at least 1 object, got {}",
            ingest_outcome.object_count
        );

        assert!(
            ingest_outcome.outcome.data_path.is_some(),
            "data_path should be set after successful thin pack ingestion"
        );
        assert!(
            ingest_outcome.outcome.index_path.is_some(),
            "index_path should be set after successful thin pack ingestion"
        );

        Ok(())
    }

    // Task 10.3: Integration test — Pipeline Step API usage
    #[test]
    fn integration_pipeline_step_api_usage() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let source = git_init_bare(tmp.path(), "source.git");
        let (commit_oid, _tree_oid) =
            create_commit(&source, b"pipeline step api\n", "step.txt", None, "step commit");

        let pack_bytes = pack_objects(&source, &[&commit_oid]);

        let dest = git_init_bare(tmp.path(), "dest.git");
        let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())?;
        handler.disable_reflog();

        // Step 1: ingest_pack directly
        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        let ingest_outcome = handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for a valid pack");

        // Verify .keep file exists after ingestion
        let keep_path = ingest_outcome
            .outcome
            .keep_path
            .as_ref()
            .expect("keep_path should be Some for a non-empty pack")
            .clone();
        assert!(
            keep_path.exists(),
            ".keep file should exist after pack ingestion at {:?}",
            keep_path
        );

        // Step 2: skip check_connectivity (integrator choice)

        // Step 3: call transact_refs directly with creation updates
        let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        let new_id = gix_hash::ObjectId::from_hex(commit_oid.as_bytes())
            .expect("commit oid should be valid hex");

        let updates = vec![Update {
            old_id: null_id,
            new_id,
            ref_name: "refs/heads/main".into(),
        }];

        let transaction_result = handler
            .transact_refs(&updates)
            .expect("transact_refs should succeed without prior check_connectivity");

        assert_eq!(
            transaction_result.ref_results.len(),
            1,
            "should have one ref result"
        );
        assert_eq!(
            transaction_result.ref_results[0].status,
            RefUpdateStatus::Ok,
            "ref update should be Ok"
        );

        // Verify .keep file is removed
        assert!(
            !keep_path.exists(),
            ".keep file should be removed after successful transact_refs"
        );

        // Verify refs are actually updated via git show-ref
        let show_ref_output = git_in(&dest, &["show-ref", "--verify", "refs/heads/main"]);
        assert!(
            show_ref_output.starts_with(&commit_oid),
            "refs/heads/main should point to the pushed commit {}, got: {}",
            commit_oid,
            show_ref_output
        );

        Ok(())
    }

    // Task 10.4: Integration test — CAS mismatch detection
    #[test]
    fn integration_cas_mismatch_detection() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let repo = git_init_bare(tmp.path(), "repo.git");
        let (commit_a, _) =
            create_commit(&repo, b"first commit\n", "a.txt", None, "commit A");

        // Point refs/heads/main at commit A
        git_in(&repo, &["update-ref", "refs/heads/main", &commit_a]);

        let (commit_b, _) =
            create_commit(&repo, b"second commit\n", "b.txt", Some(&commit_a), "commit B");

        let rev_input = format!("{}\n^{}\n", commit_b, commit_a);
        let pack_bytes = pack_objects_with_exclusions(&repo, &rev_input);

        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;
        handler.disable_reflog();

        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed");

        // Attempt transact_refs with a WRONG old_id (CAS mismatch)
        let wrong_old_id = gix_hash::ObjectId::from_hex(
            b"0000000000000000000000000000000000000001",
        )
        .expect("hex should parse");
        let new_id = gix_hash::ObjectId::from_hex(commit_b.as_bytes())
            .expect("commit B oid should be valid hex");

        let updates = vec![Update {
            old_id: wrong_old_id,
            new_id,
            ref_name: "refs/heads/main".into(),
        }];

        let result = handler.transact_refs(&updates);

        match result {
            Err(TransactError::Prepare(_)) => {
                // Expected: CAS mismatch causes preparation failure
            }
            Err(other) => {
                panic!(
                    "expected TransactError::Prepare for CAS mismatch, got: {other}"
                );
            }
            Ok(_) => {
                panic!(
                    "expected TransactError::Prepare for CAS mismatch, but transact_refs succeeded"
                );
            }
        }

        Ok(())
    }

    // Task 10.5: Integration test — connectivity walk termination on deep history
    #[test]
    fn integration_connectivity_walk_terminates_on_deep_history() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let repo = git_init_bare(tmp.path(), "repo.git");

        // Create a chain of 12 commits: C0 → C1 → ... → C11
        let mut prev_oid: Option<String> = None;
        let mut all_oids = Vec::new();
        for i in 0..12 {
            let content = format!("content for commit {}\n", i);
            let filename = format!("file_{}.txt", i);
            let message = format!("commit {}", i);
            let (commit_oid, _tree_oid) = create_commit(
                &repo,
                content.as_bytes(),
                &filename,
                prev_oid.as_deref(),
                &message,
            );
            all_oids.push(commit_oid.clone());
            prev_oid = Some(commit_oid);
        }

        // Point refs/heads/main at the tip of the chain (C11)
        let tip_oid = all_oids.last().expect("should have at least one commit");
        git_in(&repo, &["update-ref", "refs/heads/main", tip_oid]);

        // Create one new commit on top (C12, parent = C11)
        let (new_commit_oid, _) = create_commit(
            &repo,
            b"new commit on top of deep history\n",
            "new_file.txt",
            Some(tip_oid),
            "new commit C12",
        );

        // Create a pack containing only the new commit's objects
        let rev_input = format!("{}\n^{}\n", new_commit_oid, tip_oid);
        let pack_bytes = pack_objects_with_exclusions(&repo, &rev_input);

        let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())?;
        let mut cursor = std::io::Cursor::new(pack_bytes.as_slice());
        handler
            .ingest_pack(&mut cursor)
            .expect("ingest_pack should succeed for the incremental pack");

        let old_id = gix_hash::ObjectId::from_hex(tip_oid.as_bytes())
            .expect("tip oid should be valid hex");
        let new_id = gix_hash::ObjectId::from_hex(new_commit_oid.as_bytes())
            .expect("new commit oid should be valid hex");

        let update = Update {
            old_id,
            new_id,
            ref_name: "refs/heads/main".into(),
        };

        let start = std::time::Instant::now();
        let result = handler.check_connectivity(&[update]);
        let elapsed = start.elapsed();

        assert!(
            result.is_ok(),
            "connectivity check should succeed on deep history, got: {:?}",
            result.err()
        );

        let connectivity_result = result.expect("already asserted Ok");

        // new_objects should be small — just the new commit, its tree, and its blob (3 objects)
        assert!(
            connectivity_result.new_objects.len() <= 5,
            "new_objects should be small (only new commit's reachable objects), got {} objects",
            connectivity_result.new_objects.len()
        );

        assert!(
            connectivity_result.new_objects.contains(&new_id),
            "new_objects should contain the new commit"
        );

        // Verify old commits are NOT in new_objects
        for oid_str in &all_oids {
            let oid = gix_hash::ObjectId::from_hex(oid_str.as_bytes())
                .expect("oid should be valid hex");
            assert!(
                !connectivity_result.new_objects.contains(&oid),
                "new_objects should NOT contain pre-existing commit {}",
                oid_str
            );
        }

        // The walk should complete very quickly
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "connectivity walk should complete quickly (terminated at pre-existing tips), \
             took {:?}",
            elapsed
        );

        Ok(())
    }
}
