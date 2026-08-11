//! Integration tests for the receive-pack handler.
//!
//! Tests pack ingestion, connectivity checking, ref transactions, state machine
//! behavior, the Delegate trait implementation, and end-to-end push round-trips.

use std::path::{Path, PathBuf};

use gix_protocol::receive_pack::handler::{
    ConnectivityError, OpenError, Options, ReceivePackHandler, SessionState, TransactError,
};
use gix_protocol::receive_pack::{SessionConfig, Update};

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
            let result = handler.ingest_pack(&mut cursor, &SessionConfig::default());

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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
        let outcome = handler.ingest_pack(&mut cursor, &SessionConfig::default());

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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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

        let result = handler.transact_refs(&[update], &SessionConfig::default());
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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

        let result = handler.transact_refs(&updates, &SessionConfig::default());
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .transact_refs(&updates, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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
            .transact_refs(&updates, &SessionConfig::default())
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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

        let result = handler.transact_refs(&updates, &SessionConfig::default());

        match result {
            Ok(transaction_result) => {
                // Per-ref mode: CAS mismatch causes the ref to be reported as Rejected
                assert_eq!(
                    transaction_result.ref_results.len(),
                    1,
                    "should have one ref result"
                );
                match &transaction_result.ref_results[0].status {
                    gix_protocol::receive_pack::handler::RefUpdateStatus::Rejected { reason } => {
                        assert!(
                            reason.contains("lock/cas failed"),
                            "rejection reason should indicate CAS failure, got: {reason}"
                        );
                    }
                    other => {
                        panic!(
                            "expected Rejected status for CAS mismatch, got: {other:?}"
                        );
                    }
                }
            }
            Err(e) => {
                panic!(
                    "expected Ok(TransactionResult) with Rejected status for CAS mismatch, got error: {e}"
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
            .ingest_pack(&mut cursor, &SessionConfig::default())
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

// ---------------------------------------------------------------------------
// Atomic all-or-nothing ref transaction properties
// ---------------------------------------------------------------------------

// Feature: receive-pack-v1-support, Property 4: Atomic mode — all-or-nothing ref transaction semantics
mod atomic_transaction_properties {
    use super::*;
    use gix_protocol::receive_pack::handler::RefUpdateStatus;
    use proptest::prelude::*;

    // **Validates: Requirements 4.1, 4.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        /// In atomic mode, when one ref has a CAS mismatch (wrong old_id),
        /// ALL refs in the update set must be reported with the same status.
        /// Since a CAS mismatch is injected, all must be Rejected.
        #[test]
        fn atomic_mode_all_rejected_on_any_cas_failure(
            ref_count in 2usize..=6,
            failing_index in 0usize..6,
        ) {
            // Clamp failing_index to actual ref_count
            let failing_index = failing_index % ref_count;

            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let source = git_init_bare(tmp.path(), "source.git");
            let dest = git_init_bare(tmp.path(), "dest.git");

            // Create ref_count commits in source, each pushed to dest on a distinct ref.
            let mut commit_oids = Vec::with_capacity(ref_count);
            let mut ref_names = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("atomic test content {}\n", i);
                let filename = format!("file_{}.txt", i);
                let message = format!("commit for ref {}", i);
                let (commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/branch_{}", i);
                ref_names.push(ref_name.clone());
                commit_oids.push(commit_oid.clone());

                // Point the ref in dest to this commit so we can later update it
                // (first we need the objects in dest)
            }

            // Pack all source objects and ingest them into dest
            let pack_bytes = pack_objects(&source, &commit_oids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open for dest repo");
            handler.disable_reflog();

            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor, &SessionConfig::default())
                .expect("ingest_pack should succeed");

            // Set up refs in dest pointing to the commits (simulates existing state).
            // We do this via git update-ref after the objects are in the ODB.
            for (i, commit_oid) in commit_oids.iter().enumerate() {
                git_in(&dest, &["update-ref", &ref_names[i], commit_oid]);
            }

            // Create new commits (updates) from a second batch in source
            let mut new_commit_oids = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("updated atomic content {}\n", i);
                let filename = format!("file_{}.txt", i);
                let message = format!("update commit for ref {}", i);
                let (new_commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    Some(&commit_oids[i]),
                    &message,
                );
                new_commit_oids.push(new_commit_oid);
            }

            // Pack the new objects and ingest into a fresh handler
            let new_pack_bytes = pack_objects_with_exclusions(
                &source,
                &new_commit_oids.iter()
                    .enumerate()
                    .map(|(i, new_oid)| format!("{}\n^{}\n", new_oid, commit_oids[i]))
                    .collect::<String>(),
            );

            let mut handler2 = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open for dest repo (second time)");
            handler2.disable_reflog();

            let mut cursor2 = std::io::Cursor::new(&new_pack_bytes);
            handler2
                .ingest_pack(&mut cursor2, &SessionConfig::default())
                .expect("second ingest_pack should succeed");

            // Build updates: all have correct old_id EXCEPT the failing_index one
            // which gets a wrong old_id (injected CAS failure).
            let wrong_old_id = gix_hash::ObjectId::from_bytes_or_panic(&[0xAB; 20]);

            let updates: Vec<Update> = (0..ref_count)
                .map(|i| {
                    let old_id = if i == failing_index {
                        // Inject CAS mismatch: use a bogus old_id
                        wrong_old_id
                    } else {
                        gix_hash::ObjectId::from_hex(commit_oids[i].as_bytes())
                            .expect("commit oid should be valid hex")
                    };
                    let new_id = gix_hash::ObjectId::from_hex(new_commit_oids[i].as_bytes())
                        .expect("new commit oid should be valid hex");
                    Update {
                        old_id,
                        new_id,
                        ref_name: ref_names[i].clone().into(),
                    }
                })
                .collect();

            // Execute in atomic mode
            let atomic_config = SessionConfig { no_thin: false, atomic: true };
            let result = handler2.transact_refs(&updates, &atomic_config);
            let transaction_result = result.expect("transact_refs should not return a hard error");

            // PROPERTY ASSERTION: In atomic mode with a CAS failure, ALL refs must be Rejected.
            prop_assert_eq!(
                transaction_result.ref_results.len(),
                ref_count,
                "should have one result per update"
            );

            let all_rejected = transaction_result.ref_results.iter().all(|r| {
                matches!(r.status, RefUpdateStatus::Rejected { .. })
            });
            prop_assert!(
                all_rejected,
                "atomic mode with CAS failure on ref index {} should reject ALL refs, but got: {:?}",
                failing_index,
                transaction_result.ref_results
            );
        }

        /// In atomic mode, when all refs have correct old_ids (no CAS failure),
        /// ALL refs must be reported as Ok.
        #[test]
        fn atomic_mode_all_ok_when_no_failures(
            ref_count in 2usize..=6,
        ) {
            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let source = git_init_bare(tmp.path(), "source.git");
            let dest = git_init_bare(tmp.path(), "dest.git");

            // Create ref_count commits in source
            let mut commit_oids = Vec::with_capacity(ref_count);
            let mut ref_names = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("atomic ok content {}\n", i);
                let filename = format!("file_{}.txt", i);
                let message = format!("commit for ok ref {}", i);
                let (commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/ok_branch_{}", i);
                ref_names.push(ref_name);
                commit_oids.push(commit_oid);
            }

            // Pack and ingest into dest
            let pack_bytes = pack_objects(&source, &commit_oids.iter().map(|s| s.as_str()).collect::<Vec<_>>());

            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open");
            handler.disable_reflog();

            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor, &SessionConfig::default())
                .expect("ingest_pack should succeed");

            // Build creation updates (old_id = null → no CAS check needed for "must not exist")
            let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
            let updates: Vec<Update> = (0..ref_count)
                .map(|i| {
                    let new_id = gix_hash::ObjectId::from_hex(commit_oids[i].as_bytes())
                        .expect("commit oid should be valid hex");
                    Update {
                        old_id: null_id,
                        new_id,
                        ref_name: ref_names[i].clone().into(),
                    }
                })
                .collect();

            // Execute in atomic mode
            let atomic_config = SessionConfig { no_thin: false, atomic: true };
            let result = handler.transact_refs(&updates, &atomic_config);
            let transaction_result = result.expect("transact_refs should not return a hard error");

            // PROPERTY ASSERTION: In atomic mode with no CAS failures, ALL refs must be Ok.
            prop_assert_eq!(
                transaction_result.ref_results.len(),
                ref_count,
                "should have one result per update"
            );

            let all_ok = transaction_result.ref_results.iter().all(|r| {
                matches!(r.status, RefUpdateStatus::Ok)
            });
            prop_assert!(
                all_ok,
                "atomic mode with valid updates should report ALL refs as Ok, but got: {:?}",
                transaction_result.ref_results
            );
        }
    }
}


// ---------------------------------------------------------------------------
// Per-ref independent ref reporting properties
// ---------------------------------------------------------------------------

// Feature: receive-pack-v1-support, Property 5: Per-ref mode — independent ref reporting
mod per_ref_independence_properties {
    use super::*;
    use gix_protocol::receive_pack::handler::RefUpdateStatus;
    use proptest::prelude::*;

    // **Validates: Requirements 4.3, 4.4, 4.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        /// In per-ref mode, refs with correct old_id succeed (Ok) and refs with
        /// wrong old_id are rejected independently. A CAS failure on one ref does
        /// NOT cause other refs to be rejected.
        #[test]
        fn per_ref_mode_independent_reporting(
            ref_count in 2usize..=6,
            failure_seed in proptest::collection::vec(any::<bool>(), 2..=6),
        ) {
            // Adjust failure_seed to match ref_count and ensure at least one success + one failure
            let mut failure_mask: Vec<bool> = failure_seed.into_iter().take(ref_count).collect();
            // Pad if seed was shorter than ref_count
            while failure_mask.len() < ref_count {
                failure_mask.push(false);
            }
            // Ensure at least one failure
            if !failure_mask.iter().any(|&b| b) {
                failure_mask[0] = true;
            }
            // Ensure at least one success
            if !failure_mask.iter().any(|&b| !b) {
                let last = failure_mask.len() - 1;
                failure_mask[last] = false;
            }

            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let source = git_init_bare(tmp.path(), "source.git");
            let dest = git_init_bare(tmp.path(), "dest.git");

            // Create ref_count independent commits in source.
            let mut commit_oids = Vec::with_capacity(ref_count);
            let mut ref_names = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("per-ref independence content {}\n", i);
                let filename = format!("perref_{}.txt", i);
                let message = format!("commit for per-ref test {}", i);
                let (commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/perref_{}", i);
                ref_names.push(ref_name);
                commit_oids.push(commit_oid);
            }

            // Pack all source objects and ingest into dest.
            let pack_bytes = pack_objects(
                &source,
                &commit_oids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            );

            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open for dest repo");
            handler.disable_reflog();

            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor, &SessionConfig::default())
                .expect("ingest_pack should succeed");

            // Establish refs in dest pointing to those commits (simulates existing refs).
            for (i, commit_oid) in commit_oids.iter().enumerate() {
                git_in(&dest, &["update-ref", &ref_names[i], commit_oid]);
            }

            // Create new commits (one per ref) to serve as update targets.
            let mut new_commit_oids = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("updated per-ref content {}\n", i);
                let filename = format!("perref_{}.txt", i);
                let message = format!("update commit for per-ref {}", i);
                let (new_commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    Some(&commit_oids[i]),
                    &message,
                );
                new_commit_oids.push(new_commit_oid);
            }

            // Pack new objects and ingest into a fresh handler on dest.
            let new_pack_bytes = pack_objects_with_exclusions(
                &source,
                &new_commit_oids
                    .iter()
                    .enumerate()
                    .map(|(i, new_oid)| format!("{}\n^{}\n", new_oid, commit_oids[i]))
                    .collect::<String>(),
            );

            let mut handler2 = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open for dest repo (second time)");
            handler2.disable_reflog();

            let mut cursor2 = std::io::Cursor::new(&new_pack_bytes);
            handler2
                .ingest_pack(&mut cursor2, &SessionConfig::default())
                .expect("second ingest_pack should succeed");

            // Build updates: refs where failure_mask[i] == true get a wrong old_id (CAS failure),
            // while refs where failure_mask[i] == false get the correct old_id.
            let wrong_old_id = gix_hash::ObjectId::from_bytes_or_panic(&[0xCD; 20]);

            let updates: Vec<Update> = (0..ref_count)
                .map(|i| {
                    let old_id = if failure_mask[i] {
                        // Inject CAS mismatch
                        wrong_old_id
                    } else {
                        gix_hash::ObjectId::from_hex(commit_oids[i].as_bytes())
                            .expect("commit oid should be valid hex")
                    };
                    let new_id = gix_hash::ObjectId::from_hex(new_commit_oids[i].as_bytes())
                        .expect("new commit oid should be valid hex");
                    Update {
                        old_id,
                        new_id,
                        ref_name: ref_names[i].clone().into(),
                    }
                })
                .collect();

            // Execute in per-ref mode (atomic = false).
            let per_ref_config = SessionConfig { no_thin: false, atomic: false };
            let result = handler2.transact_refs(&updates, &per_ref_config);
            let transaction_result = result
                .expect("transact_refs in per-ref mode should not return a hard error");

            // PROPERTY ASSERTION: Each ref is reported independently.
            prop_assert_eq!(
                transaction_result.ref_results.len(),
                ref_count,
                "should have one result per update"
            );

            for (i, ref_result) in transaction_result.ref_results.iter().enumerate() {
                if failure_mask[i] {
                    // This ref had a CAS mismatch → should be Rejected
                    prop_assert!(
                        matches!(ref_result.status, RefUpdateStatus::Rejected { .. }),
                        "ref at index {} ({}) had injected CAS failure but was not Rejected: {:?}",
                        i,
                        ref_names[i],
                        ref_result.status
                    );
                } else {
                    // This ref had correct old_id → should be Ok
                    prop_assert!(
                        matches!(ref_result.status, RefUpdateStatus::Ok),
                        "ref at index {} ({}) had correct old_id but was not Ok: {:?}",
                        i,
                        ref_names[i],
                        ref_result.status
                    );
                }
            }
        }

        /// In per-ref mode, when ALL refs have correct old_ids (no failures),
        /// all refs should be reported as Ok.
        #[test]
        fn per_ref_mode_all_ok_when_no_failures(
            ref_count in 2usize..=6,
        ) {
            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let source = git_init_bare(tmp.path(), "source.git");
            let dest = git_init_bare(tmp.path(), "dest.git");

            // Create ref_count commits in source
            let mut commit_oids = Vec::with_capacity(ref_count);
            let mut ref_names = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("per-ref all-ok content {}\n", i);
                let filename = format!("allok_{}.txt", i);
                let message = format!("commit for all-ok ref {}", i);
                let (commit_oid, _) = create_commit(
                    &source,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/allok_{}", i);
                ref_names.push(ref_name);
                commit_oids.push(commit_oid);
            }

            // Pack and ingest into dest
            let pack_bytes = pack_objects(
                &source,
                &commit_oids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            );

            let mut handler = ReceivePackHandler::open(dest.clone(), Options::default())
                .expect("handler should open");
            handler.disable_reflog();

            let mut cursor = std::io::Cursor::new(&pack_bytes);
            handler
                .ingest_pack(&mut cursor, &SessionConfig::default())
                .expect("ingest_pack should succeed");

            // Build creation updates (old_id = null → refs must not exist yet)
            let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
            let updates: Vec<Update> = (0..ref_count)
                .map(|i| {
                    let new_id = gix_hash::ObjectId::from_hex(commit_oids[i].as_bytes())
                        .expect("commit oid should be valid hex");
                    Update {
                        old_id: null_id,
                        new_id,
                        ref_name: ref_names[i].clone().into(),
                    }
                })
                .collect();

            // Execute in per-ref mode (atomic = false, the default)
            let per_ref_config = SessionConfig { no_thin: false, atomic: false };
            let result = handler.transact_refs(&updates, &per_ref_config);
            let transaction_result = result
                .expect("transact_refs in per-ref mode should not return a hard error");

            // PROPERTY ASSERTION: All refs succeed independently.
            prop_assert_eq!(
                transaction_result.ref_results.len(),
                ref_count,
                "should have one result per update"
            );

            let all_ok = transaction_result.ref_results.iter().all(|r| {
                matches!(r.status, RefUpdateStatus::Ok)
            });
            prop_assert!(
                all_ok,
                "per-ref mode with all valid updates should report ALL refs as Ok, but got: {:?}",
                transaction_result.ref_results
            );
        }
    }
}

// ---------------------------------------------------------------------------
// No-thin enforcement properties
// ---------------------------------------------------------------------------

// Feature: receive-pack-v1-support, Property 6: No-thin enforcement rejects thin packs
mod no_thin_enforcement_properties {
    use super::*;
    use proptest::prelude::*;

    /// Generate a thin pack using `git pack-objects --stdout --thin --revs`.
    ///
    /// Creates a parent commit (base) and a child commit whose tree shares
    /// similar content, then packs the child with the parent excluded. This
    /// forces git to emit REF_DELTA entries whose bases exist only in the ODB
    /// (not in the pack).
    ///
    /// Returns (thin_pack_bytes, child_commit_oid) or None if git didn't produce output.
    fn create_thin_pack_via_commits(
        repo: &std::path::Path,
        base_content: &[u8],
        child_content: &[u8],
    ) -> Option<(Vec<u8>, String)> {
        // Create parent commit with a file
        let (parent_oid, _) = create_commit(repo, base_content, "shared.txt", None, "base commit");

        // Create child commit with similar file content (same filename to maximize deltification)
        let (child_oid, _) = create_commit(
            repo,
            child_content,
            "shared.txt",
            Some(&parent_oid),
            "child commit",
        );

        // Pack the child with parent excluded and --thin enabled.
        // This tells git "the receiver has parent, so delta against its objects".
        let rev_input = format!("{}\n^{}\n", child_oid, parent_oid);
        let mut child_proc = std::process::Command::new("git")
            .args(["pack-objects", "--stdout", "--thin", "--revs"])
            .current_dir(repo)
            .env("GIT_DIR", repo)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("git pack-objects --thin --revs should spawn");
        {
            use std::io::Write;
            child_proc
                .stdin
                .take()
                .expect("stdin available")
                .write_all(rev_input.as_bytes())
                .expect("write revs to pack-objects stdin should succeed");
        }
        let output = child_proc
            .wait_with_output()
            .expect("git pack-objects --thin --revs should complete");
        if !output.status.success() || output.stdout.is_empty() {
            return None;
        }
        Some((output.stdout, child_oid))
    }

    /// Check if a pack file actually contains REF_DELTA entries by verifying
    /// it fails to be ingested without ODB lookup (indicating it's truly thin).
    fn is_truly_thin_pack(_repo: &std::path::Path, pack_bytes: &[u8]) -> bool {
        // Create a separate empty repo where the base objects don't exist.
        // If the pack ingests fine there, it's NOT truly thin.
        let empty_tmp = tempfile::tempdir()
            .expect("should be able to create temp dir for thin check");
        let empty_repo = git_init_bare(empty_tmp.path(), "empty-check.git");

        let mut handler = ReceivePackHandler::open(empty_repo, Options::default())
            .expect("handler should open for empty repo");

        let no_thin_config = SessionConfig { no_thin: true, atomic: false };
        let mut cursor = std::io::Cursor::new(pack_bytes);
        let result = handler.ingest_pack(&mut cursor, &no_thin_config);
        // If it fails in an empty repo with no ODB lookup, it IS truly thin.
        // (Note: we also skip the ODB lookup by using no_thin: true, but since
        // the empty repo has no objects anyway, it doesn't matter.)
        result.is_err()
    }

    // **Validates: Requirements 5.1, 5.2, 5.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        /// With `no_thin: true`, a thin pack whose ref-delta bases are NOT included
        /// in the pack itself must be rejected. With `no_thin: false` on the same
        /// repo (where bases exist in the ODB), the same thin pack must succeed.
        #[test]
        fn no_thin_rejects_thin_packs_and_without_no_thin_succeeds(
            // Use base content large enough that git will delta-compress the child.
            // Git needs at least ~50-60 bytes of shared content for delta heuristics
            // to kick in. We use 128+ bytes to be safe.
            base_size in 128usize..512,
            suffix_size in 4usize..32,
        ) {
            // Generate deterministic base content (repeating pattern)
            let base_content: Vec<u8> = (0..base_size)
                .map(|i| b'A' + (i % 26) as u8)
                .collect();
            // Child content = base + extra suffix (very similar → forces delta encoding)
            let mut child_content = base_content.clone();
            let extra: Vec<u8> = (0..suffix_size)
                .map(|i| b'z' - (i % 26) as u8)
                .collect();
            child_content.extend_from_slice(&extra);

            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let repo = git_init_bare(tmp.path(), "thin-test.git");

            let (thin_pack_bytes, _child_oid) = match create_thin_pack_via_commits(
                &repo,
                &base_content,
                &child_content,
            ) {
                Some(result) => result,
                None => {
                    // git didn't produce a thin pack for this content — skip
                    return Ok(());
                }
            };

            // Verify this is actually a thin pack (contains REF_DELTA entries
            // referencing objects not in the pack). If it's NOT thin, skip.
            if !is_truly_thin_pack(&repo, &thin_pack_bytes) {
                // git decided not to produce ref-deltas for this content.
                // This can happen if the objects are too small or dissimilar.
                return Ok(());
            }

            // --- Test 1: no_thin = true → should reject the thin pack ---
            // The repo HAS the base objects, but no_thin disables ODB lookup,
            // so the ref-delta entries cannot be resolved.
            {
                let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())
                    .expect("handler should open for thin-test repo");

                let no_thin_config = SessionConfig { no_thin: true, atomic: false };
                let mut cursor = std::io::Cursor::new(&thin_pack_bytes);
                let result = handler.ingest_pack(&mut cursor, &no_thin_config);

                prop_assert!(
                    result.is_err(),
                    "ingest_pack with no_thin=true should reject a thin pack with \
                     unresolvable ref-delta bases, but got Ok: {:?}",
                    result
                );
            }

            // --- Test 2: no_thin = false → should succeed (bases in ODB) ---
            {
                let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())
                    .expect("handler should open for thin-test repo (second time)");

                let allow_thin_config = SessionConfig { no_thin: false, atomic: false };
                let mut cursor = std::io::Cursor::new(&thin_pack_bytes);
                let result = handler.ingest_pack(&mut cursor, &allow_thin_config);

                prop_assert!(
                    result.is_ok(),
                    "ingest_pack with no_thin=false should succeed for a thin pack \
                     whose bases exist in the ODB, but got error: {:?}",
                    result
                );

                let outcome = result.expect("already asserted Ok");
                prop_assert!(
                    outcome.object_count >= 1,
                    "ingested thin pack should have at least 1 object, got {}",
                    outcome.object_count
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Delete-only push detection properties
// ---------------------------------------------------------------------------

mod delete_only_detection_properties {
    use super::*;
    use gix_protocol::receive_pack::{Capability, Delegate, RefStatus, Request, UnpackStatus};
    use proptest::prelude::*;

    // Feature: receive-pack-v1-support, Property 8: Delete-only pushes skip pack ingestion and connectivity
    // **Validates: Requirements 7.1, 7.2, 7.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// When ALL updates in a push request have new_id == zero (all deletions),
        /// the handler skips pack ingestion and connectivity checking, and
        /// successfully processes the deletion ref edits.
        #[test]
        fn delete_only_push_skips_pack_and_connectivity(
            ref_count in 1usize..=5,
        ) {
            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let repo = git_init_bare(tmp.path(), "repo.git");

            // Create ref_count commits and point refs at them (simulates existing state).
            let mut commit_oids = Vec::with_capacity(ref_count);
            let mut ref_names = Vec::with_capacity(ref_count);
            for i in 0..ref_count {
                let content = format!("delete-only content {}\n", i);
                let filename = format!("del_{}.txt", i);
                let message = format!("commit for deletion ref {}", i);
                let (commit_oid, _) = create_commit(
                    &repo,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/del_branch_{}", i);
                git_in(&repo, &["update-ref", &ref_name, &commit_oid]);
                ref_names.push(ref_name);
                commit_oids.push(commit_oid);
            }

            // Build a delete-only request: all updates have new_id = null (zero id).
            let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
            let updates: Vec<Update> = (0..ref_count)
                .map(|i| {
                    let old_id = gix_hash::ObjectId::from_hex(commit_oids[i].as_bytes())
                        .expect("commit oid should be valid hex");
                    Update {
                        old_id,
                        new_id: null_id,
                        ref_name: ref_names[i].clone().into(),
                    }
                })
                .collect();

            let request = Request {
                capabilities: vec![Capability {
                    name: "report-status".into(),
                    value: None,
                }],
                updates,
                push_options: Vec::new(),
            };

            let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())
                .expect("handler should open for repo");
            handler.disable_reflog();

            // Provide EMPTY pack data — delete-only should NOT attempt to read any.
            let empty_data: &[u8] = &[];
            let mut cursor = std::io::Cursor::new(empty_data);

            let response = handler
                .receive(&request, &mut cursor)
                .expect("Delegate::receive should succeed for delete-only push");

            // PROPERTY ASSERTION 1: Unpack status is Ok (no pack was needed).
            prop_assert_eq!(
                response.unpack_status,
                UnpackStatus::Ok,
                "delete-only push should yield UnpackStatus::Ok since no pack is consumed"
            );

            // PROPERTY ASSERTION 2: Per-ref statuses present for each deletion.
            prop_assert_eq!(
                response.ref_statuses.len(),
                ref_count,
                "should have exactly one ref status per deletion update"
            );

            // PROPERTY ASSERTION 3: All refs should be Ok (deletions processed).
            for (i, status) in response.ref_statuses.iter().enumerate() {
                match status {
                    RefStatus::Ok { ref_name } => {
                        prop_assert_eq!(
                            ref_name.as_ref() as &[u8],
                            ref_names[i].as_bytes(),
                            "ref status name should match the update at index {}",
                            i
                        );
                    }
                    RefStatus::Rejected { ref_name, message } => {
                        prop_assert!(
                            false,
                            "expected RefStatus::Ok for deletion of {}, got Rejected: {}",
                            ref_name,
                            message
                        );
                    }
                }
            }

            // PROPERTY ASSERTION 4: Verify refs are actually deleted in the repo.
            for ref_name in &ref_names {
                let show_ref_output = std::process::Command::new("git")
                    .args(["show-ref", "--verify", ref_name])
                    .current_dir(&repo)
                    .env("GIT_DIR", &repo)
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .output()
                    .expect("git show-ref should execute");
                prop_assert!(
                    !show_ref_output.status.success(),
                    "ref {} should no longer exist after delete-only push",
                    ref_name
                );
            }
        }

        /// When a push mixes deletions with creations/updates, the handler
        /// follows normal flow — it requires pack data for the non-deletion
        /// updates and would fail if no valid pack data is provided.
        #[test]
        fn mixed_push_requires_normal_flow(
            deletion_count in 1usize..=3,
            creation_count in 1usize..=3,
        ) {
            let tmp = tempfile::tempdir()
                .expect("should be able to create temp directory");
            let repo = git_init_bare(tmp.path(), "repo.git");

            // Create commits for existing refs that will be deleted.
            let mut deletion_oids = Vec::with_capacity(deletion_count);
            let mut deletion_refs = Vec::with_capacity(deletion_count);
            for i in 0..deletion_count {
                let content = format!("mixed delete content {}\n", i);
                let filename = format!("mixed_del_{}.txt", i);
                let message = format!("commit for mixed deletion {}", i);
                let (commit_oid, _) = create_commit(
                    &repo,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/mixed_del_{}", i);
                git_in(&repo, &["update-ref", &ref_name, &commit_oid]);
                deletion_refs.push(ref_name);
                deletion_oids.push(commit_oid);
            }

            // Create commits that will be "pushed" as creations.
            let mut creation_oids = Vec::with_capacity(creation_count);
            let mut creation_refs = Vec::with_capacity(creation_count);
            for i in 0..creation_count {
                let content = format!("mixed create content {}\n", i);
                let filename = format!("mixed_create_{}.txt", i);
                let message = format!("commit for mixed creation {}", i);
                let (commit_oid, _) = create_commit(
                    &repo,
                    content.as_bytes(),
                    &filename,
                    None,
                    &message,
                );
                let ref_name = format!("refs/heads/mixed_create_{}", i);
                creation_refs.push(ref_name);
                creation_oids.push(commit_oid);
            }

            // Build a mixed request: some deletions + some creations.
            let null_id = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
            let mut updates = Vec::with_capacity(deletion_count + creation_count);

            // Deletion updates
            for i in 0..deletion_count {
                let old_id = gix_hash::ObjectId::from_hex(deletion_oids[i].as_bytes())
                    .expect("deletion oid should be valid hex");
                updates.push(Update {
                    old_id,
                    new_id: null_id,
                    ref_name: deletion_refs[i].clone().into(),
                });
            }

            // Creation updates (non-deletion: new_id is not null)
            for i in 0..creation_count {
                let new_id = gix_hash::ObjectId::from_hex(creation_oids[i].as_bytes())
                    .expect("creation oid should be valid hex");
                updates.push(Update {
                    old_id: null_id,
                    new_id,
                    ref_name: creation_refs[i].clone().into(),
                });
            }

            let request = Request {
                capabilities: vec![Capability {
                    name: "report-status".into(),
                    value: None,
                }],
                updates,
                push_options: Vec::new(),
            };

            // Provide INVALID pack data — mixed pushes attempt to ingest pack data.
            // Because there's at least one non-deletion update, the handler should
            // try to read pack data and fail (proving it doesn't skip ingestion).
            let invalid_data = b"not-a-valid-pack";
            let mut cursor = std::io::Cursor::new(invalid_data.as_slice());

            let mut handler = ReceivePackHandler::open(repo.clone(), Options::default())
                .expect("handler should open for repo");
            handler.disable_reflog();

            let response = handler
                .receive(&request, &mut cursor)
                .expect("Delegate::receive should return Ok(Response) even on pack failure");

            // PROPERTY ASSERTION: Mixed push attempts pack ingestion and fails with
            // UnpackStatus::Error (proving it did NOT skip the pack ingestion step).
            match &response.unpack_status {
                UnpackStatus::Error(msg) => {
                    prop_assert!(
                        !msg.is_empty(),
                        "mixed push with invalid pack data should produce an error message"
                    );
                }
                UnpackStatus::Ok => {
                    prop_assert!(
                        false,
                        "mixed push with invalid pack data should NOT succeed — \
                         it should attempt pack ingestion and fail, proving normal flow is used"
                    );
                }
            }

            // All refs should be rejected because the pack ingestion failed.
            prop_assert_eq!(
                response.ref_statuses.len(),
                deletion_count + creation_count,
                "should have one ref status per update even on failure"
            );

            for status in &response.ref_statuses {
                match status {
                    RefStatus::Rejected { .. } => { /* expected */ }
                    RefStatus::Ok { ref_name } => {
                        prop_assert!(
                            false,
                            "expected all refs to be Rejected after pack ingestion failure, \
                             but {} was Ok",
                            ref_name
                        );
                    }
                }
            }
        }
    }
}
