# Requirements Document

## Introduction

This feature introduces an experimental gate that allows using the built-in in-process `upload-pack` implementation (provided by `gix-protocol`'s `upload_pack` module) instead of spawning an external `git-upload-pack` process when cloning or fetching from `file://` URLs. The goal is to validate the built-in upload-pack path under a feature flag before promoting it to a stable default, starting with the `gix` CLI as a dev tool.

## Glossary

- **Built_In_Upload_Pack**: The in-process upload-pack implementation provided by `gix-protocol`'s `upload_pack` module (e.g., `serve_v2()`, `negotiate_fetch_with_repository()`), which handles pack negotiation and generation without spawning an external process.
- **External_Upload_Pack**: The current behavior where a `git-upload-pack` child process is spawned via `SpawnProcessOnDemand` to serve pack data for `file://` URLs.
- **Experimental_Feature_Flag**: A Cargo feature (e.g., `experimental`) on the `gix` crate that gates compilation of experimental, not-yet-stable functionality.
- **Runtime_Option**: A mechanism (CLI flag, configuration key, or environment variable) that allows users to opt-in to the Built_In_Upload_Pack at runtime, even when the Experimental_Feature_Flag is compiled in.
- **Gix_CLI**: The `gix` binary produced by the `gitoxide` crate, used as a plumbing dev tool.
- **Journey_Test**: An end-to-end shell-based test (`tests/journey/gix.sh`) that exercises CLI behavior against real git repositories.

## Requirements

### Requirement 1: Experimental Cargo Feature Flag

**User Story:** As a crate maintainer, I want an experimental Cargo feature flag on the `gix` crate, so that built-in upload-pack support can be compiled in without affecting stable builds.

#### Acceptance Criteria

1. THE Gix_CLI crate SHALL expose a Cargo feature named `experimental` (or similarly prefixed) that gates compilation of Built_In_Upload_Pack integration code.
2. IF the `experimental` feature is not enabled, THEN THE Gix_CLI crate SHALL not compile any Built_In_Upload_Pack integration code, and any module or symbol related to it SHALL be absent from the build.
3. IF the `experimental` feature is enabled, THEN THE Gix_CLI crate SHALL compile the Built_In_Upload_Pack integration code and expose the Runtime_Option as a recognized CLI flag visible in `--help` output.
4. THE `gitoxide` workspace crate (which produces the `gix` binary) SHALL enable the `experimental` feature by default in its `max` and `max-pure` feature sets, as it is a dev tool.
5. IF a build is performed without the `experimental` feature and application code references Built_In_Upload_Pack symbols, THEN THE build SHALL fail with a compilation error.

### Requirement 2: Runtime Option for Built-In Upload-Pack

**User Story:** As a developer, I want a runtime option to choose between the external `git-upload-pack` process and the built-in implementation, so that I can test the in-process path without changing my default behavior.

#### Acceptance Criteria

1. WHEN the Experimental_Feature_Flag is compiled in, THE Gix_CLI SHALL provide a CLI flag that activates the Built_In_Upload_Pack for `file://` URL operations in commands that perform fetches (e.g., `clone`, `fetch`).
2. WHEN the runtime option is not set, THE Gix_CLI SHALL use External_Upload_Pack as the default behavior for `file://` URLs.
3. WHEN the runtime option is set and the target URL uses the `file://` scheme, THE Gix_CLI SHALL use the Built_In_Upload_Pack instead of spawning a `git-upload-pack` process.
4. IF the runtime option is set but the target URL does not use the `file://` scheme, THEN THE Gix_CLI SHALL ignore the flag and proceed with the standard transport for that URL scheme.
5. IF the runtime option is set but the Experimental_Feature_Flag is not compiled in, THEN THE Gix_CLI SHALL exit with a non-zero exit code and print an error message to stderr indicating that the feature requires the `experimental` build.
6. WHEN the runtime option is set and the Built_In_Upload_Pack completes successfully, THE Gix_CLI SHALL produce the same observable outcome (refs, pack data, working tree) as when using External_Upload_Pack for the same repository state.

### Requirement 3: Built-In Upload-Pack Transport Integration

**User Story:** As a developer, I want the built-in upload-pack to integrate with the existing transport layer for `file://` URLs, so that clone and fetch operations work identically whether using the external or built-in path.

#### Acceptance Criteria

1. WHEN the Built_In_Upload_Pack is activated via the Runtime_Option, THE transport layer SHALL open the target repository in-process and use `gix-protocol`'s `upload_pack` module to serve pack data using protocol V2.
2. WHEN a `file://` clone or fetch is performed with the Built_In_Upload_Pack, THE transport layer SHALL produce the same set of advertised refs (including symref targets and peeled object IDs) and a pack containing the same objects as External_Upload_Pack for the same repository state.
3. IF the Built_In_Upload_Pack encounters an error opening the target repository or resolving requested refs, THEN THE transport layer SHALL return an error that includes the failing repository path or ref name and a description of the failure cause, without terminating the calling process.
4. WHEN a `file://` fetch is performed with the Built_In_Upload_Pack and the client already has common objects, THE transport layer SHALL perform pack negotiation via `gix-protocol`'s `negotiate_fetch_with_repository` and produce a pack containing only the objects not already held by the client.

### Requirement 4: Default Experimental Features in gix CLI Builds

**User Story:** As a developer building `gix` for local use, I want experimental features enabled by default, so that I can test new functionality without extra build flags.

#### Acceptance Criteria

1. THE `gitoxide` workspace crate SHALL include the `experimental` feature in its `max` feature set.
2. THE `gitoxide` workspace crate SHALL include the `experimental` feature in its `max-pure` feature set.
3. THE `gitoxide` workspace crate SHALL NOT include the `experimental` feature in the `small`, `lean`, or `lean-async` feature sets, as those target production-oriented minimal builds.

### Requirement 5: Journey Test for Built-In Upload-Pack Clone

**User Story:** As a developer, I want a journey test that clones a repository using the built-in upload-pack option, so that the feature is validated end-to-end in CI.

#### Acceptance Criteria

1. WHEN the journey tests run with `max` or `max-pure` build kinds, THE Journey_Test suite SHALL include a test that clones a `file://` repository using the Built_In_Upload_Pack runtime option.
2. WHEN the Built_In_Upload_Pack clone test executes, THE Journey_Test SHALL verify that the clone command exits successfully (exit code 0) and produces a repository where `git rev-parse HEAD` resolves to a valid commit and all remote-tracking refs match those advertised by the source repository.
3. WHEN the Built_In_Upload_Pack clone test executes, THE Journey_Test SHALL verify that the set of refs and their resolved object IDs in the cloned repository are identical to those produced by cloning the same source repository using External_Upload_Pack.
4. WHEN the Built_In_Upload_Pack clone test executes against a source repository that contains at least one branch and one tag, THE Journey_Test SHALL confirm that both the branch and tag refs are present in the clone output.
