# Implementation Plan

## Overview

Fix the `negotiate_fetch_with_repository()` function in `gix-protocol/src/upload_pack.rs` to correctly handle the `done` flag in Git protocol v2 fetch requests. The fix replaces the existing two-branch acknowledgements conditional with a three-branch one that accounts for `request.done`, producing `Ready` when common haves exist, an empty vector when no common haves exist (fresh clone), and preserving the original behavior when `done=false`.

## Tasks

- [-] 1. Write bug condition exploration test
  - **Property 1: Bug Condition** - Done Flag Ignored in Acknowledgements Construction
  - **CRITICAL**: This test MUST FAIL on unfixed code - failure confirms the bug exists
  - **DO NOT attempt to fix the test or the code when it fails**
  - **NOTE**: This test encodes the expected behavior - it will validate the fix when it passes after implementation
  - **GOAL**: Surface counterexamples that demonstrate `request.done` is ignored when building the acknowledgements vector
  - **Scoped PBT Approach**: Scope the property to concrete failing cases: `done=true` with known haves, and `done=true` with no haves
  - Test file: `gix-protocol/src/upload_pack.rs` (in existing `mod tests` block)
  - Follow existing test patterns: use `temporary_ref_store`, `object_id`, `Fetch { ..Default::default() }` with `done: true`
  - Test case 1: `done=true` with a single common have — assert acknowledgements end with `Acknowledgement::Ready`
  - Test case 2: `done=true` with no haves (fresh clone scenario) — assert acknowledgements is empty `Vec`
  - Test case 3: `done=true` with all-unknown haves — assert acknowledgements is empty `Vec`
  - Test case 4: `done=true` with a mix of known/unknown haves — assert `[Common(known_id), Ready]`
  - Run tests on UNFIXED code with `cargo test -p gix-protocol negotiate_fetch`
  - **EXPECTED OUTCOME**: Tests FAIL (this is correct — confirms `done` flag is never consulted, acknowledgements never contain `Ready`, and are never empty)
  - Document counterexamples: e.g., "with `done: true, haves: [known_id]` got `[Common(known_id)]` instead of `[Common(known_id), Ready]`"
  - Mark task complete when tests are written, run, and failure is documented
  - _Requirements: 1.1, 1.2, 2.1, 2.2_

- [~] 2. Write preservation property tests (BEFORE implementing fix)
  - **Property 2: Preservation** - Unchanged Behavior When Done Is False
  - **IMPORTANT**: Follow observation-first methodology
  - Test file: `gix-protocol/src/upload_pack.rs` (in existing `mod tests` block)
  - Observe: `negotiate_fetch_with_repository` with `done: false` and common haves produces `[Common(...)]` on unfixed code
  - Observe: `negotiate_fetch_with_repository` with `done: false` and no common haves produces `[Nak]` on unfixed code
  - Write property-based tests covering the `done == false` input domain (from Preservation Requirements in design):
    - For all requests where `done == false` and at least one have is known: acknowledgements equals `[Common(id) for each unique known have]` — never contains `Ready`, never empty
    - For all requests where `done == false` and no haves are known: acknowledgements equals `[Nak]`
    - Non-acknowledgement fields (`known_wants`, `missing_wants`, `common_haves`, `wanted_refs`, `unresolved_want_refs`) are unaffected by `done` flag value
  - Use multiple concrete test cases with varying inputs as property-based approximation (project uses standard `#[test]`, not a PBT framework)
  - Include test for duplicate haves deduplication (observed behavior)
  - Verify tests PASS on UNFIXED code
  - **EXPECTED OUTCOME**: Tests PASS (confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing on unfixed code
  - _Requirements: 3.1, 3.2, 3.3_

- [ ] 3. Fix for done flag ignored in acknowledgements construction

  - [~] 3.1 Implement the fix in `negotiate_fetch_with_repository`
    - File: `gix-protocol/src/upload_pack.rs`
    - Replace the two-branch acknowledgements conditional with a three-branch conditional on `request.done`:
      - `request.done == true` + `common_haves` non-empty → `[Common(id1), ..., Common(idN), Ready]`
      - `request.done == true` + `common_haves` empty → `Vec::new()` (empty — serializer omits acknowledgments section)
      - `request.done == false` + `common_haves` non-empty → `[Common(id1), ..., Common(idN)]` (no `Ready`)
      - `request.done == false` + `common_haves` empty → `[Nak]`
    - No function signature change needed (`&Fetch` already contains `done: bool`)
    - Update the function's doc-comment to note `done` flag handling
    - _Bug_Condition: isBugCondition(input) where input.done == true_
    - _Expected_Behavior: when done=true, acknowledgements end with Ready (common haves exist) or are empty (fresh clone)_
    - _Preservation: when done=false, output is identical to current implementation_
    - _Requirements: 2.1, 2.2, 2.3, 3.1, 3.2_

  - [~] 3.2 Verify bug condition exploration test now passes
    - **Property 1: Expected Behavior** - Done Flag Correctly Handled
    - **IMPORTANT**: Re-run the SAME tests from task 1 - do NOT write new tests
    - The tests from task 1 encode the expected behavior (Ready appended, empty vec for fresh clone)
    - When these tests pass, it confirms the expected behavior is satisfied
    - Run `cargo test -p gix-protocol negotiate_fetch`
    - **EXPECTED OUTCOME**: Tests PASS (confirms bug is fixed)
    - _Requirements: 2.1, 2.2, 2.3_

  - [~] 3.3 Verify preservation tests still pass
    - **Property 2: Preservation** - Unchanged Behavior When Done Is False
    - **IMPORTANT**: Re-run the SAME tests from task 2 - do NOT write new tests
    - Run `cargo test -p gix-protocol negotiate_fetch`
    - **EXPECTED OUTCOME**: Tests PASS (confirms no regressions for done=false paths)
    - Confirm all tests still pass after fix (no regressions)
    - _Requirements: 3.1, 3.2_

  - [~] 3.4 Add integration test through `serve_v2` for done=true with pack data
    - Test file: `gix-protocol/src/upload_pack.rs` (in existing `mod tests` block)
    - Follow existing `serve_fetch_with_pack_sideband` test pattern using `MockDelegate`, `StreamingPeekableIter`, and packetline parsing
    - Simulate fresh clone: `done=true`, no haves → delegate returns `FetchOutput::new(pack_data)` with empty acknowledgements
    - Verify wire output contains `packfile` section WITHOUT preceding `acknowledgments` section
    - Simulate incremental fetch: `done=true`, common haves → delegate returns `FetchOutput` with `[Common(id), Ready]` acknowledgements and pack data
    - Verify wire output contains `acknowledgments` section with `ready` line followed by `packfile` section
    - Use `next_text_line`, `expect_delimiter`, and `next_band_data` helpers from existing test infrastructure
    - _Requirements: 2.1, 2.2, 2.3, 3.3_

- [~] 4. Checkpoint - Ensure all tests pass
  - Run `cargo test -p gix-protocol` to verify all tests pass
  - Run `cargo clippy -p gix-protocol` to verify no warnings
  - Ensure all tests pass, ask the user if questions arise.


## Task Dependency Graph

```json
{
  "waves": [
    { "tasks": ["1", "2"] },
    { "tasks": ["3.1"] },
    { "tasks": ["3.2", "3.3", "3.4"] },
    { "tasks": ["4"] }
  ]
}
```

## Notes

- Tests 1 and 2 MUST be written and run BEFORE implementing the fix (task 3.1)
- Task 1 is expected to FAIL on unfixed code — this confirms the bug exists
- Task 2 is expected to PASS on unfixed code — this captures baseline behavior
- The project uses standard `#[test]` functions; property-based coverage is achieved through multiple targeted test cases covering the input domain
- All tests live in `gix-protocol/src/upload_pack.rs` in the existing `mod tests` block
- Follow AGENTS.md conventions: no `.unwrap()`, use `?` with `Box<dyn std::error::Error>`, use `.expect("context")` only where relevant
- Use existing test helpers: `temporary_ref_store()`, `object_id()`, `temporary_object_store_with_linear_history()`, `request_bytes()`, `next_text_line()`, `expect_delimiter()`, `next_band_data()`
