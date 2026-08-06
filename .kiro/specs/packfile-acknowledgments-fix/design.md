# Packfile Acknowledgments Fix — Bugfix Design

## Overview

The `negotiate_fetch_with_repository()` function in `gix-protocol` never accounts for whether the client sent `done`, resulting in incorrect acknowledgement output. When a client signals negotiation is complete (`done = true`), the server must either include `Acknowledgement::Ready` to signal packfile follows (when common objects exist), or omit the `acknowledgments` section entirely (fresh clone). Currently the function always produces `[Nak]` or `[Common(...)]` regardless of the `done` flag, causing clients to fail with `fatal: expected 'packfile', received 'acknowledgments'`.

The fix is a minimal, breaking API change: read `request.done` within `negotiate_fetch_with_repository()` and adjust the acknowledgements list accordingly. No new types or traits are required.

## Glossary

- **Bug_Condition (C)**: The client has sent `done = true` in the `Fetch` request, signaling negotiation is complete and the server should send pack data
- **Property (P)**: When `done` is true, the acknowledgements list must either include `Ready` (common haves exist) or be empty (fresh clone) so the response conforms to Git protocol v2
- **Preservation**: When `done` is false, the existing behaviour (ACK common lines without Ready, or NAK alone) must remain unchanged
- **`negotiate_fetch_with_repository()`**: The function in `gix-protocol/src/upload_pack.rs` that translates a `Fetch` request into a `FetchNegotiation` containing acknowledgements, wanted-refs, and object classification
- **`write_fetch_response()`**: The function that serializes a `FetchOutput` to the wire; it only emits the `acknowledgments` section when the list is non-empty
- **`Acknowledgement::Ready`**: Enum variant that, when written, produces the `ready` line telling the client that `packfile` follows

## Bug Details

### Bug Condition

The bug manifests when a client sends `done = true` in its fetch request. The `negotiate_fetch_with_repository()` function ignores `request.done` entirely and unconditionally produces either `[Nak]` (no common haves) or `[Common(id1), Common(id2), ...]` (common haves found). Neither output is correct when the server intends to follow up with pack data.

**Formal Specification:**
```
FUNCTION isBugCondition(input)
  INPUT: input of type Fetch request
  OUTPUT: boolean

  RETURN input.done == true
END FUNCTION
```

### Examples

- **Fresh clone with `done`**: Client sends `wants=[X], haves=[], done=true`. Current output: `acknowledgements = [Nak]` → wire emits `acknowledgments\nNAK\n` before `packfile`, client fails. Expected: `acknowledgements = []` → section omitted, response starts with `packfile`.
- **Fetch with common objects and `done`**: Client sends `wants=[X], haves=[A,B], done=true` where A and B exist on server. Current output: `acknowledgements = [Common(A), Common(B)]` → wire emits `acknowledgments\nACK A common\nACK B common\n` without `ready`, client fails. Expected: `acknowledgements = [Common(A), Common(B), Ready]` → wire emits the section with `ready` line, client proceeds to read packfile.
- **Ongoing negotiation (no `done`)**: Client sends `wants=[X], haves=[A], done=false`. Current output: `acknowledgements = [Common(A)]`. This is correct—no `Ready` because negotiation continues.
- **No haves, no `done`**: Client sends `wants=[X], haves=[], done=false`. Current output: `acknowledgements = [Nak]`. This is correct for ongoing negotiation.

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- When `done` is false and common haves exist, produce `[Common(...)]` entries only (no `Ready`)
- When `done` is false and no common haves exist, produce `[Nak]`
- `write_fetch_response()` continues to gate the `acknowledgments` section on `!response.acknowledgements.is_empty()`
- `FetchNegotiation::into_output()` continues to copy acknowledgements directly into `FetchOutput`
- `Delegate::fetch()` implementations that manually construct `FetchOutput` are unaffected (they bypass `negotiate_fetch_with_repository`)
- Pack data emission in the `packfile` section remains unchanged
- All other `FetchNegotiation` fields (`wanted_refs`, `known_wants`, `missing_wants`, `common_haves`, `unresolved_want_refs`) remain unchanged

**Scope:**
All inputs where `request.done == false` must produce identical results to the current implementation. The fix only alters the acknowledgements computation when `request.done == true`.

## Hypothesized Root Cause

Based on the code analysis, the root cause is straightforward:

1. **Missing `done` flag inspection**: The acknowledgements construction block at the end of `negotiate_fetch_with_repository()` does not read `request.done`. It uses only `common_haves.is_empty()` to decide between `[Nak]` and `[Common(...)]`, with no third branch for "done + common haves" or "done + no common haves".

2. **No `Ready` variant usage**: The `Acknowledgement::Ready` variant exists in the enum and `format_acknowledgement_line` already handles it (producing `"ready"`), but nothing in `negotiate_fetch_with_repository()` ever adds it to the output.

3. **No empty-list path for fresh clones**: The current code always produces at least one acknowledgement entry. There is no path that produces an empty `Vec`, which is what `write_fetch_response()` needs to skip the section entirely.

The function signature already has access to `request.done` via the `&Fetch` reference—no additional parameters are needed. The fix is purely in the logic that builds the `acknowledgements` vector.

## Correctness Properties

Property 1: Bug Condition - Ready appended when done with common haves

_For any_ `Fetch` request where `done == true` AND at least one `have` object exists in the repository, the fixed `negotiate_fetch_with_repository` function SHALL produce an acknowledgements list ending with `Acknowledgement::Ready`, preceded by `Acknowledgement::Common(id)` entries for each known have.

**Validates: Requirements 2.1, 2.3**

Property 2: Bug Condition - Empty acknowledgements on fresh clone with done

_For any_ `Fetch` request where `done == true` AND no `have` objects exist in the repository (or haves list is empty), the fixed `negotiate_fetch_with_repository` function SHALL produce an empty acknowledgements list.

**Validates: Requirements 2.2**

Property 3: Preservation - Unchanged behavior when done is false

_For any_ `Fetch` request where `done == false`, the fixed `negotiate_fetch_with_repository` function SHALL produce the same acknowledgements as the original function: `[Nak]` when no common haves exist, or `[Common(id), ...]` when common haves exist, without `Ready`.

**Validates: Requirements 3.1, 3.2**

## Fix Implementation

### Changes Required

**File**: `gix-protocol/src/upload_pack.rs`

**Function**: `negotiate_fetch_with_repository`

**Specific Changes**:

1. **Modify acknowledgements construction logic**: Replace the current two-branch conditional with a three-branch conditional that accounts for `request.done`:

   ```rust
   let acknowledgements = if request.done {
       if common_haves.is_empty() {
           Vec::new()
       } else {
           let mut acks: Vec<Acknowledgement> = common_haves
               .iter()
               .cloned()
               .map(Acknowledgement::Common)
               .collect();
           acks.push(Acknowledgement::Ready);
           acks
       }
   } else if common_haves.is_empty() {
       vec![Acknowledgement::Nak]
   } else {
       common_haves.iter().cloned().map(Acknowledgement::Common).collect()
   };
   ```

2. **No signature change needed**: The function already receives `&Fetch` which contains `done`. No breaking API change to the public function signature is required—just the internal logic changes.

3. **Update function doc-comment**: Add a note that when `request.done` is true, the function signals readiness via `Acknowledgement::Ready` or omits acknowledgements for fresh clones.

4. **Update existing tests**: The existing tests use `..Default::default()` which sets `done: false`, so they remain valid. Add new test cases for `done: true` scenarios.

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bug on unfixed code, then verify the fix works correctly and preserves existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bug BEFORE implementing the fix. Confirm the root cause: `request.done` is simply ignored.

**Test Plan**: Write tests that call `negotiate_fetch_with_repository` with `done: true` and assert correct protocol v2 behavior. Run on UNFIXED code to observe failures.

**Test Cases**:
1. **Done with common haves**: Call with `done: true, haves: [known_id]`—expect `[Common(known_id), Ready]` (will fail: gets `[Common(known_id)]`)
2. **Done with no common haves (fresh clone)**: Call with `done: true, haves: []`—expect empty vec (will fail: gets `[Nak]`)
3. **Done with unknown haves only**: Call with `done: true, haves: [unknown_id]`—expect empty vec (will fail: gets `[Nak]`)
4. **Done with multiple common haves**: Call with `done: true, haves: [id1, id2]` both known—expect `[Common(id1), Common(id2), Ready]` (will fail: gets `[Common(id1), Common(id2)]`)

**Expected Counterexamples**:
- Acknowledgements list never contains `Ready`
- Acknowledgements list is never empty (always at least `[Nak]`)
- Root cause confirmed: `request.done` field is never read

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds (`done == true`), the fixed function produces the expected behavior.

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition(input) DO
  result := negotiate_fetch_with_repository_fixed(input)
  IF input.common_haves IS NOT EMPTY THEN
    ASSERT result.acknowledgements ENDS WITH Acknowledgement::Ready
    ASSERT ALL preceding entries ARE Acknowledgement::Common(id)
  ELSE
    ASSERT result.acknowledgements IS EMPTY
  END IF
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold (`done == false`), the fixed function produces the same result as the original function.

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition(input) DO
  ASSERT negotiate_fetch_with_repository_original(input)
       = negotiate_fetch_with_repository_fixed(input)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many combinations of wants, haves, and object-existence predicates
- It catches edge cases with duplicate haves, empty lists, and mixed known/unknown objects
- It provides strong guarantees that the `done == false` path is truly unchanged

**Test Plan**: Observe behavior on UNFIXED code first for `done: false` requests, then write property-based tests capturing that behavior remains stable after the fix.

**Test Cases**:
1. **No-done with common haves**: Verify `[Common(...)]` output unchanged
2. **No-done with no common haves**: Verify `[Nak]` output unchanged
3. **Non-acknowledgement fields preserved**: Verify `known_wants`, `missing_wants`, `wanted_refs`, `unresolved_want_refs`, `common_haves` are identical regardless of `done` flag

### Unit Tests

- Test `done: true` with common haves → acknowledgements end with `Ready`
- Test `done: true` with no haves → empty acknowledgements
- Test `done: true` with all-unknown haves → empty acknowledgements
- Test `done: true` with mix of known/unknown haves → `Common` for known only, then `Ready`
- Test `done: false` with common haves → `[Common(...)]` only (existing test, unchanged)
- Test `done: false` with no common haves → `[Nak]` (existing test, unchanged)

### Property-Based Tests

- Generate random `Fetch` requests with `done: true` and arbitrary haves/wants; verify acknowledgements satisfy the protocol invariant (ends with `Ready` if non-empty, else is empty)
- Generate random `Fetch` requests with `done: false` and arbitrary haves/wants; verify acknowledgements match original implementation (never contains `Ready`, never empty)
- Generate random object-existence predicates and verify `common_haves` field is consistent with acknowledgement entries in both done and not-done paths

### Integration Tests

- End-to-end test: simulate a fresh clone (`done: true`, no haves) through `serve_v2` and verify the wire output contains `packfile` section without preceding `acknowledgments`
- End-to-end test: simulate a fetch with common objects (`done: true`, haves present) through `serve_v2` and verify wire output contains `acknowledgments` section with `ready` line followed by `packfile`
- End-to-end test: simulate ongoing negotiation (`done: false`) and verify wire output matches current behavior exactly
