# Upload-Pack Done Negotiation Fix — Bugfix Design

## Overview

The `negotiate_fetch_with_repository()` function in `gix-protocol`'s `upload_pack` module ignores the `done` flag when constructing the acknowledgements section. Smart HTTP stateless clients always send `done`, causing them to receive an invalid protocol v2 response: an `acknowledgments` section without `ready` followed by a `packfile` section. Git clients reject this with "fatal: expected no other sections to be sent after no 'ready'". The fix adds a conditional branch on `request.done` that either appends `Ready` to acknowledgements (when common haves exist) or produces an empty acknowledgements list (for fresh clones), leaving `done=false` behavior unchanged.

## Glossary

- **Bug_Condition (C)**: The condition that triggers the bug — when `request.done == true` in a protocol v2 fetch request (Smart HTTP stateless clients always set this)
- **Property (P)**: The desired behavior when `done=true` — acknowledgements end with `Ready` if common haves exist, or are empty (omitted on wire) if no common haves exist
- **Preservation**: Existing `done=false` behavior must remain unchanged — `Nak` for no common haves, `Common(id)` list without `Ready` for ongoing negotiation
- **`negotiate_fetch_with_repository()`**: The function in `gix-protocol/src/upload_pack.rs` that resolves have/want lines against repository data and produces a `FetchNegotiation` result
- **`FetchNegotiation`**: The output struct containing `acknowledgements`, `wanted_refs`, `known_wants`, `missing_wants`, `common_haves`, and `unresolved_want_refs`
- **`Acknowledgement`**: An enum with variants `Common(ObjectId)`, `Ready`, and `Nak`
- **`write_fetch_response()`**: The function that serializes `FetchOutput` (including acknowledgements) to the Git protocol v2 wire format

## Bug Details

### Bug Condition

The bug manifests when a client sends a protocol v2 fetch request with `done=true`. The `negotiate_fetch_with_repository()` function does not check the `done` flag when constructing the `acknowledgements` vector, so it always produces `[Common(id1), ..., Common(idN)]` (no `Ready`) when haves match, or `[Nak]` when no haves match — regardless of `done`.

When this output is serialized by `write_fetch_response()` with pack data, the wire format contains an `acknowledgments` section without a `ready` line followed by a `packfile` section, violating Git protocol v2.

**Formal Specification:**
```
FUNCTION isBugCondition(input)
  INPUT: input of type Fetch (protocol v2 fetch request)
  OUTPUT: boolean

  RETURN input.done == true
END FUNCTION
```

### Examples

- **Done + common haves (Smart HTTP incremental fetch)**: Client sends `want X`, `have Y` (known), `done`. Unfixed code returns `acknowledgements = [Common(Y)]`. Wire output has `acknowledgments` section without `ready`, then `packfile` section → client rejects with fatal error. Fixed code returns `acknowledgements = [Common(Y), Ready]`.

- **Done + no common haves (Smart HTTP fresh clone)**: Client sends `want X`, `done`. Unfixed code returns `acknowledgements = [Nak]`. Wire output has `acknowledgments` section with `NAK`, then `packfile` section → client rejects. Fixed code returns `acknowledgements = []` (empty), so the serializer skips the acknowledgments section entirely.

- **Done + all unknown haves**: Client sends `want X`, `have Z` (unknown), `done`. Unfixed code returns `acknowledgements = [Nak]`. Fixed code returns `acknowledgements = []`.

- **No done + common haves (multi-round negotiation)**: Client sends `want X`, `have Y` (known), no `done`. Both unfixed and fixed code return `acknowledgements = [Common(Y)]`. No `packfile` section follows. Behavior unchanged.

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**
- When `done=false` and common haves exist, acknowledgements are `[Common(id1), ..., Common(idN)]` without `Ready` (negotiation continues)
- When `done=false` and no common haves exist, acknowledgements are `[Nak]`
- All non-acknowledgement fields (`known_wants`, `missing_wants`, `common_haves`, `wanted_refs`, `unresolved_want_refs`) are computed identically regardless of `done`
- `write_fetch_response()` serialization logic is unchanged — it simply reflects the `acknowledgements` vector it receives

**Scope:**
All inputs where `done=false` should be completely unaffected by this fix. This includes:
- Multi-round negotiation fetch requests (stateful connections)
- Any request without the `done` argument
- All non-fetch protocol v2 commands (`ls-refs`)

## Hypothesized Root Cause

Based on the bug description, the root cause is:

1. **Missing conditional branch on `done` flag**: The original `negotiate_fetch_with_repository()` function constructs acknowledgements using only the presence/absence of common haves, without consulting `request.done`. The logic was:
   - Common haves exist → `[Common(id1), ..., Common(idN)]`
   - No common haves → `[Nak]`

   This is correct for multi-round negotiation (`done=false`) but incorrect when `done=true`, where the Git protocol v2 spec requires:
   - Common haves exist → acknowledgements MUST end with `ready`
   - No common haves → acknowledgements section MUST be omitted entirely (empty vector)

2. **Protocol v2 spec misunderstanding**: The `done` flag signals that the client will not send further rounds. The server must either respond with `ready` (I have data for you) or omit the acknowledgements section (proceed directly to packfile for fresh clones). The original code treats all cases uniformly as "ongoing negotiation."

## Correctness Properties

Property 1: Bug Condition - Done With Common Haves Produces Ready

_For any_ fetch request where `done=true` and the repository contains at least one object matching the client's `have` lines (common haves is non-empty), the fixed `negotiate_fetch_with_repository()` SHALL produce an acknowledgements vector that ends with `Acknowledgement::Ready`, preceded by `Common(id)` entries for each known have.

**Validates: Requirements 2.1, 2.3**

Property 2: Bug Condition - Done Without Common Haves Produces Empty Acknowledgements

_For any_ fetch request where `done=true` and no objects from the client's `have` lines are known to the repository (common haves is empty — either no haves sent or all unknown), the fixed `negotiate_fetch_with_repository()` SHALL produce an empty acknowledgements vector so that `write_fetch_response()` omits the acknowledgements section on wire.

**Validates: Requirements 2.2, 2.3**

Property 3: Preservation - No-Done Behavior Unchanged

_For any_ fetch request where `done=false`, the fixed `negotiate_fetch_with_repository()` SHALL produce the same `FetchNegotiation` result as the original function, preserving `[Nak]` for no common haves and `[Common(id1), ..., Common(idN)]` without `Ready` for ongoing negotiation.

**Validates: Requirements 3.1, 3.2, 3.3**

## Fix Implementation

### Changes Required

Assuming our root cause analysis is correct:

**File**: `gix-protocol/src/upload_pack.rs`

**Function**: `negotiate_fetch_with_repository()`

**Specific Changes**:

1. **Add conditional branch on `request.done`**: After computing `common_haves`, add an `if request.done { ... } else { ... }` block for constructing the `acknowledgements` vector.

2. **Done + common haves path**: When `request.done == true && !common_haves.is_empty()`, produce:
   ```rust
   let mut acks: Vec<Acknowledgement> = common_haves
       .iter()
       .copied()
       .map(Acknowledgement::Common)
       .collect();
   acks.push(Acknowledgement::Ready);
   acks
   ```

3. **Done + no common haves path**: When `request.done == true && common_haves.is_empty()`, produce:
   ```rust
   Vec::new()  // empty → serializer omits acknowledgments section
   ```

4. **No-done path (preserved)**: When `request.done == false`, keep the existing logic:
   - Common haves exist → `common_haves.iter().copied().map(Acknowledgement::Common).collect()`
   - No common haves → `vec![Acknowledgement::Nak]`

5. **Update function doc comment**: Add documentation noting the `done` flag behavior so future maintainers understand the branching.

## Testing Strategy

### Validation Approach

The testing strategy follows a two-phase approach: first, surface counterexamples that demonstrate the bug on unfixed code, then verify the fix works correctly and preserves existing behavior.

### Exploratory Bug Condition Checking

**Goal**: Surface counterexamples that demonstrate the bug BEFORE implementing the fix. Confirm or refute the root cause analysis. If we refute, we will need to re-hypothesize.

**Test Plan**: Call `negotiate_fetch_with_repository()` with `done=true` requests against a mock object store and assert that the acknowledgements vector is correctly formed. Run these tests on the UNFIXED code to observe failures (no `Ready` appended, `Nak` instead of empty).

**Test Cases**:
1. **Done + single common have**: `done=true`, one `have` that exists → assert `acknowledgements` ends with `Ready` (will fail on unfixed code — no `Ready`)
2. **Done + multiple common haves**: `done=true`, three haves that exist → assert `acknowledgements` ends with `Ready` (will fail on unfixed code)
3. **Done + no haves (fresh clone)**: `done=true`, no `have` lines → assert `acknowledgements` is empty (will fail on unfixed code — produces `[Nak]`)
4. **Done + all unknown haves**: `done=true`, haves that don't exist in repository → assert `acknowledgements` is empty (will fail on unfixed code — produces `[Nak]`)

**Expected Counterexamples**:
- `acknowledgements` lacks `Ready` when `done=true` and common haves exist
- `acknowledgements` contains `[Nak]` when `done=true` and no common haves, instead of being empty

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed function produces the expected behavior.

**Pseudocode:**
```
FOR ALL input WHERE isBugCondition(input) DO
  result := negotiate_fetch_with_repository_fixed(input)
  IF common_haves(result) is non-empty THEN
    ASSERT result.acknowledgements.last() == Ready
    ASSERT all preceding entries are Common(id)
  ELSE
    ASSERT result.acknowledgements is empty
  END IF
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed function produces the same result as the original function.

**Pseudocode:**
```
FOR ALL input WHERE NOT isBugCondition(input) DO
  ASSERT negotiate_fetch_with_repository_original(input) = negotiate_fetch_with_repository_fixed(input)
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:
- It generates many combinations of have/want lists automatically
- It catches edge cases like duplicate haves, empty want lists, or mixed known/unknown object IDs
- It provides strong guarantees that `done=false` behavior is byte-identical

**Test Plan**: Capture behavior on UNFIXED code for `done=false` requests, then write property-based tests ensuring the fixed code produces identical results for the same inputs.

**Test Cases**:
1. **No-done + common haves preservation**: Generate random `Fetch` with `done=false` and random haves (some known), verify `acknowledgements = [Common(id1), ..., Common(idN)]`
2. **No-done + no common haves preservation**: Generate random `Fetch` with `done=false` and no matching haves, verify `acknowledgements = [Nak]`
3. **Non-acknowledgement fields preservation**: Generate random `Fetch` with any `done` value, verify `known_wants`, `missing_wants`, `common_haves`, `wanted_refs`, `unresolved_want_refs` are computed identically

### Unit Tests

- Test `negotiate_fetch_with_repository()` with `done=true`, single common have → ends with `Ready`
- Test `negotiate_fetch_with_repository()` with `done=true`, no common haves → empty
- Test `negotiate_fetch_with_repository()` with `done=false`, common haves → no `Ready`, just `Common` entries
- Test `negotiate_fetch_with_repository()` with `done=false`, no common haves → `[Nak]`
- Test wire-level output via `serve_v2()` with `done=true` fresh clone → no `acknowledgments` section
- Test wire-level output via `serve_v2()` with `done=true` + common → `acknowledgments` with `ready` then `packfile`

### Property-Based Tests

- Generate random `Fetch` requests with `done=true` and random object existence, assert the correctness property holds for all generated inputs
- Generate random `Fetch` requests with `done=false` and random object existence, assert preservation property holds — output matches the unfixed function
- Generate random combinations of haves/wants with varying counts (0, 1, many) and verify acknowledgements are well-formed

### Integration Tests

- End-to-end `serve_v2()` test: Smart HTTP fresh clone flow (done=true, no haves) produces valid wire output parseable by a Git client
- End-to-end `serve_v2()` test: Smart HTTP incremental fetch (done=true, common haves) produces wire output with `ready` line
- End-to-end `serve_v2()` test: Stateful multi-round negotiation (done=false) produces wire output without `ready` and without `packfile`
