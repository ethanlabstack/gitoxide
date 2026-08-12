# Bugfix Requirements Document

## Introduction

The `negotiate_fetch_with_repository()` function in `gix-protocol`'s upload_pack module does not account for the `done` flag when constructing the acknowledgements section of a protocol v2 fetch response. This causes Smart HTTP stateless clients (which always send `done`) to receive an invalid response: an acknowledgements section without `ready` followed by a packfile section, violating the Git protocol v2 specification. Git clients reject this with "fatal: expected no other sections to be sent after no 'ready'".

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN the client sends a fetch request with `done=true` and haves that match known objects in the repository THEN the system produces acknowledgements containing only `Common(id)` entries without appending `Ready`

1.2 WHEN the client sends a fetch request with `done=true` and no haves (fresh clone scenario) THEN the system produces acknowledgements containing `[Nak]` instead of an empty acknowledgements list

1.3 WHEN the response with acknowledgements lacking `ready` is serialized with a packfile section THEN the wire output contains an `acknowledgments` section without a `ready` line followed by a `packfile` section, violating Git protocol v2

### Expected Behavior (Correct)

2.1 WHEN the client sends a fetch request with `done=true` and haves that match known objects THEN the system SHALL produce acknowledgements ending with `Ready` (i.e., `[Common(id1), ..., Common(idN), Ready]`)

2.2 WHEN the client sends a fetch request with `done=true` and no common haves (no haves at all, or all haves are unknown) THEN the system SHALL produce an empty acknowledgements list so that the serializer omits the acknowledgements section entirely

2.3 WHEN the response is serialized with `done=true` THEN the wire output SHALL either contain an `acknowledgments` section with a `ready` line followed by the `packfile` section, or skip the `acknowledgments` section and proceed directly to the `packfile` section

### Unchanged Behavior (Regression Prevention)

3.1 WHEN the client sends a fetch request with `done=false` and haves that match known objects THEN the system SHALL CONTINUE TO produce acknowledgements containing `Common(id)` for each known have without `Ready` (negotiation continues)

3.2 WHEN the client sends a fetch request with `done=false` and no common haves THEN the system SHALL CONTINUE TO produce acknowledgements containing `[Nak]`

3.3 WHEN the client sends a fetch request with `done=false` THEN all non-acknowledgement output fields (`known_wants`, `missing_wants`, `common_haves`, `wanted_refs`, `unresolved_want_refs`) SHALL CONTINUE TO be computed identically regardless of the `done` flag value

---

## Bug Condition

```pascal
FUNCTION isBugCondition(X)
  INPUT: X of type Fetch (protocol v2 fetch request)
  OUTPUT: boolean

  // Returns true when the done flag is set (Smart HTTP stateless always sends done)
  RETURN X.done == true
END FUNCTION
```

## Property Specification

```pascal
// Property: Fix Checking — Done Flag Correctly Handled
FOR ALL X WHERE isBugCondition(X) DO
  result ← negotiate_fetch_with_repository'(X)
  IF common_haves(X) is non-empty THEN
    ASSERT result.acknowledgements ends with Ready
  ELSE
    ASSERT result.acknowledgements is empty
  END IF
END FOR
```

## Preservation Goal

```pascal
// Property: Preservation Checking — Unchanged When Done Is False
FOR ALL X WHERE NOT isBugCondition(X) DO
  ASSERT negotiate_fetch_with_repository(X) = negotiate_fetch_with_repository'(X)
END FOR
```

This ensures that for all requests where `done=false`, the fixed code produces identical output to the original implementation.
