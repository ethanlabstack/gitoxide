---
inclusion: auto
---

# Git Protocol V2 Fetch Response Framing (Gold Standard)

Source: https://git-scm.com/docs/gitprotocol-v2/2.55.0

## Fetch output grammar

```
output = acknowledgements flush-pkt |
         [acknowledgments delim-pkt]
         [shallow-info delim-pkt]
         [wanted-refs delim-pkt]
         [packfile-uris delim-pkt]
         packfile flush-pkt
```

Two forms:

1. **No pack follows** — `acknowledgements flush-pkt`  
   Ack section terminated directly by flush (`0000`). No delimiter.

2. **Pack follows** — sections separated by `delim-pkt` (`0001`), ending with `packfile flush-pkt`  
   Each optional section uses a delimiter only when a packfile section follows.

## Acknowledgments section rules

- Omitted entirely when client sends `done` and server is ready (fresh clone or ready state).
- Contains `NAK` when no common objects found (ongoing negotiation).
- Contains `ACK <oid>` for each common object.
- Contains `ready` when server has found a cut point and will send a pack.
- Cannot have both `ACK` lines and `NAK` in the same response.
- Server MAY omit `ACK` lines when sending `ready` (optimization).

## Packet-line semantics

- `0000` (flush-pkt) — end of message
- `0001` (delim-pkt) — separates sections within a message
- `0002` (response-end-pkt) — end of response for stateless connections

## Key constraint

When `has_pack_data` is false, the last metadata section MUST be followed by flush (`0000`), NOT delimiter (`0001`). Delimiters signal "more sections follow" — flush signals "response complete."
