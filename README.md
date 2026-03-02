# Distributed High-Throughput Rate Limiter

## Overview

A rate limiter for a fleet of web servers that tracks request counts across all instances using a shared `DistributedKeyValueStore`. Designed to handle 100M+ requests/minute to the same key without creating hot keys in the store.

## How it works

The core idea is **local batching**. Instead of calling the distributed store on every request (which would be a network call each time), each server accumulates counts in a local `AtomicInteger` and flushes them to the store periodically.

Once the global count exceeds the limit, all subsequent requests are denied locally with a single `volatile` read — no atomics, no network call. This is how the vast majority of requests are handled under sustained load.

### Flush triggers

Flushes happen on two conditions:
- **Count-based**: when the local count reaches `limit / batchDivisor` (default divisor is 10)
- **Time-based**: if more than 1 second passes without a count-based flush — this is a safety net for low-traffic keys that would otherwise never reach the batch threshold

### Window strategy

Fixed 60-second windows aligned to epoch minutes. The window ID is embedded in the store key (e.g. `clientId:27548321`), so each window gets its own counter with a 60-second TTL.

## Design decisions

**Local batching over per-request RPCs** — At 100M req/min, calling the store on every request would create an extreme hot key. Batching reduces store calls from O(requests) to O(requests / batchSize).

**Optimistic global update during flush** — When a flush starts, `pending` is reset to 0. Without bumping `knownGlobal` immediately, concurrent threads would see `knownGlobal + 0` and massively underestimate, over-allowing during the network round trip. The optimistic bump fills that gap until the real response arrives.

**Decrement on local deny** — When a request is denied based on the local estimate, I undo the `pending` increment. Without this, denied requests would accumulate, get flushed, and inflate the global count — making the limiter increasingly restrictive within a window.

**Fail-open on store errors** — If the store is down, requests are allowed rather than blocked. The requirement says clients must make "at least" their configured limit, so blocking legitimate traffic on a transient store failure is the worse outcome.

**`volatile` for `knownGlobal` instead of `AtomicInteger`** — Only one thread writes to it at a time (guarded by the `flushing` CAS gate), everyone else just reads. So I only need visibility, not atomicity. `volatile` is cheaper.

**`AtomicInteger` for `pending` instead of `LongAdder`** — `LongAdder` is better under contention for pure increments, but its `sumThenReset()` is not atomic with concurrent additions — counts would be lost between the sum and the reset. `AtomicInteger.getAndSet(0)` is atomic.

**Fixed windows instead of sliding windows** — A sliding window (weighting current + previous window) would be smoother at boundaries but roughly doubles state and store interactions. The problem allows approximate counting, so fixed windows keep it simple.

## Trade-offs

| Decision | Upside | Downside |
|---|---|---|
| Batching | Handles 100M req/min, no hot keys | Overshoot up to `batchThreshold × numServers` at window start |
| Fixed windows | Simple, one counter per window | Burst at boundary can temporarily allow ~2x the limit |
| Fail-open | No false negatives on store outage | Briefly allows over-limit traffic when store is down |
| Optimistic update | Prevents undercount during flush | Brief inconsistency if flush fails (rolled back) |
| Single flush thread per key | No contention on store writes | Slightly delayed global visibility if flush is slow |

## What I'd improve with more time

- **Sliding window** — weight current + previous window counters for smoother boundary behavior
- **Adaptive batch size** — start small for accuracy, grow when well under the limit
- **Metrics** — expose allowed/denied/flush/error counters for operational visibility
- **Graceful shutdown** — flush all pending counts before stopping so they're not lost

## Running

```bash
mvn test
```

Requires Java 21+.
