package ratelimiter;

import java.time.Clock;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rate limiter designed for a fleet of servers. Instead of hitting the distributed
 * store on every request (which would create hot keys at 100M+ req/min), we batch
 * counts locally and flush periodically.
 *
 * Uses fixed 60-second windows. Approximate counting: clients can make at least their
 * configured limit, but may slightly exceed it during convergence at the start of each
 * window (bounded by batchThreshold * number_of_servers).
 *
 * Thread-safe via ConcurrentHashMap + AtomicInteger + volatile fields.
 */
public class DistributedHighThroughputRateLimiter {

    static final int WINDOW_SECONDS = 60;
    private static final long WINDOW_MS = WINDOW_SECONDS * 1000L;
    private static final int DEFAULT_BATCH_DIVISOR = 10;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 1000;

    private final DistributedKeyValueStore store;
    private final ConcurrentHashMap<String, WindowState> states;
    private final ScheduledExecutorService cleanup;
    private final Clock clock;
    private final int batchDivisor;
    private final long flushIntervalMs;

    /** Local state for one key in one time window on this server. */
    static class WindowState {
        final AtomicInteger pending = new AtomicInteger(0);    // not yet flushed
        volatile int knownGlobal = 0;                          // last value from store
        final AtomicBoolean flushing = new AtomicBoolean(false);
        volatile long lastFlushMs;
        final long windowId;

        WindowState(long windowId, long nowMs) {
            this.windowId = windowId;
            this.lastFlushMs = nowMs;
        }
    }

    public DistributedHighThroughputRateLimiter(DistributedKeyValueStore store) {
        this(store, Clock.systemUTC(), DEFAULT_BATCH_DIVISOR, DEFAULT_FLUSH_INTERVAL_MS);
    }

    /**
     * @param batchDivisor    batch size = limit / batchDivisor. Higher → more accurate, more RPCs.
     * @param flushIntervalMs max ms between flushes (safety net for low-traffic keys)
     */
    public DistributedHighThroughputRateLimiter(DistributedKeyValueStore store, Clock clock,
                                                int batchDivisor, long flushIntervalMs) {
        if (store == null || clock == null) throw new NullPointerException();
        if (batchDivisor <= 0 || flushIntervalMs <= 0) throw new IllegalArgumentException();

        this.store = store;
        this.clock = clock;
        this.batchDivisor = batchDivisor;
        this.flushIntervalMs = flushIntervalMs;
        this.states = new ConcurrentHashMap<>();

        this.cleanup = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ratelimiter-cleanup");
            t.setDaemon(true);
            return t;
        });
        cleanup.scheduleAtFixedRate(this::evictExpired, 120, 120, TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        if (limit <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        long now = clock.millis();
        long windowId = now / WINDOW_MS;
        String wKey = key + ":" + windowId;

        WindowState state = states.computeIfAbsent(wKey, k -> new WindowState(windowId, now));

        // Fast path: already over limit — just a volatile read, no increment, no RPC
        if (state.knownGlobal >= limit) {
            return CompletableFuture.completedFuture(false);
        }

        int localCount = state.pending.incrementAndGet();
        int threshold = Math.max(1, limit / batchDivisor);

        // Flush on batch-size OR time trigger (the time trigger catches low-traffic keys
        // that would otherwise never reach the batch threshold)
        boolean batchFull = localCount >= threshold;
        boolean overdue = localCount > 0 && (now - state.lastFlushMs) >= flushIntervalMs;

        if ((batchFull || overdue) && state.flushing.compareAndSet(false, true)) {
            return flush(wKey, state, limit, now);
        }

        // No flush — estimate from local state
        int estimate = state.knownGlobal + state.pending.get();
        if (estimate > limit) {
            state.pending.decrementAndGet(); // undo so denied reqs don't inflate the flushed count
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> flush(String wKey, WindowState state, int limit, long now) {
        int toFlush = state.pending.getAndSet(0);
        if (toFlush == 0) {
            state.flushing.set(false);
            return CompletableFuture.completedFuture(state.knownGlobal <= limit);
        }

        // Optimistically bump global count so concurrent threads don't see stale pending=0
        // while the RPC is in flight. Corrected when the response arrives.
        state.knownGlobal += toFlush;
        state.lastFlushMs = now;

        try {
            return store.incrementByAndExpire(wKey, toFlush, WINDOW_SECONDS)
                    .thenApply(globalCount -> {
                        state.knownGlobal = globalCount; // replace optimistic with actual
                        state.flushing.set(false);
                        return globalCount <= limit;
                    })
                    .exceptionally(ex -> {
                        // rollback and put count back for next flush attempt
                        state.knownGlobal -= toFlush;
                        state.pending.addAndGet(toFlush);
                        state.flushing.set(false);
                        return true; // fail open
                    });
        } catch (Exception e) {
            // incrementByAndExpire declares checked exceptions
            state.knownGlobal -= toFlush;
            state.pending.addAndGet(toFlush);
            state.flushing.set(false);
            return CompletableFuture.completedFuture(true); // fail open
        }
    }

    private void evictExpired() {
        long currentWindow = clock.millis() / WINDOW_MS;
        states.entrySet().removeIf(e -> e.getValue().windowId < currentWindow);
    }

    public void shutdown() {
        cleanup.shutdown();
    }
}
