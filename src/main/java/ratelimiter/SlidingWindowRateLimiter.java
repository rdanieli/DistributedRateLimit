package ratelimiter;

import java.time.Clock;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sliding window variant. Blends the previous window's count with the current one
 * to avoid the 2x burst at window boundaries that fixed windows allow.
 *
 * Estimate = previousCount * (1 - elapsedRatio) + currentCount
 *
 * Same batching strategy as the fixed window version — counts accumulate locally
 * and flush to the distributed store periodically.
 */
public class SlidingWindowRateLimiter {

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

    static class WindowState {
        final AtomicInteger pending = new AtomicInteger(0);
        volatile int knownGlobal = 0;
        final AtomicBoolean flushing = new AtomicBoolean(false);
        volatile long lastFlushMs;
        final long windowId;

        WindowState(long windowId, long nowMs) {
            this.windowId = windowId;
            this.lastFlushMs = nowMs;
        }

        int total() {
            return knownGlobal + pending.get();
        }
    }

    public SlidingWindowRateLimiter(DistributedKeyValueStore store) {
        this(store, Clock.systemUTC(), DEFAULT_BATCH_DIVISOR, DEFAULT_FLUSH_INTERVAL_MS);
    }

    public SlidingWindowRateLimiter(DistributedKeyValueStore store, Clock clock,
                                    int batchDivisor, long flushIntervalMs) {
        if (store == null || clock == null) throw new NullPointerException();
        if (batchDivisor <= 0 || flushIntervalMs <= 0) throw new IllegalArgumentException();

        this.store = store;
        this.clock = clock;
        this.batchDivisor = batchDivisor;
        this.flushIntervalMs = flushIntervalMs;
        this.states = new ConcurrentHashMap<>();

        this.cleanup = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("sliding-ratelimiter-cleanup").factory());
        cleanup.scheduleAtFixedRate(this::evictExpired, 120, 120, TimeUnit.SECONDS);
    }

    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        if (limit <= 0) return CompletableFuture.completedFuture(false);

        long now = clock.millis();
        long windowId = now / WINDOW_MS;
        var wKey = key + ":" + windowId;

        var current = states.computeIfAbsent(wKey, k -> new WindowState(windowId, now));

        // Fast deny using the blended sliding estimate (no increment yet)
        if (slidingEstimate(key, current, now) >= limit) {
            return CompletableFuture.completedFuture(false);
        }

        int localCount = current.pending.incrementAndGet();
        int threshold = Math.max(1, limit / batchDivisor);

        boolean batchFull = localCount >= threshold;
        boolean overdue = localCount > 0 && (now - current.lastFlushMs) >= flushIntervalMs;

        if ((batchFull || overdue) && current.flushing.compareAndSet(false, true)) {
            return flush(wKey, current, key, limit, now);
        }

        // Re-check after increment with sliding estimate
        if (slidingEstimate(key, current, now) > limit) {
            current.pending.decrementAndGet();
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(true);
    }

    /**
     * Blended count: prevWindowCount * (1 - elapsed/window) + currentWindowCount.
     * At the start of a new window the previous count has full weight;
     * by the end it's fully decayed.
     */
    private int slidingEstimate(String baseKey, WindowState current, long now) {
        long windowId = now / WINDOW_MS;
        var prev = states.get(baseKey + ":" + (windowId - 1));
        int prevCount = (prev != null) ? prev.total() : 0;
        double weight = 1.0 - ((now % WINDOW_MS) / (double) WINDOW_MS);
        return (int)(prevCount * weight) + current.total();
    }

    private CompletableFuture<Boolean> flush(String wKey, WindowState current,
                                              String baseKey, int limit, long now) {
        int toFlush = current.pending.getAndSet(0);
        if (toFlush == 0) {
            current.flushing.set(false);
            return CompletableFuture.completedFuture(slidingEstimate(baseKey, current, now) <= limit);
        }

        current.knownGlobal += toFlush;
        current.lastFlushMs = now;

        try {
            return store.incrementByAndExpire(wKey, toFlush, WINDOW_SECONDS)
                    .thenApply(globalCount -> {
                        current.knownGlobal = globalCount;
                        current.flushing.set(false);
                        return slidingEstimate(baseKey, current, now) <= limit;
                    })
                    .exceptionally(ex -> {
                        current.knownGlobal -= toFlush;
                        current.pending.addAndGet(toFlush);
                        current.flushing.set(false);
                        return true; // fail open
                    });
        } catch (Exception e) {
            current.knownGlobal -= toFlush;
            current.pending.addAndGet(toFlush);
            current.flushing.set(false);
            return CompletableFuture.completedFuture(true);
        }
    }

    private void evictExpired() {
        long currentWindow = clock.millis() / WINDOW_MS;
        // Keep current AND previous window (previous is needed for blending)
        states.entrySet().removeIf(e -> e.getValue().windowId < currentWindow - 1);
    }

    public void shutdown() {
        cleanup.shutdown();
    }
}
