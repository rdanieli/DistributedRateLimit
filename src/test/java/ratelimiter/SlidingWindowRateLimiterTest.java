package ratelimiter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowRateLimiterTest {

    static class TestClock extends Clock {
        private final AtomicLong millis;
        TestClock(long initialMillis) { this.millis = new AtomicLong(initialMillis); }
        void advance(long deltaMs)   { millis.addAndGet(deltaMs); }

        @Override public long millis()              { return millis.get(); }
        @Override public ZoneId getZone()            { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant()           { return Instant.ofEpochMilli(millis.get()); }
    }

    static class MockStore extends DistributedKeyValueStore {
        final ConcurrentHashMap<String, AtomicInteger> data = new ConcurrentHashMap<>();
        final AtomicInteger calls = new AtomicInteger(0);

        @Override
        public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expSec) {
            calls.incrementAndGet();
            var counter = data.computeIfAbsent(key, k -> new AtomicInteger(0));
            return CompletableFuture.completedFuture(counter.addAndGet(delta));
        }
    }

    private TestClock clock;
    private MockStore store;
    private SlidingWindowRateLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new TestClock(60_000L);
        store = new MockStore();
        limiter = new SlidingWindowRateLimiter(store, clock, 10, 1000);
    }

    @AfterEach
    void tearDown() {
        if (limiter != null) limiter.shutdown();
    }

    @Test
    void allowsRequestsBelowLimit() throws Exception {
        for (int i = 0; i < 9; i++) {
            assertTrue(limiter.isAllowed("c1", 100).get());
        }
        assertEquals(0, store.calls.get());
    }

    @Test
    void deniesAfterLimitExceeded() throws Exception {
        int limit = 100;
        int allowed = 0;
        for (int i = 0; i < 200; i++) {
            if (limiter.isAllowed("c1", limit).get()) allowed++;
        }
        assertTrue(allowed >= limit, "at least " + limit + " should pass, got " + allowed);
        assertTrue(allowed < 200, "some should be denied");
    }

    @Test
    void preventsWindowBoundaryBurst() throws Exception {
        int limit = 100;

        // Use 80 requests near the end of window 1
        int firstWindowAllowed = 0;
        for (int i = 0; i < 80; i++) {
            if (limiter.isAllowed("c1", limit).get()) firstWindowAllowed++;
        }
        assertEquals(80, firstWindowAllowed);

        // Jump to the very start of window 2 — previous window weight ≈ 1.0
        clock.advance(60_001);

        // Sliding estimate starts at ~80 from previous window, so only ~20 more should pass
        int secondWindowAllowed = 0;
        for (int i = 0; i < 50; i++) {
            if (limiter.isAllowed("c1", limit).get()) secondWindowAllowed++;
        }

        // With fixed windows this would allow all 50 (fresh counter).
        // Sliding window should cap it around 20.
        assertTrue(secondWindowAllowed < 50,
                "sliding window should restrict at boundary, allowed " + secondWindowAllowed);
        assertTrue(secondWindowAllowed >= 15,
                "should still allow roughly 20, allowed " + secondWindowAllowed);
    }

    @Test
    void previousWindowDecaysOverTime() throws Exception {
        int limit = 100;

        // Fill window 1 to 80
        for (int i = 0; i < 80; i++) limiter.isAllowed("c1", limit).get();

        // Advance to 30 seconds into window 2 (previous weight = 0.5)
        // Estimate from previous: 80 * 0.5 = 40, so ~60 more should be allowed
        clock.advance(90_000); // 60s (new window) + 30s into it

        int allowed = 0;
        for (int i = 0; i < 80; i++) {
            if (limiter.isAllowed("c1", limit).get()) allowed++;
        }
        assertTrue(allowed > 50, "half-decayed previous should allow ~60, got " + allowed);
        assertTrue(allowed < 80, "shouldn't allow all 80, got " + allowed);
    }

    @Test
    void firstWindowWorksWithoutPrevious() throws Exception {
        // No previous window exists — should behave like a normal fixed window
        int limit = 100;
        int allowed = 0;
        for (int i = 0; i < 150; i++) {
            if (limiter.isAllowed("c1", limit).get()) allowed++;
        }
        assertTrue(allowed >= limit);
        assertTrue(allowed < 150);
    }

    @Test
    void keysAreIndependent() throws Exception {
        for (int i = 0; i < 45; i++) limiter.isAllowed("c1", 50).get();
        for (int i = 0; i < 45; i++) {
            assertTrue(limiter.isAllowed("c2", 50).get());
        }
    }

    @Test
    void batchesCallsToStore() throws Exception {
        for (int i = 0; i < 95; i++) limiter.isAllowed("c1", 100).get();
        int calls = store.calls.get();
        assertTrue(calls > 0 && calls < 95);
    }

    @Test
    void concurrentAccess() throws Exception {
        int limit = 500;
        int threads = 10;
        int perThread = 200;
        var allowed = new AtomicInteger(0);
        var go = new CountDownLatch(1);
        var done = new CountDownLatch(threads);

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < perThread; i++) {
                            if (limiter.isAllowed("k", limit).get()) allowed.incrementAndGet();
                        }
                    } catch (Exception e) {
                        fail(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            go.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }
        assertTrue(allowed.get() >= limit, "got " + allowed.get());
    }

    @Test
    void windowRollover() throws Exception {
        int limit = 50;
        for (int i = 0; i < 60; i++) limiter.isAllowed("c1", limit).get();
        assertFalse(limiter.isAllowed("c1", limit).get());

        // Advance past TWO windows so previous window is fully gone
        clock.advance(120_001);
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.isAllowed("c1", limit).get(), "should allow after full decay");
        }
    }

    @Test
    void edgeCaseLimits() throws Exception {
        assertFalse(limiter.isAllowed("c1", 0).get());
        assertFalse(limiter.isAllowed("c1", -5).get());

        assertTrue(limiter.isAllowed("one", 1).get());
        assertFalse(limiter.isAllowed("one", 1).get());
    }
}
