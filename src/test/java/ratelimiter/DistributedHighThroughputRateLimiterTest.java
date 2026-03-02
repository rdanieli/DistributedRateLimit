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

class DistributedHighThroughputRateLimiterTest {

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
            AtomicInteger counter = data.computeIfAbsent(key, k -> new AtomicInteger(0));
            return CompletableFuture.completedFuture(counter.addAndGet(delta));
        }
    }

    static class FailingStore extends DistributedKeyValueStore {
        @Override
        public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expSec) {
            return CompletableFuture.failedFuture(new RuntimeException("store down"));
        }
    }

    static class SyncThrowingStore extends DistributedKeyValueStore {
        @Override
        public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expSec) throws Exception {
            throw new Exception("connection refused");
        }
    }

    private TestClock clock;
    private MockStore store;
    private DistributedHighThroughputRateLimiter limiter;

    @BeforeEach
    void setUp() {
        clock = new TestClock(60_000L); // clean minute boundary
        store = new MockStore();
        limiter = new DistributedHighThroughputRateLimiter(store, clock, 10, 1000);
    }

    @AfterEach
    void tearDown() {
        if (limiter != null) limiter.shutdown();
    }

    @Test
    void allowsRequestsBelowLimit() throws Exception {
        // threshold = 100/10 = 10, first 9 stay local
        for (int i = 0; i < 9; i++) {
            assertTrue(limiter.isAllowed("c1", 100).get());
        }
        assertEquals(0, store.calls.get(), "no store calls yet");
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
        assertTrue(calls > 0 && calls < 95,
                "expected batched calls, got " + calls + " for 95 requests");
    }

    @Test
    void fastDenySkipsStore() throws Exception {
        for (int i = 0; i < 30; i++) limiter.isAllowed("c1", 20).get();
        int callsBefore = store.calls.get();

        for (int i = 0; i < 1000; i++) {
            assertFalse(limiter.isAllowed("c1", 20).get());
        }
        assertEquals(callsBefore, store.calls.get(), "fast deny should make zero store calls");
    }

    @Test
    void timeBasedFlush() throws Exception {
        // limit=1000 → threshold=100, only send 50
        for (int i = 0; i < 50; i++) limiter.isAllowed("c1", 1000).get();
        assertEquals(0, store.calls.get());

        clock.advance(1100);
        limiter.isAllowed("c1", 1000).get();
        assertTrue(store.calls.get() > 0, "should have flushed on time trigger");
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

        assertTrue(allowed.get() >= limit, "at least limit should pass, got " + allowed.get());
        assertTrue(store.calls.get() < threads * perThread, "calls should be batched");
    }

    @Test
    void failsOpenOnAsyncError() throws Exception {
        var fl = new DistributedHighThroughputRateLimiter(new FailingStore(), clock, 10, 1000);
        try {
            // threshold=1 for limit=10, every call triggers a flush that fails
            for (int i = 0; i < 5; i++) {
                assertNotNull(fl.isAllowed("c1", 10).get());
            }
        } finally {
            fl.shutdown();
        }
    }

    @Test
    void failsOpenOnSyncException() throws Exception {
        var fl = new DistributedHighThroughputRateLimiter(new SyncThrowingStore(), clock, 10, 1000);
        try {
            assertTrue(fl.isAllowed("c1", 10).get(), "should fail open");
        } finally {
            fl.shutdown();
        }
    }

    @Test
    void windowRollover() throws Exception {
        int limit = 50;
        for (int i = 0; i < 60; i++) limiter.isAllowed("c1", limit).get();
        assertFalse(limiter.isAllowed("c1", limit).get());

        clock.advance(60_001);
        for (int i = 0; i < 10; i++) {
            assertTrue(limiter.isAllowed("c1", limit).get(), "new window should allow");
        }
    }

    @Test
    void edgeCaseLimits() throws Exception {
        assertFalse(limiter.isAllowed("c1", 0).get());
        assertFalse(limiter.isAllowed("c1", -5).get());

        assertTrue(limiter.isAllowed("one", 1).get());
        assertFalse(limiter.isAllowed("one", 1).get());
    }

    @Test
    void constructorValidation() {
        assertThrows(NullPointerException.class, () -> new DistributedHighThroughputRateLimiter(null));
        assertThrows(IllegalArgumentException.class,
                () -> new DistributedHighThroughputRateLimiter(store, clock, 0, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> new DistributedHighThroughputRateLimiter(store, clock, 10, -1));
    }

    @Test
    void multiServerSimulation() throws Exception {
        var s1 = new DistributedHighThroughputRateLimiter(store, clock, 10, 1000);
        var s2 = new DistributedHighThroughputRateLimiter(store, clock, 10, 1000);
        try {
            int limit = 100;
            int a1 = 0, a2 = 0;
            for (int i = 0; i < 200; i++) {
                if (i % 2 == 0) { if (s1.isAllowed("shared", limit).get()) a1++; }
                else             { if (s2.isAllowed("shared", limit).get()) a2++; }
            }
            int total = a1 + a2;
            assertTrue(total >= limit, "total should be >= limit, got " + total);
            assertTrue(total < limit * 3, "shouldn't be wildly over limit");
        } finally {
            s1.shutdown();
            s2.shutdown();
        }
    }
}
