package ratelimiter;

import java.util.concurrent.CompletableFuture;

/**
 * Provided data access layer. Increments a key by delta and sets TTL on first creation.
 * Every call is a network round-trip. Key is initialized to 0 if it doesn't exist.
 */
public class DistributedKeyValueStore {

    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta,
                                                           int expirationSeconds) throws Exception {
        throw new UnsupportedOperationException("Implementation provided by infrastructure");
    }
}
