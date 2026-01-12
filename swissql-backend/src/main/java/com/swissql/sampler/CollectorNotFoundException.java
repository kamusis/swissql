package com.swissql.sampler;

/**
 * Thrown when a collector cannot be resolved by collector_id or collector_ref.
 */
public class CollectorNotFoundException extends RuntimeException {
    /**
     * Create a new exception.
     *
     * @param message error message
     */
    public CollectorNotFoundException(String message) {
        super(message);
    }
}
