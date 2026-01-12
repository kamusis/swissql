package com.swissql.sampler;

/**
 * Thrown when a collector_id matches multiple collector definitions and the caller must disambiguate.
 */
public class CollectorAmbiguousException extends RuntimeException {
    /**
     * Create a new exception.
     *
     * @param message error message
     */
    public CollectorAmbiguousException(String message) {
        super(message);
    }
}
