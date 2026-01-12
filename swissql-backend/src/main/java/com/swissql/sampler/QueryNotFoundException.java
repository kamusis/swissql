package com.swissql.sampler;

/**
 * Thrown when a query_id cannot be found within a resolved collector.
 */
public class QueryNotFoundException extends RuntimeException {
    /**
     * Create a new exception.
     *
     * @param message error message
     */
    public QueryNotFoundException(String message) {
        super(message);
    }
}
