package com.swissql.api;

import lombok.Data;

import java.util.Map;

/**
 * Query execution result for a single {@code query_id}.
 *
 * <p>This wraps semantic metadata (collector/query identity and render hints) and embeds an
 * {@link ExecuteResponse} shaped {@code result} for renderer reuse.
 */
@Data
public class QueryResult {
    private String dbType;
    private String collectorId;
    private String sourceFile;
    private String queryId;
    private String description;
    private Map<String, Object> renderHint;
    private ExecuteResponse result;
}
