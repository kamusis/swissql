package com.swissql.model;

import lombok.Data;

import java.util.Map;

/**
 * Generic collector execution result.
 *
 * <p>This is the long-term contract that replaces fixed-field snapshots. Output is map-based
 * (layers/queries) to avoid hardcoded business identifiers.
 */
@Data
public class CollectorResult {
    private String dbType;
    private Integer intervalSec;
    private String collectorId;
    private String sourceFile;
    private Map<String, LayerResult> layers;
    private Map<String, Object> queries;
}
