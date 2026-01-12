package com.swissql.api;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

import java.util.Map;

/**
 * Request payload for executing a collector or a single collector query.
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CollectorsRunRequest {
    private String sessionId;
    private String collectorId;
    private String collectorRef;
    private String queryId;
    private Map<String, Object> params;
}
