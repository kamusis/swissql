package com.swissql.api;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

/**
 * A query candidate discovered from YAML packs (only queries defined under the `queries:` node).
 */
@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CollectorQueryCandidate {
    private String collectorId;
    private String collectorRef;
    private String sourceFile;
    private String queryId;
    private String description;
}
