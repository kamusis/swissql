package com.swissql.api;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Response payload for listing runnable queries available to the current session context.
 * Only queries defined under the `queries:` node are returned (layers are excluded).
 */
@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CollectorsQueriesListResponse {
    private List<CollectorQueryCandidate> queries;
}
