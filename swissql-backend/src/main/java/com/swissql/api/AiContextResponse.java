package com.swissql.api;

import com.swissql.service.AiContextItem;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Response DTO for retrieving recent AI context for a session.
 */
@Data
@Builder
public class AiContextResponse {

    /**
     * Backend session identifier.
     */
    private String sessionId;

    /**
     * Context items (most recent first).
     */
    private List<AiContextItem> items;
}
