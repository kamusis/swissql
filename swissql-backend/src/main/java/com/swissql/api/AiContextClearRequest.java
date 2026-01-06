package com.swissql.api;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Request DTO for clearing AI context for a session.
 */
@Data
public class AiContextClearRequest {

    /**
     * Backend session identifier.
     */
    @NotBlank(message = "Session ID is required")
    private String sessionId;
}
