package com.swissql.api;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class MetaExplainRequest {
    @NotBlank(message = "Session ID is required")
    private String sessionId;

    @NotBlank(message = "SQL is required")
    private String sql;

    private boolean analyze;
}
