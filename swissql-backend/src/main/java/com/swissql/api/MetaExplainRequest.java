package com.swissql.api;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class MetaExplainRequest {
    @NotBlank(message = "Session ID is required")
    private String sessionId;

    @NotBlank(message = "SQL is required")
    private String sql;

    private boolean analyze;
}
