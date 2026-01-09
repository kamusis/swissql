package com.swissql.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class TopConfigRequest {
    @NotNull(message = "sessionId is required")
    private String sessionId;

    @Min(value = 1, message = "intervalSec must be at least 1")
    private Integer intervalSec;

    private Boolean enableTopSql;

    private Boolean enableTopSessions;

    @Min(value = 1, message = "maxItems must be at least 1")
    private Integer maxItems;
}
