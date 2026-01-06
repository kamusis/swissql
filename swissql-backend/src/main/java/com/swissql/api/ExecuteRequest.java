package com.swissql.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ExecuteRequest {
    @NotBlank(message = "Session ID is required")
    private String sessionId;
    
    @NotBlank(message = "SQL is required")
    private String sql;
    
    @Valid
    @NotNull(message = "Options are required")
    private ExecuteOptions options = new ExecuteOptions();

    @Data
    public static class ExecuteOptions {
        private int limit = 100;
        private int fetchSize = 50;
        private int queryTimeoutMs = 30000;
    }
}
