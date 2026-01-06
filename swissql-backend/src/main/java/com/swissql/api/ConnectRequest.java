package com.swissql.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class ConnectRequest {
    @NotBlank(message = "DSN is required")
    private String dsn;
    
    @NotBlank(message = "Database type is required")
    private String dbType;
    
    @Valid
    @NotNull(message = "Options are required")
    private ConnectOptions options = new ConnectOptions();

    @Data
    public static class ConnectOptions {
        private boolean readOnly = false;
        private boolean useMcp = false;
        private int connectionTimeoutMs = 5000;
    }
}
