package com.swissql.model;

import com.swissql.api.ConnectRequest;
import lombok.Builder;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
@Builder
public class SessionInfo {
    private String sessionId;
    private String dsn;
    private String dbType;
    private ConnectRequest.ConnectOptions options;
    private OffsetDateTime createdAt;
    private OffsetDateTime lastAccessedAt;
    private OffsetDateTime expiresAt;
}
