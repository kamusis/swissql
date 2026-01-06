package com.swissql.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectResponse {
    private String sessionId;
    private String traceId;
    private OffsetDateTime expiresAt;
}
