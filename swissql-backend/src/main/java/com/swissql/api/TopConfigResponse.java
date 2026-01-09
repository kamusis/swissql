package com.swissql.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TopConfigResponse {
    private String message;
    private Integer intervalSec;
    private Boolean enableTopSql;
    private Boolean enableTopSessions;
    private Integer maxItems;
}
