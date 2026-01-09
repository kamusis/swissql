package com.swissql.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SqlTextResponse {
    private String sqlId;
    private String text;
    private Boolean truncated;
}
