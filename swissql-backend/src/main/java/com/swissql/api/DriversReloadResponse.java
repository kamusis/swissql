package com.swissql.api;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Response for {@code POST /v1/meta/drivers/reload}.
 */
@Data
public class DriversReloadResponse {
    private String status;
    private Map<String, Object> reloaded = new HashMap<>();
}
