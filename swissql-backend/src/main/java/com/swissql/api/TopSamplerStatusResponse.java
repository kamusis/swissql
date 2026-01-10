package com.swissql.api;

import lombok.Builder;
import lombok.Data;

/**
 * Response payload that describes current top sampler status for a session.
 */
@Data
@Builder
public class TopSamplerStatusResponse {
    private String status;
    private String reason;
}
