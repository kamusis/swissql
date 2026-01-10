package com.swissql.api;

import lombok.Builder;
import lombok.Data;

/**
 * Response payload for top sampler control operations such as start/stop/restart.
 */
@Data
@Builder
public class TopSamplerControlResponse {
    private String message;
    private String status;
    private String reason;
}
