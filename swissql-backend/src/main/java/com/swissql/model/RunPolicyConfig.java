package com.swissql.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Runtime execution policy for a sampler.
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class RunPolicyConfig {
    private Integer timeoutMs;
    private Integer maxConcurrency;
    private String onOverlap;
    private String errorPolicy;
}
