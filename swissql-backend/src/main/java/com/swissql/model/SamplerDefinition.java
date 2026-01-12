package com.swissql.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Sampler definition loaded from JSON configuration.
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SamplerDefinition {
    private String samplerId;
    private Boolean enabled;
    private ScheduleConfig schedule;
    private RunPolicyConfig runPolicy;
    private ResultPolicyConfig resultPolicy;
    private TargetConfig target;
}
