package com.swissql.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Scheduling configuration for a sampler.
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ScheduleConfig {
    private String type;
    private Integer intervalSec;
    private String cron;
    private String timezone;
    private Integer startDelaySec;
    private Integer jitterSec;
}
