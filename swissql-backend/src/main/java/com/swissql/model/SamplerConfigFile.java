package com.swissql.model;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

import java.util.List;

/**
 * Root configuration object for sampler definitions loaded from JSON.
 */
@Data
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SamplerConfigFile {
    private Integer version;
    private List<SamplerDefinition> samplers;
}
