package com.swissql.sampler;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SamplerConfig {
    private Integer intervalSec;
    private Boolean enableTopSql;
    private Boolean enableTopSessions;
    private Integer maxItems;
}
