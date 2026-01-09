package com.swissql.model;

import lombok.Data;

import java.util.Map;

@Data
public class CollectorDefinition {
    private String description;
    private Map<String, LayerConfig> layers;
    private Map<String, QueryConfig> queries;
}
