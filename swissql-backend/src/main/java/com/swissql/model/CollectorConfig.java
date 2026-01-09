package com.swissql.model;

import lombok.Data;

import java.util.Map;

@Data
public class CollectorConfig {
    private String dbType;
    private SupportedVersions supportedVersions;
    private String description;
    private Map<String, CollectorDefinition> collectors;
}
