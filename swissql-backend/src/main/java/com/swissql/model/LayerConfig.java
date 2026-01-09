package com.swissql.model;

import lombok.Data;

@Data
public class LayerConfig {
    private String name;
    private String description;
    private String sql;
    private Boolean singleRow;
}
