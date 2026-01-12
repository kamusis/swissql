package com.swissql.model;

import lombok.Data;

import java.util.Map;

@Data
/**
 * Layer configuration loaded from collector YAML.
 *
 * <p>Note: this class intentionally models YAML schema fields such as {@code order} and
 * {@code render_hint} so SnakeYAML {@code load_as} can deserialize them. This is contract
 * modeling, not business-identifier hardcoding.
 */
public class LayerConfig {
    private String name;
    private String description;
    private Integer order;
    private Map<String, Object> render_hint;
    private String sql;
    private Boolean singleRow;
}
