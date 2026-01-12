package com.swissql.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Layer result produced by running a collector layer.
 */
@Data
public class LayerResult {
    /**
     * The order of the layer result.
     */
    private Integer order;
    /**
     * Rendering hints for the layer result.
     */
    private Map<String, Object> renderHint;
    /**
     * The rows of data in the layer result.
     */
    private List<Map<String, Object>> rows;
}
