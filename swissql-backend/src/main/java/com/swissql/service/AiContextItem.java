package com.swissql.service;

import lombok.Data;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * A single context item representing one executed SQL statement and a sanitized summary of its result.
 */
@Data
public class AiContextItem {

    private String sql;
    private OffsetDateTime executedAt;
    private String type;


    /**
     * Error message when execution failed.
     */
    private String error;

    private List<Column> columns;
    private List<Map<String, Object>> sampleRows;

    private boolean truncated;
    private long rowsAffected;
    private long durationMs;

    /**
     * Column metadata.
     */
    @Data
    public static class Column {
        private String name;
        private String type;
    }
}
