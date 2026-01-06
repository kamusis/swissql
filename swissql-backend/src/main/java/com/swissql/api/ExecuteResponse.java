package com.swissql.api;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class ExecuteResponse {
    private String type; // tabular | text | file
    private DataContent data;
    private Metadata metadata;

    @Data
    public static class DataContent {
        private List<ColumnDefinition> columns;
        private List<Map<String, Object>> rows;
        private String textContent;
        private String fileUrl;
    }

    @Data
    public static class ColumnDefinition {
        private String name;
        private String type;
    }

    @Data
    public static class Metadata {
        private boolean truncated;
        private long rowsAffected;
        private long durationMs;
        private String nextPageToken;
    }
}
