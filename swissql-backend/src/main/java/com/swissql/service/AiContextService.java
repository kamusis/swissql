package com.swissql.service;

import com.swissql.api.ExecuteResponse;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores recent executed SQL statements and sanitized result summaries per session.
 */
@Service
public class AiContextService {

    private static final int DEFAULT_MAX_ITEMS = 10;
    private static final int DEFAULT_MAX_SAMPLE_ROWS = 3;
    private static final int DEFAULT_MAX_COLUMNS_PER_ROW = 20;
    private static final int DEFAULT_MAX_CELL_LENGTH = 64;
    private static final int DEFAULT_MAX_ERROR_LENGTH = 512;

    private static final Set<String> SENSITIVE_COLUMN_MARKERS = Set.of(
            "password",
            "passwd",
            "token",
            "secret",
            "key",
            "credential"
    );

    private final Map<String, Deque<AiContextItem>> buffers = new ConcurrentHashMap<>();

    /**
     * Record an execution response into the session context buffer.
     *
     * @param sessionId session identifier
     * @param sql executed sql string
     * @param response execution response
     */
    public void recordExecute(String sessionId, String sql, ExecuteResponse response) {
        if (sessionId == null || sessionId.isBlank() || sql == null || sql.isBlank() || response == null) {
            return;
        }

        AiContextItem item = buildContextItem(sql, response);
        Deque<AiContextItem> deque = buffers.computeIfAbsent(sessionId, k -> new ArrayDeque<>());
        synchronized (deque) {
            deque.addFirst(item);
            while (deque.size() > DEFAULT_MAX_ITEMS) {
                deque.removeLast();
            }
        }
    }

    /**
     * Record a failed execution into the session context buffer.
     *
     * @param sessionId session identifier
     * @param sql executed sql string
     * @param error execution error
     */
    public void recordExecuteError(String sessionId, String sql, Exception error) {
        if (sessionId == null || sessionId.isBlank() || sql == null || sql.isBlank() || error == null) {
            return;
        }

        AiContextItem item = new AiContextItem();
        item.setSql(sql);
        item.setExecutedAt(OffsetDateTime.now());
        item.setType("ERROR");
        item.setError(sanitizeErrorMessage(error.getMessage()));

        Deque<AiContextItem> deque = buffers.computeIfAbsent(sessionId, k -> new ArrayDeque<>());
        synchronized (deque) {
            deque.addFirst(item);
            while (deque.size() > DEFAULT_MAX_ITEMS) {
                deque.removeLast();
            }
        }
    }

    /**
     * Get recent context items for a session.
     *
     * @param sessionId session identifier
     * @param limit max items to return
     * @return list of items (most recent first)
     */
    public List<AiContextItem> getRecent(String sessionId, int limit) {
        if (sessionId == null || sessionId.isBlank()) {
            return List.of();
        }

        Deque<AiContextItem> deque = buffers.get(sessionId);
        if (deque == null) {
            return List.of();
        }

        int resolvedLimit = limit > 0 ? Math.min(limit, DEFAULT_MAX_ITEMS) : DEFAULT_MAX_ITEMS;
        List<AiContextItem> out = new ArrayList<>(resolvedLimit);
        synchronized (deque) {
            int i = 0;
            for (AiContextItem item : deque) {
                if (i >= resolvedLimit) {
                    break;
                }
                out.add(item);
                i++;
            }
        }
        return out;
    }

    /**
     * Clear all context items for a session.
     *
     * @param sessionId session identifier
     */
    public void clear(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return;
        }
        buffers.remove(sessionId);
    }

    private AiContextItem buildContextItem(String sql, ExecuteResponse response) {
        AiContextItem item = new AiContextItem();
        item.setSql(sql);
        item.setExecutedAt(OffsetDateTime.now());
        item.setType(response.getType());

        ExecuteResponse.Metadata metadata = response.getMetadata();
        if (metadata != null) {
            item.setTruncated(metadata.isTruncated());
            item.setRowsAffected(metadata.getRowsAffected());
            item.setDurationMs(metadata.getDurationMs());
        }

        ExecuteResponse.DataContent data = response.getData();
        if (data != null) {
            if (data.getColumns() != null) {
                List<AiContextItem.Column> cols = new ArrayList<>(data.getColumns().size());
                for (ExecuteResponse.ColumnDefinition c : data.getColumns()) {
                    AiContextItem.Column col = new AiContextItem.Column();
                    col.setName(c.getName());
                    col.setType(c.getType());
                    cols.add(col);
                }
                item.setColumns(cols);
            }

            if (data.getRows() != null && !data.getRows().isEmpty()) {
                item.setSampleRows(sanitizeSampleRows(data.getRows()));
            }
        }

        return item;
    }

    private List<Map<String, Object>> sanitizeSampleRows(List<Map<String, Object>> rows) {
        int maxRows = Math.min(DEFAULT_MAX_SAMPLE_ROWS, rows.size());
        List<Map<String, Object>> out = new ArrayList<>(maxRows);

        for (int i = 0; i < maxRows; i++) {
            Map<String, Object> row = rows.get(i);
            Map<String, Object> sanitized = new HashMap<>();
            if (row == null) {
                out.add(sanitized);
                continue;
            }

            int colCount = 0;
            for (Map.Entry<String, Object> e : row.entrySet()) {
                if (colCount >= DEFAULT_MAX_COLUMNS_PER_ROW) {
                    break;
                }

                String colName = e.getKey();
                Object val = e.getValue();
                if (isSensitiveColumn(colName)) {
                    sanitized.put(colName, "***");
                } else {
                    sanitized.put(colName, truncateCell(val));
                }
                colCount++;
            }

            out.add(sanitized);
        }

        return out;
    }

    private boolean isSensitiveColumn(String colName) {
        if (colName == null) {
            return false;
        }
        String lowered = colName.toLowerCase(Locale.ROOT);
        for (String marker : SENSITIVE_COLUMN_MARKERS) {
            if (lowered.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private Object truncateCell(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof String s) {
            if (s.length() <= DEFAULT_MAX_CELL_LENGTH) {
                return s;
            }
            return s.substring(0, DEFAULT_MAX_CELL_LENGTH);
        }
        return val;
    }

    private String sanitizeErrorMessage(String message) {
        if (message == null) {
            return null;
        }

        String sanitized = message.trim();

        sanitized = sanitized.replaceAll("(?i)^(error:\\s*)+", "");

        sanitized = sanitized.replaceAll("(?i)(password|passwd|token|secret|key)\\s*=\\s*[^\\s]+", "$1=***");

        if (sanitized.length() <= DEFAULT_MAX_ERROR_LENGTH) {
            return sanitized;
        }
        return sanitized.substring(0, DEFAULT_MAX_ERROR_LENGTH);
    }
}
