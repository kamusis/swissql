package com.swissql.sampler;

import com.swissql.model.CollectorConfig;
import com.swissql.model.CollectorDefinition;
import com.swissql.model.LayerConfig;
import com.swissql.model.QueryConfig;
import com.swissql.model.TopSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class GenericCollector {
    private static final Logger log = LoggerFactory.getLogger(GenericCollector.class);

    // TODO(P2): Add metrics logging: sample duration, success/failure counts per collector.
    // Consider using Micrometer if available for production monitoring.

    public Object collect(Connection conn, String collectorName, CollectorConfig config) {
        CollectorDefinition collectorDef = config.getCollectors().get(collectorName);
        if (collectorDef == null) {
            log.error("Collector definition not found: {}", collectorName);
            return null;
        }

        try {
            if (collectorDef.getLayers() != null) {
                return collectLayers(conn, collectorDef.getLayers());
            } else if (collectorDef.getQueries() != null) {
                return collectQuery(conn, collectorDef.getQueries());
            }
        } catch (Exception e) {
            log.error("Failed to collect data for collector: {}", collectorName, e);
        }

        log.error("No layers or queries found in collector: {}", collectorName);
        return null;
    }

    private TopSnapshot collectLayers(Connection conn, Map<String, LayerConfig> layers) {
        TopSnapshot snapshot = new TopSnapshot();

        for (Map.Entry<String, LayerConfig> entry : layers.entrySet()) {
            String layerName = entry.getKey();
            LayerConfig layer = entry.getValue();
            try {
                Object result = executeQuery(conn, layer.getSql(), layer.getSingleRow());

                // TODO(P1): Avoid hardcoding YAML layer identifiers here.
                // Current design couples collector YAML `layers` keys (e.g., `topSessions`) to Java code.
                // This makes collector extension harder and risks naming drift across boundaries
                // (YAML identifiers vs JSON snake_case contract, e.g., `top_sessions`).
                // Refactor to either:
                // - return layers as a map keyed by canonical snake_case identifiers, or
                // - introduce centralized mapping + normalization (support both `topSessions` and `top_sessions`).

                switch (layerName) {
                    case "context" -> snapshot.setContext((Map<String, Object>) result);
                    case "cpu" -> snapshot.setCpu((Map<String, Object>) result);
                    case "sessions" -> snapshot.setSessions((Map<String, Object>) result);
                    case "waits" -> snapshot.setWaits((List<Map<String, Object>>) result);
                    case "topSessions" -> {
                        List<Map<String, Object>> sessions = (List<Map<String, Object>>) result;
                        snapshot.setTopSessions(sessions);
                        // log.debug("topSessions query returned {} rows", sessions != null ? sessions.size() : 0);
                    }
                    case "io" -> snapshot.setIo((Map<String, Object>) result);
                }
            } catch (Exception e) {
                log.error("Failed to execute layer: {}", layerName, e);
            }
        }

        return snapshot;
    }

    private Object collectQuery(Connection conn, Map<String, QueryConfig> queries) throws Exception {
        if (queries.isEmpty()) {
            return null;
        }

        QueryConfig query = queries.values().iterator().next();
        return executeQuery(conn, query.getSql(), query.getSingleRow());
    }

    private Object executeQuery(Connection conn, String sql, Boolean singleRow) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (singleRow != null && singleRow && rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    row.put(columnName, rs.getObject(i));
                }
                return row;
            } else {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        row.put(columnName, rs.getObject(i));
                    }
                    rows.add(row);
                }
                return rows;
            }
        }
    }
}
