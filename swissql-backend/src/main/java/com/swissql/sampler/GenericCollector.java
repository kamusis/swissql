package com.swissql.sampler;

import com.swissql.api.ExecuteResponse;
import com.swissql.model.CollectorResult;
import com.swissql.model.CollectorConfig;
import com.swissql.model.CollectorDefinition;
import com.swissql.model.LayerConfig;
import com.swissql.model.LayerResult;
import com.swissql.model.QueryConfig;
import com.swissql.util.JdbcJsonSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class GenericCollector {
    private static final Logger log = LoggerFactory.getLogger(GenericCollector.class);

    // TODO(P2): Add metrics logging: sample duration, success/failure counts per collector.
    // Consider using Micrometer if available for production monitoring.

    /**
     * Collects a full collector definition into a generic {@link CollectorResult}.
     *
     * <p>This method must not hardcode layer identifiers.
     *
     * @param conn database connection
     * @param collectorName collector id (map key in YAML)
     * @param config resolved collector config (pack)
     * @return collector result
     */
    public CollectorResult collectCollector(Connection conn, String collectorName, CollectorConfig config) {
        CollectorDefinition collectorDef = config.getCollectors().get(collectorName);
        if (collectorDef == null) {
            log.error("Collector definition not found: {}", collectorName);
            return null;
        }

        try {
            CollectorResult out = new CollectorResult();
            out.setDbType(config.getDbType());
            out.setCollectorId(collectorName);
            out.setSourceFile(config.getSourceFile());

            if (collectorDef.getLayers() != null) {
                out.setLayers(collectLayers(conn, collectorDef.getLayers()));
                return out;
            }

            if (collectorDef.getQueries() != null) {
                // For now, represent collector queries execution as a map of raw objects.
                // QueryResult is produced by CollectorRunner when query_id is specified.
                Map<String, Object> queries = new LinkedHashMap<>();
                for (Map.Entry<String, QueryConfig> e : collectorDef.getQueries().entrySet()) {
                    QueryConfig q = e.getValue();
                    if (q == null) {
                        continue;
                    }
                    Object result = executeQueryRows(conn, q.getSql(), q.getSingleRow());
                    queries.put(e.getKey(), result);
                }
                out.setQueries(queries);
                return out;
            }
        } catch (Exception e) {
            log.error("Failed to collect data for collector: {}", collectorName, e);
        }

        log.error("No layers or queries found in collector: {}", collectorName);
        return null;
    }

    private Map<String, LayerResult> collectLayers(Connection conn, Map<String, LayerConfig> layers) throws Exception {
        if (layers == null || layers.isEmpty()) {
            return Map.of();
        }

        List<Map.Entry<String, LayerConfig>> ordered = layers.entrySet().stream()
                .filter(e -> e.getKey() != null && !e.getKey().isBlank())
                .filter(e -> e.getValue() != null)
                .sorted(Comparator
                        .comparing((Map.Entry<String, LayerConfig> e) -> safeOrder(e.getValue()))
                        .thenComparing(Map.Entry::getKey))
                .toList();

        Map<String, LayerResult> out = new LinkedHashMap<>();
        for (Map.Entry<String, LayerConfig> entry : ordered) {
            String layerId = entry.getKey();
            LayerConfig layer = entry.getValue();
            try {
                List<Map<String, Object>> rows = executeQueryRows(conn, layer.getSql(), layer.getSingleRow());
                LayerResult layerResult = new LayerResult();
                layerResult.setOrder(layer.getOrder());
                layerResult.setRenderHint(layer.getRender_hint());
                layerResult.setRows(rows);
                out.put(layerId, layerResult);
            } catch (Exception e) {
                log.error("Failed to execute layer: {}", layerId, e);
            }
        }
        return out;
    }

    /**
     * Executes a query and returns rows.
     *
     * @param conn database connection
     * @param sql SQL text
     * @param singleRow whether only the first row should be returned
     * @return rows
     */
    public List<Map<String, Object>> executeQueryRows(Connection conn, String sql, Boolean singleRow) throws Exception {
        return executeQueryRows(conn, sql, singleRow, null);
    }

    public List<Map<String, Object>> executeQueryRows(Connection conn, String sql, Boolean singleRow, Map<String, Object> params) throws Exception {
        if (conn == null) {
            throw new IllegalArgumentException("connection is null");
        }
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql is blank");
        }

        NamedSql named = NamedSql.parse(sql);
        try (PreparedStatement stmt = conn.prepareStatement(named.getSql())) {
            bindNamedParams(stmt, named, params);
            try (ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    row.put(columnName, JdbcJsonSafe.readJsonSafeValue(rs, i));
                }
                rows.add(row);
                if (singleRow != null && singleRow) {
                    break;
                }
            }
            return rows;
            }
        }
    }

    /**
     * Executes a query and builds an {@link ExecuteResponse} shaped result.
     *
     * @param conn database connection
     * @param sql SQL text
     * @param singleRow whether only the first row should be returned
     * @return execute response
     */
    public ExecuteResponse executeQueryExecuteResponse(Connection conn, String sql, Boolean singleRow) throws Exception {
        return executeQueryExecuteResponse(conn, sql, singleRow, null);
    }

    public ExecuteResponse executeQueryExecuteResponse(Connection conn, String sql, Boolean singleRow, Map<String, Object> params) throws Exception {
        if (conn == null) {
            throw new IllegalArgumentException("connection is null");
        }
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql is blank");
        }

        long start = System.currentTimeMillis();
        NamedSql named = NamedSql.parse(sql);
        try (PreparedStatement stmt = conn.prepareStatement(named.getSql())) {
            bindNamedParams(stmt, named, params);
            try (ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            ExecuteResponse response = new ExecuteResponse();
            response.setType("tabular");

            ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
            List<ExecuteResponse.ColumnDefinition> cols = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                ExecuteResponse.ColumnDefinition c = new ExecuteResponse.ColumnDefinition();
                c.setName(metaData.getColumnLabel(i));
                c.setType(metaData.getColumnTypeName(i));
                cols.add(c);
            }

            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnLabel(i), JdbcJsonSafe.readJsonSafeValue(rs, i));
                }
                rows.add(row);
                if (singleRow != null && singleRow) {
                    break;
                }
            }

            data.setColumns(cols);
            data.setRows(rows);
            response.setData(data);

            ExecuteResponse.Metadata md = new ExecuteResponse.Metadata();
            md.setRowsAffected(rows.size());
            md.setTruncated(false);
            md.setDurationMs(System.currentTimeMillis() - start);
            response.setMetadata(md);
            return response;
            }
        }
    }

    private void bindNamedParams(PreparedStatement stmt, NamedSql named, Map<String, Object> params) throws Exception {
        if (stmt == null || named == null || named.getParamNames().isEmpty()) {
            return;
        }
        Map<String, Object> safeParams = params != null ? params : Map.of();
        for (int i = 0; i < named.getParamNames().size(); i++) {
            String name = named.getParamNames().get(i);
            Object value = safeParams.get(name);
            // JDBC uses 1-based parameter indexing.
            stmt.setObject(i + 1, value);
        }
    }

    private static class NamedSql {
        private final String sql;
        private final List<String> paramNames;

        private NamedSql(String sql, List<String> paramNames) {
            this.sql = sql;
            this.paramNames = paramNames;
        }

        public String getSql() {
            return sql;
        }

        public List<String> getParamNames() {
            return paramNames;
        }

        public static NamedSql parse(String sql) {
            if (sql == null) {
                return new NamedSql("", List.of());
            }

            StringBuilder out = new StringBuilder(sql.length());
            List<String> names = new ArrayList<>();
            boolean inSingleQuote = false;

            for (int i = 0; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (c == '\'') {
                    inSingleQuote = !inSingleQuote;
                    out.append(c);
                    continue;
                }

                if (!inSingleQuote && c == ':') {
                    // PostgreSQL type casts use '::' (e.g., NULL::bigint). Do not treat as a named parameter.
                    if ((i > 0 && sql.charAt(i - 1) == ':') || (i + 1 < sql.length() && sql.charAt(i + 1) == ':')) {
                        out.append(c);
                        continue;
                    }

                    int j = i + 1;
                    while (j < sql.length()) {
                        char cj = sql.charAt(j);
                        if (Character.isLetterOrDigit(cj) || cj == '_') {
                            j++;
                            continue;
                        }
                        break;
                    }

                    String name = sql.substring(i + 1, j);
                    if (!name.isBlank()) {
                        names.add(name);
                        out.append('?');
                        i = j - 1;
                        continue;
                    }
                }

                out.append(c);
            }

            return new NamedSql(out.toString(), names);
        }
    }

    private Integer safeOrder(LayerConfig layer) {
        if (layer == null) {
            return Integer.MAX_VALUE;
        }
        Integer o = layer.getOrder();
        return o != null ? o : Integer.MAX_VALUE;
    }
}
