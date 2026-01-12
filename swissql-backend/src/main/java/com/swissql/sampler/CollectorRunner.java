package com.swissql.sampler;

import com.swissql.api.ExecuteResponse;
import com.swissql.api.QueryResult;
import com.swissql.model.CollectorConfig;
import com.swissql.model.CollectorDefinition;
import com.swissql.model.CollectorResult;
import com.swissql.model.QueryConfig;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;

/**
 * Internal collector runner used by both HTTP controllers and samplers.
 *
 * <p>This class centralizes collector/query resolution (collector_id vs collector_ref) and execution.
 * It must not call HTTP endpoints.
 */
@Component
public class CollectorRunner {
    private static final Logger log = LoggerFactory.getLogger(CollectorRunner.class);

    private final CollectorRegistry collectorRegistry;
    private final GenericCollector genericCollector;

    /**
     * Create a collector runner.
     *
     * @param collectorRegistry registry for collector YAML packs
     * @param genericCollector executor for layers/queries
     */
    public CollectorRunner(CollectorRegistry collectorRegistry, GenericCollector genericCollector) {
        this.collectorRegistry = collectorRegistry;
        this.genericCollector = genericCollector;
    }

    /**
     * Runs a collector and returns {@link CollectorResult}.
     *
     * @param conn database connection
     * @param dbType database type
     * @param collectorId collector id (optional when collectorRef is provided)
     * @param collectorRef collector ref (optional)
     * @return collector result
     */
    public CollectorResult runCollector(Connection conn, String dbType, String collectorId, String collectorRef) {
        ResolvedCollector resolved = resolveCollector(conn, dbType, collectorId, collectorRef);
        CollectorResult result = genericCollector.collectCollector(conn, resolved.collectorId, resolved.config);
        if (result != null) {
            result.setIntervalSec(null);
        }
        return result;
    }

    /**
     * Runs a single query within a resolved collector and returns {@link QueryResult}.
     *
     * @param conn database connection
     * @param dbType database type
     * @param collectorId collector id (optional when collectorRef is provided)
     * @param collectorRef collector ref (optional)
     * @param queryId query id
     * @return query result
     */
    public QueryResult runQuery(Connection conn, String dbType, String collectorId, String collectorRef, String queryId) {
        return runQuery(conn, dbType, collectorId, collectorRef, queryId, null);
    }

    public QueryResult runQuery(Connection conn, String dbType, String collectorId, String collectorRef, String queryId, Map<String, Object> params) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("query_id is required");
        }

        ResolvedQuery resolved = resolveQuery(conn, dbType, collectorId, collectorRef, queryId);
        QueryConfig queryConfig = resolved.queryConfig;

        try {
            ExecuteResponse exec = genericCollector.executeQueryExecuteResponse(conn, queryConfig.getSql(), queryConfig.getSingleRow(), params);
            QueryResult out = new QueryResult();
            out.setDbType(dbType);
            out.setCollectorId(resolved.collectorId);
            out.setSourceFile(resolved.config.getSourceFile());
            out.setQueryId(queryId);
            out.setDescription(queryConfig.getDescription());
            out.setRenderHint(null);
            out.setResult(exec);
            return out;
        } catch (Exception e) {
            String causeMsg = e.getMessage();
            Throwable c = e.getCause();
            while (c != null) {
                if (c.getMessage() != null && !c.getMessage().isBlank()) {
                    causeMsg = c.getMessage();
                }
                c = c.getCause();
            }

            String detail = "Failed to execute query: " + queryId;
            if (collectorRef != null && !collectorRef.isBlank()) {
                detail += " (collector_ref=" + collectorRef + ")";
            } else if (collectorId != null && !collectorId.isBlank()) {
                detail += " (collector_id=" + collectorId + ")";
            }
            if (causeMsg != null && !causeMsg.isBlank()) {
                detail += ": " + causeMsg;
            }

            log.error(
                    "Collector query execution failed: db_type={}, query_id={}, collector_id={}, collector_ref={}",
                    dbType,
                    queryId,
                    collectorId,
                    collectorRef,
                    e
            );
            throw new RuntimeException(detail, e);
        }
    }

    private ResolvedQuery resolveQuery(Connection conn, String dbType, String collectorId, String collectorRef, String queryId) {
        ResolvedCollector resolved = null;
        if (collectorRef != null && !collectorRef.isBlank()) {
            resolved = resolveCollector(conn, dbType, null, collectorRef);
        } else if (collectorId != null && !collectorId.isBlank()) {
            resolved = resolveCollector(conn, dbType, collectorId, null);
        }

        if (resolved != null) {
            CollectorDefinition def = resolved.config.getCollectors().get(resolved.collectorId);
            if (def == null || def.getQueries() == null) {
                throw new QueryNotFoundException("Query not found: " + queryId);
            }
            QueryConfig queryConfig = def.getQueries().get(queryId);
            if (queryConfig == null) {
                throw new QueryNotFoundException("Query not found: " + queryId);
            }
            return new ResolvedQuery(resolved.config, resolved.collectorId, queryConfig);
        }

        // Shorthand resolution: scan all matching packs/collectors for a unique query_id.
        List<CollectorConfig> matching = collectorRegistry.getMatchingConfigs(conn, dbType);
        if (matching.isEmpty()) {
            throw new CollectorNotFoundException("No collector packs found for db_type: " + dbType);
        }

        class Hit {
            private final CollectorConfig config;
            private final String collectorId;
            private final QueryConfig queryConfig;

            private Hit(CollectorConfig config, String collectorId, QueryConfig queryConfig) {
                this.config = config;
                this.collectorId = collectorId;
                this.queryConfig = queryConfig;
            }
        }

        List<Hit> hits = new ArrayList<>();
        for (CollectorConfig config : matching) {
            if (config == null || config.getCollectors() == null) {
                continue;
            }
            for (Map.Entry<String, CollectorDefinition> entry : config.getCollectors().entrySet()) {
                String cid = entry.getKey();
                CollectorDefinition def = entry.getValue();
                if (def == null || def.getQueries() == null) {
                    continue;
                }
                QueryConfig qc = def.getQueries().get(queryId);
                if (qc != null) {
                    hits.add(new Hit(config, cid, qc));
                }
            }
        }

        if (hits.isEmpty()) {
            throw new QueryNotFoundException("Query not found: " + queryId);
        }

        if (hits.size() > 1) {
            String details = hits.stream()
                    .map(h -> normalizePackId(h.config.getSourceFile()) + ":" + h.collectorId)
                    .distinct()
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("<unknown>");
            throw new CollectorAmbiguousException(
                    "Query is ambiguous: " + queryId + ". Candidates: [" + details + "]. Use collector_ref."
            );
        }

        Hit h = hits.get(0);
        return new ResolvedQuery(h.config, h.collectorId, h.queryConfig);
    }

    private ResolvedCollector resolveCollector(Connection conn, String dbType, String collectorId, String collectorRef) {
        if (conn == null) {
            throw new IllegalArgumentException("connection is null");
        }
        if (dbType == null || dbType.isBlank()) {
            throw new IllegalArgumentException("db_type is blank");
        }

        List<CollectorConfig> matching = collectorRegistry.getMatchingConfigs(conn, dbType);
        if (matching.isEmpty()) {
            throw new CollectorNotFoundException("No collector packs found for db_type: " + dbType);
        }

        if (collectorRef != null && !collectorRef.isBlank()) {
            String[] parts = collectorRef.split(":", 2);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("Invalid collector_ref: " + collectorRef);
            }
            String packId = normalizePackId(parts[0]);
            String resolvedCollectorId = parts[1];

            CollectorConfig config = matching.stream()
                    .filter(Objects::nonNull)
                    .filter(c -> packId.equals(normalizePackId(c.getSourceFile())))
                    .findFirst()
                    .orElse(null);

            // If a collector_ref is provided but does not exist within the dbType-matching packs,
            // fall back to collector_id when available. This allows dbType-agnostic sampler
            // definitions to carry a stable collector_id while still permitting explicit refs.
            if (config == null) {
                if (collectorId != null && !collectorId.isBlank()) {
                    collectorRef = null;
                } else {
                    throw new CollectorNotFoundException("Collector pack not found: " + packId);
                }
            } else if (config.getCollectors() == null || !config.getCollectors().containsKey(resolvedCollectorId)) {
                if (collectorId != null && !collectorId.isBlank()) {
                    collectorRef = null;
                } else {
                    throw new CollectorNotFoundException("Collector not found: " + collectorRef);
                }
            } else {
                return new ResolvedCollector(config, resolvedCollectorId);
            }
        }

        if (collectorId == null || collectorId.isBlank()) {
            throw new IllegalArgumentException("collector_id or collector_ref is required");
        }

        List<CollectorConfig> hits = matching.stream()
                .filter(Objects::nonNull)
                .filter(c -> c.getCollectors() != null && c.getCollectors().containsKey(collectorId))
                .toList();

        if (hits.isEmpty()) {
            throw new CollectorNotFoundException("Collector not found: " + collectorId);
        }
        if (hits.size() > 1) {
            String details = hits.stream()
                    .map(CollectorConfig::getSourceFile)
                    .filter(Objects::nonNull)
                    .distinct()
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("<unknown>");
            throw new CollectorAmbiguousException(
                    "Collector is ambiguous: " + collectorId + ". Candidates: [" + details + "]. Use collector_ref."
            );
        }

        return new ResolvedCollector(hits.get(0), collectorId);
    }

    private static String normalizePackId(String packId) {
        if (packId == null) {
            return "";
        }
        String s = packId.trim();
        if (s.endsWith(".yaml")) {
            return s.substring(0, s.length() - 5);
        }
        if (s.endsWith(".yml")) {
            return s.substring(0, s.length() - 4);
        }
        return s;
    }

    private static class ResolvedCollector {
        private final CollectorConfig config;
        private final String collectorId;

        private ResolvedCollector(CollectorConfig config, String collectorId) {
            this.config = config;
            this.collectorId = collectorId;
        }
    }

    private static class ResolvedQuery {
        private final CollectorConfig config;
        private final String collectorId;
        private final QueryConfig queryConfig;

        private ResolvedQuery(CollectorConfig config, String collectorId, QueryConfig queryConfig) {
            this.config = config;
            this.collectorId = collectorId;
            this.queryConfig = queryConfig;
        }
    }
}
