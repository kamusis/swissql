package com.swissql.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swissql.model.SamplerConfigFile;
import com.swissql.model.SamplerDefinition;
import com.swissql.model.CollectorResult;
import com.swissql.model.SessionInfo;
import com.swissql.service.DatabaseService;
import com.swissql.service.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Manages samplers for database sessions.
 */
@Component
public class SamplerManager {
    private static final Logger log = LoggerFactory.getLogger(SamplerManager.class);

    // TODO(P2): Add sampling metrics (last sample duration, failure count, last success time).
    // This helps with observability and troubleshooting sampling issues in production.

    private final SessionManager sessionManager;
    private final DatabaseService databaseService;
    private final CollectorRegistry collectorRegistry;
    private final CollectorRunner collectorRunner;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;

    private final Map<String, CollectorSampler> samplers = new ConcurrentHashMap<>();
    private final Map<String, String> samplerStoppedReasons = new ConcurrentHashMap<>();

    private final Map<String, SamplerDefinition> defaultSamplerDefinitionsById;

    /**
     * Represents the current sampler status for a session.
     */
    public static class SamplerStatus {
        private final String status;
        private final String reason;

        public SamplerStatus(String status, String reason) {
            this.status = status;
            this.reason = reason;
        }

        public String getStatus() {
            return status;
        }

        public String getReason() {
            return reason;
        }
    }

    public SamplerManager(
            SessionManager sessionManager,
            DatabaseService databaseService,
            CollectorRegistry collectorRegistry,
            CollectorRunner collectorRunner,
            ObjectMapper objectMapper
    ) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
        this.collectorRegistry = collectorRegistry;
        this.collectorRunner = collectorRunner;
        this.objectMapper = objectMapper;
        this.scheduler = Executors.newScheduledThreadPool(10);

        this.defaultSamplerDefinitionsById = loadDefaultSamplerDefinitions();
    }

    /**
     * Returns the latest snapshot for a session+sampler.
     *
     * @param sessionId the session ID
     * @param samplerId sampler id
     * @return the latest snapshot, or null if no sampler is found
     */
    public CollectorResult getSnapshot(String sessionId, String samplerId) {
        CollectorSampler sampler = samplers.get(key(sessionId, samplerId));
        if (sampler == null) {
            log.warn("No sampler found: session_id={}, sampler_id={}", sessionId, samplerId);
            return null;
        }
        return sampler.getLatestResult();
    }

    public String getStoppedReason(String sessionId, String samplerId) {
        return samplerStoppedReasons.get(key(sessionId, samplerId));
    }

    public SamplerStatus getSamplerStatus(String sessionId, String samplerId) {
        CollectorSampler sampler = samplers.get(key(sessionId, samplerId));
        if (sampler != null && sampler.isRunning()) {
            return new SamplerStatus("RUNNING", null);
        }

        String reason = samplerStoppedReasons.get(key(sessionId, samplerId));
        if (reason != null && !reason.isBlank()) {
            return new SamplerStatus("STOPPED", reason);
        }

        return new SamplerStatus("STOPPED", null);
    }

    public void restartSampler(String sessionId, String samplerId) {
        stopSampler(sessionId, samplerId);
        samplerStoppedReasons.remove(key(sessionId, samplerId));
        startSampler(sessionId, samplerId);
    }

    public void startSampler(String sessionId, String samplerId) {
        String samplerKey = key(sessionId, samplerId);
        if (samplers.containsKey(samplerKey)) {
            log.warn("Sampler already exists: session_id={}, sampler_id={}", sessionId, samplerId);
            return;
        }

        samplerStoppedReasons.remove(samplerKey);
        collectorRegistry.reloadConfigs();

        try {
            SamplerDefinition definition = getDefaultDefinitionOrThrow(samplerId);
            if (definition.getEnabled() != null && !definition.getEnabled()) {
                String reason = "Sampler disabled in default.json";
                samplerStoppedReasons.put(samplerKey, reason);
                log.warn("Sampler not started: session_id={}, sampler_id={}, reason={}", sessionId, samplerId, reason);
                return;
            }

            CollectorSampler sampler = createCollectorSampler(sessionId, samplerId, definition);
            samplers.put(samplerKey, sampler);
            sampler.start();
        } catch (Exception e) {
            log.error("Failed to start sampler: session_id={}, sampler_id={}", sessionId, samplerId, e);
        }
    }

    public void upsertSampler(String sessionId, String samplerId, SamplerDefinition definition) {
        if (definition == null) {
            throw new IllegalArgumentException("sampler definition is required");
        }

        SamplerDefinition merged = mergeWithDefaultDefinition(samplerId, definition);
        if (merged.getSchedule() == null || merged.getSchedule().getIntervalSec() == null || merged.getSchedule().getIntervalSec() <= 0) {
            throw new IllegalArgumentException("invalid schedule.interval_sec");
        }

        String samplerKey = key(sessionId, samplerId);
        stopSampler(sessionId, samplerId);
        samplerStoppedReasons.remove(samplerKey);

        if (merged.getEnabled() != null && !merged.getEnabled()) {
            return;
        }

        collectorRegistry.reloadConfigs();
        CollectorSampler sampler = createCollectorSampler(sessionId, samplerId, merged);
        samplers.put(samplerKey, sampler);
        sampler.start();
    }

    public void stopSampler(String sessionId, String samplerId) {
        CollectorSampler sampler = samplers.remove(key(sessionId, samplerId));
        if (sampler != null) {
            sampler.stop();
            log.info("Stopped sampler: session_id={}, sampler_id={}", sessionId, samplerId);
            return;
        }
    }

    /**
     * Stops and removes all samplers for a session.
     *
     * @param sessionId session id
     */
    public void stopAllSamplers(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return;
        }

        List<String> samplerIds = listSamplerIds(sessionId);
        for (String samplerId : samplerIds) {
            stopSampler(sessionId, samplerId);
        }
    }

    public List<String> listSamplerIds(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return List.of();
        }
        return samplers.keySet().stream()
                .filter(k -> k.startsWith(sessionId + ":"))
                .map(k -> k.substring((sessionId + ":").length()))
                .sorted()
                .toList();
    }

    public void cleanup() {
        samplers.values().forEach(CollectorSampler::stop);
        samplers.clear();
        samplerStoppedReasons.clear();
        scheduler.shutdown();
    }

    public CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }

    private CollectorSampler createCollectorSampler(String sessionId, String samplerId, SamplerDefinition definition) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            throw new IllegalArgumentException("Session not found: " + sessionId);
        }

        SessionInfo sessionInfo = sessionInfoOpt.get();
        Connection connection;
        try {
            connection = databaseService.getConnection(sessionInfo);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get connection for session: " + sessionId, e);
        }
        String dbType = sessionInfo.getDbType();

        // Validate that there is at least one collector pack for this db context.
        if (collectorRegistry.getConfig(connection, dbType) == null) {
            throw new IllegalStateException("No collector config found for dbType: " + dbType);
        }

        return new CollectorSampler(
                sessionId,
                samplerId,
                dbType,
                connection,
                collectorRunner,
                scheduler,
                definition,
                (stoppedSessionId, stoppedSamplerId, reason) -> {
                    String k = key(stoppedSessionId, stoppedSamplerId);
                    samplers.remove(k);
                    samplerStoppedReasons.put(k, reason);
                    log.warn("Sampler stopped: session_id={}, sampler_id={}, reason={}", stoppedSessionId, stoppedSamplerId, reason);
                }
        );
    }

    private SamplerDefinition getDefaultDefinitionOrThrow(String samplerId) {
        if (samplerId == null || samplerId.isBlank()) {
            throw new IllegalArgumentException("sampler_id is required");
        }
        SamplerDefinition def = defaultSamplerDefinitionsById.get(samplerId);
        if (def == null) {
            throw new IllegalArgumentException("Unknown sampler_id: " + samplerId);
        }
        return def;
    }

    private SamplerDefinition mergeWithDefaultDefinition(String samplerId, SamplerDefinition incoming) {
        SamplerDefinition base = getDefaultDefinitionOrThrow(samplerId);

        SamplerDefinition out = new SamplerDefinition();
        out.setSamplerId(samplerId);
        out.setEnabled(incoming.getEnabled() != null ? incoming.getEnabled() : base.getEnabled());
        out.setSchedule(incoming.getSchedule() != null ? incoming.getSchedule() : base.getSchedule());
        out.setRunPolicy(incoming.getRunPolicy() != null ? incoming.getRunPolicy() : base.getRunPolicy());
        out.setResultPolicy(incoming.getResultPolicy() != null ? incoming.getResultPolicy() : base.getResultPolicy());
        out.setTarget(incoming.getTarget() != null ? incoming.getTarget() : base.getTarget());
        return out;
    }

    private Map<String, SamplerDefinition> loadDefaultSamplerDefinitions() {
        try (InputStream is = ResourceUtils.class.getResourceAsStream("/samplers/default.json")) {
            if (is == null) {
                log.warn("samplers/default.json not found on classpath");
                return new ConcurrentHashMap<>();
            }
            SamplerConfigFile file = objectMapper.readValue(is, SamplerConfigFile.class);
            if (file == null || file.getSamplers() == null) {
                return new ConcurrentHashMap<>();
            }

            Map<String, SamplerDefinition> out = new ConcurrentHashMap<>();
            for (SamplerDefinition def : file.getSamplers()) {
                if (def == null || def.getSamplerId() == null || def.getSamplerId().isBlank()) {
                    continue;
                }
                out.put(def.getSamplerId(), def);
            }
            return out;
        } catch (Exception e) {
            log.error("Failed to load samplers/default.json", e);
            return new ConcurrentHashMap<>();
        }
    }

    private static String key(String sessionId, String samplerId) {
        return Objects.requireNonNull(sessionId, "sessionId") + ":" + Objects.requireNonNull(samplerId, "samplerId");
    }
}
