package com.swissql.sampler;

import com.swissql.model.TopSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopSampler {
    private static final Logger log = LoggerFactory.getLogger(TopSampler.class);

    private final String sessionId;
    private final Connection connection;
    private final String dbType;
    private final CollectorRegistry collectorRegistry;
    private final GenericCollector collector;
    private final ScheduledExecutorService scheduler;

    private SamplerConfig config;
    private ScheduledFuture<?> scheduledTask;
    private TopSnapshot latestSnapshot;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile CountDownLatch currentTaskLatch;

    public TopSampler(String sessionId, Connection connection, String dbType,
                      CollectorRegistry collectorRegistry, GenericCollector collector,
                      ScheduledExecutorService scheduler, SamplerConfig config) {
        this.sessionId = sessionId;
        this.connection = connection;
        this.dbType = dbType;
        this.collectorRegistry = collectorRegistry;
        this.collector = collector;
        this.scheduler = scheduler;
        this.config = config != null ? config : new SamplerConfig(10, true, true, 10);
    }

    public void start() {
        if (running.get()) {
            log.warn("Sampler already running for session: {}", sessionId);
            return;
        }

        running.set(true);
        scheduledTask = scheduler.scheduleAtFixedRate(
                this::collectSnapshot,
                0,
                config.getIntervalSec(),
                TimeUnit.SECONDS
        );
        log.info("Started TopSampler for session: {} with interval: {}s", sessionId, config.getIntervalSec());
    }

    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }

        // Wait for current task to complete (with timeout)
        CountDownLatch latch = currentTaskLatch;
        if (latch != null) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for sampler task to complete for session: {}", sessionId);
            }
        }

        log.info("Stopped TopSampler for session: {}", sessionId);

        // TODO(P1): Consider explicitly closing Connection in stop() to make ownership clear.
        // Current design relies on HikariDataSource.close() during session disconnect to
        // close all connections. While this works, explicit closure would make
        // boundary clearer and prevent any potential race conditions if a slow collectSnapshot
        // runs after disconnect. Alternatively, add connection validity checks before use.
    }

    public void updateConfig(SamplerConfig newConfig) {
        boolean wasRunning = running.get();
        if (wasRunning) {
            stop();
        }

        if (newConfig.getIntervalSec() != null) {
            config.setIntervalSec(newConfig.getIntervalSec());
        }
        if (newConfig.getEnableTopSql() != null) {
            config.setEnableTopSql(newConfig.getEnableTopSql());
        }
        if (newConfig.getEnableTopSessions() != null) {
            config.setEnableTopSessions(newConfig.getEnableTopSessions());
        }
        if (newConfig.getMaxItems() != null) {
            config.setMaxItems(newConfig.getMaxItems());
        }

        if (wasRunning) {
            start();
        }
    }

    public TopSnapshot getLatestSnapshot() {
        return latestSnapshot;
    }

    public boolean isRunning() {
        return running.get();
    }

    private void collectSnapshot() {
        if (!running.get()) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        currentTaskLatch = latch;

        try {
            var collectorConfig = collectorRegistry.getConfig(connection, dbType);

            // TODO(P1): Add connection validity check before using it, e.g.:
            // if (conn == null || conn.isClosed()) { return; }
            // This prevents potential issues if connection is closed by pool management.
            if (collectorConfig == null) {
                log.error("No collector config found for dbType: {}", dbType);
                return;
            }

            TopSnapshot snapshot = (TopSnapshot) collector.collect(connection, "top", collectorConfig);
            snapshot.setDbType(dbType);
            snapshot.setIntervalSec(config.getIntervalSec());
            latestSnapshot = snapshot;

            log.debug("Collected snapshot for session: {}", sessionId);
        } catch (Exception e) {
            if (running.get()) {
                log.error("Failed to collect snapshot for session: {}", sessionId, e);
            }
        } finally {
            latch.countDown();
        }
    }
}
