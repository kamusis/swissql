package com.swissql.sampler;

import com.swissql.model.CollectorResult;
import com.swissql.model.SamplerDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A generic sampler that periodically executes a collector (by {@code collector_id} or {@code collector_ref})
 * and stores the latest {@link CollectorResult}.
 *
 * <p>This is the Phase 4 replacement for {@code TopSampler}. It contains no business logic and relies on
 * collector YAML contracts for what is executed and how results are shaped.
 */
public class CollectorSampler {
    private static final Logger log = LoggerFactory.getLogger(CollectorSampler.class);

    private final String sessionId;
    private final String samplerId;
    private final String dbType;
    private final Connection connection;
    private final CollectorRunner collectorRunner;
    private final ScheduledExecutorService scheduler;
    private final SamplerDefinition definition;
    private final StopListener stopListener;

    private ScheduledFuture<?> scheduledTask;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean collecting = new AtomicBoolean(false);
    private volatile CountDownLatch currentTaskLatch;

    private volatile CollectorResult latestResult;
    private final AtomicBoolean stopReasonLogged = new AtomicBoolean(false);

    /**
     * Listener for sampler stop events.
     */
    public interface StopListener {
        /**
         * Called when the sampler is stopped due to a reason.
         *
         * @param sessionId session id
         * @param samplerId sampler id
         * @param reason stop reason
         */
        void onStopped(String sessionId, String samplerId, String reason);
    }

    /**
     * Creates a collector sampler.
     *
     * @param sessionId session id
     * @param samplerId sampler id
     * @param dbType db type
     * @param connection database connection
     * @param collectorRunner collector runner
     * @param scheduler scheduler
     * @param definition sampler definition (schedule/run_policy/target)
     * @param stopListener stop listener
     */
    public CollectorSampler(
            String sessionId,
            String samplerId,
            String dbType,
            Connection connection,
            CollectorRunner collectorRunner,
            ScheduledExecutorService scheduler,
            SamplerDefinition definition,
            StopListener stopListener
    ) {
        this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
        this.samplerId = Objects.requireNonNull(samplerId, "samplerId");
        this.dbType = Objects.requireNonNull(dbType, "dbType");
        this.connection = Objects.requireNonNull(connection, "connection");
        this.collectorRunner = Objects.requireNonNull(collectorRunner, "collectorRunner");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.definition = Objects.requireNonNull(definition, "definition");
        this.stopListener = stopListener;
    }

    /**
     * Starts the sampler.
     */
    public void start() {
        if (running.get()) {
            log.warn("Sampler already running: session_id={}, sampler_id={}", sessionId, samplerId);
            return;
        }

        Integer intervalSec = definition.getSchedule() != null ? definition.getSchedule().getIntervalSec() : null;
        if (intervalSec == null || intervalSec <= 0) {
            stopDueToReason("invalid schedule.interval_sec");
            return;
        }

        running.set(true);
        scheduledTask = scheduler.scheduleAtFixedRate(this::collectOnce, 0, intervalSec, TimeUnit.SECONDS);
        log.info("Started CollectorSampler: session_id={}, sampler_id={}, interval_sec={}", sessionId, samplerId, intervalSec);
    }

    /**
     * Stops the sampler.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }

        CountDownLatch latch = currentTaskLatch;
        if (latch != null) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for sampler to stop: session_id={}, sampler_id={}", sessionId, samplerId);
            }
        }

        log.info("Stopped CollectorSampler: session_id={}, sampler_id={}", sessionId, samplerId);
    }

    /**
     * Returns whether the sampler is running.
     *
     * @return true if running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Returns the latest collected result.
     *
     * @return latest collector result
     */
    public CollectorResult getLatestResult() {
        return latestResult;
    }

    private void collectOnce() {
        if (!running.get()) {
            return;
        }

        if (!ensureConnectionValidOrStop()) {
            return;
        }

        String onOverlap = definition.getRunPolicy() != null ? definition.getRunPolicy().getOnOverlap() : null;
        boolean skipOverlap = onOverlap == null || onOverlap.isBlank() || "skip".equalsIgnoreCase(onOverlap);
        if (skipOverlap && !collecting.compareAndSet(false, true)) {
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        currentTaskLatch = latch;

        try {
            String collectorId = definition.getTarget() != null ? definition.getTarget().getCollectorId() : null;
            String collectorRef = definition.getTarget() != null ? definition.getTarget().getCollectorRef() : null;

            CollectorResult result = collectorRunner.runCollector(connection, dbType, collectorId, collectorRef);
            if (result == null) {
                stopDueToReason("collector returned null result");
                return;
            }
            latestResult = result;
        } catch (Exception e) {
            if (running.get()) {
                log.error("Failed to collect: session_id={}, sampler_id={}", sessionId, samplerId, e);
            }



            // Sampling is only useful if it can actually collect. If the collector cannot be resolved
            // or repeatedly fails, stop the sampler and surface the reason to the client.
            stopDueToReason(e.getMessage() == null || e.getMessage().isBlank()
                    ? "collector execution failed"
                    : e.getMessage());
        } finally {
            latch.countDown();
            collecting.set(false);
        }
    }

    private boolean ensureConnectionValidOrStop() {
        try {
            if (connection.isClosed()) {
                stopDueToReason("connection is closed");
                return false;
            }
            if (!connection.isValid(2)) {
                stopDueToReason("connection is not valid");
                return false;
            }
            return true;
        } catch (Exception e) {
            stopDueToReason("connection validity check failed: " + e.getMessage());
            return false;
        }
    }

    private void stopDueToReason(String reason) {
        if (!stopReasonLogged.compareAndSet(false, true)) {
            stop();
            return;
        }

        log.warn("Stopping CollectorSampler: session_id={}, sampler_id={}, reason={}", sessionId, samplerId, reason);
        stop();

        if (stopListener != null) {
            try {
                stopListener.onStopped(sessionId, samplerId, reason);
            } catch (Exception e) {
                log.warn("Failed to notify stop listener: session_id={}, sampler_id={}", sessionId, samplerId, e);
            }
        }
    }
}
