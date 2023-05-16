/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.State;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HandleStreamStatus;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageOrigin;
import io.airbyte.workers.internal.book_keeping.MessageTracker;
import io.airbyte.workers.internal.book_keeping.SyncStatsBuilder;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This worker is the "data shovel" of ETL. It is responsible for moving data from the Source
 * container to the Destination container. It manages the full lifecycle of this process. This
 * includes:
 * <ul>
 * <li>Starting the Source and Destination containers</li>
 * <li>Passing data from Source to Destination</li>
 * <li>Executing any configured map-only operations (Mappers) in between the Source and
 * Destination</li>
 * <li>Collecting metadata about the data that is passing from Source to Destination</li>
 * <li>Listening for state messages emitted from the Destination to keep track of what data has been
 * replicated.</li>
 * <li>Handling shutdown of the Source and Destination</li>
 * <li>Handling failure cases and returning state for partially completed replications (so that the
 * next replication can pick up where it left off instead of starting from the beginning)</li>
 * </ul>
 */
@SuppressWarnings("PMD.AvoidPrintStackTrace")
public class DefaultReplicationWorker implements ReplicationWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReplicationWorker.class);

  private final String jobId;
  private final int attempt;
  private final ReplicationWorkerHelper replicationWorkerHelper;
  private final AirbyteSource source;
  private final AirbyteMapper mapper;
  private final AirbyteDestination destination;
  private final MessageTracker messageTracker;
  private final SyncPersistence syncPersistence;
  private final ExecutorService executors;
  private final AtomicBoolean cancelled;
  private final AtomicBoolean hasFailed;
  private final RecordSchemaValidator recordSchemaValidator;
  private final WorkerMetricReporter metricReporter;
  private final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone;
  private final FeatureFlagClient featureFlagClient;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;

  public DefaultReplicationWorker(final String jobId,
                                  final int attempt,
                                  final AirbyteSource source,
                                  final AirbyteMapper mapper,
                                  final AirbyteDestination destination,
                                  final MessageTracker messageTracker,
                                  final SyncPersistence syncPersistence,
                                  final RecordSchemaValidator recordSchemaValidator,
                                  final FieldSelector fieldSelector,
                                  final WorkerMetricReporter metricReporter,
                                  final ConnectorConfigUpdater connectorConfigUpdater,
                                  final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                  final FeatureFlagClient featureFlagClient,
                                  final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                  final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.replicationWorkerHelper = new ReplicationWorkerHelper(airbyteMessageDataExtractor, fieldSelector, mapper, messageTracker, syncPersistence,
        connectorConfigUpdater, replicationAirbyteMessageEventPublishingHelper);
    this.source = source;
    this.mapper = mapper;
    this.destination = destination;
    this.messageTracker = messageTracker;
    this.syncPersistence = syncPersistence;
    this.executors = Executors.newFixedThreadPool(2);
    this.recordSchemaValidator = recordSchemaValidator;
    this.metricReporter = metricReporter;
    this.srcHeartbeatTimeoutChaperone = srcHeartbeatTimeoutChaperone;
    this.featureFlagClient = featureFlagClient;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;

    this.cancelled = new AtomicBoolean(false);
    this.hasFailed = new AtomicBoolean(false);
  }

  /**
   * Run executes two threads. The first pipes data from STDOUT of the source to STDIN of the
   * destination. The second listen on STDOUT of the destination. The goal of this second thread is to
   * detect when the destination emits state messages. Only state messages emitted by the destination
   * should be treated as state that is safe to return from run. In the case when the destination
   * emits no state, we fall back on whatever state is pass in as an argument to this method.
   *
   * @param syncInput all configuration for running replication
   * @param jobRoot file root that worker is allowed to use
   * @return output of the replication attempt (including state)
   * @throws WorkerException exception from worker
   */
  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public final ReplicationOutput run(final StandardSyncInput syncInput, final Path jobRoot) throws WorkerException {
    LOGGER.info("start sync worker. job id: {} attempt id: {}", jobId, attempt);
    LOGGER
        .info("Committing states from " + (shouldCommitStateAsap(syncInput) ? "replication" : "persistState")
            + " activity");
    if (shouldCommitStatsAsap(syncInput)) {
      LOGGER.info("Committing stats from replication activity");
    }
    LineGobbler.startSection("REPLICATION");

    // todo (cgardens) - this should not be happening in the worker. this is configuration information
    // that is independent of workflow executions.
    final WorkerDestinationConfig destinationConfig = WorkerUtils.syncToWorkerDestinationConfig(syncInput);
    destinationConfig.setCatalog(mapper.mapCatalog(destinationConfig.getCatalog()));

    final ThreadedTimeTracker timeTracker = new ThreadedTimeTracker();
    timeTracker.trackReplicationStartTime();

    final AtomicReference<FailureReason> replicationRunnableFailureRef = new AtomicReference<>();
    final AtomicReference<FailureReason> destinationRunnableFailureRef = new AtomicReference<>();

    try {
      LOGGER.info("configured sync modes: {}", syncInput.getCatalog().getStreams()
          .stream()
          .collect(Collectors.toMap(s -> s.getStream().getNamespace() + "." + s.getStream().getName(),
              s -> String.format("%s - %s", s.getSyncMode(), s.getDestinationSyncMode()))));
      final WorkerSourceConfig sourceConfig = WorkerUtils.syncToWorkerSourceConfig(syncInput);

      ApmTraceUtils.addTagsToTrace(destinationConfig.getConnectionId(), jobId, jobRoot);
      final ReplicationContext replicationContext =
          new ReplicationContext(syncInput.getConnectionId(), sourceConfig.getSourceId(), destinationConfig.getDestinationId(), Long.parseLong(jobId),
              attempt, syncInput.getWorkspaceId());
      replicate(jobRoot, destinationConfig, timeTracker, replicationRunnableFailureRef, destinationRunnableFailureRef, sourceConfig,
          replicationContext, shouldCommitStateAsap(syncInput), shouldHandleStreamStatus(syncInput));
      timeTracker.trackReplicationEndTime();

      return getReplicationOutput(syncInput, destinationConfig, replicationRunnableFailureRef, destinationRunnableFailureRef, timeTracker);
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw new WorkerException("Sync failed", e);
    }

  }

  private void replicate(final Path jobRoot,
                         final WorkerDestinationConfig destinationConfig,
                         final ThreadedTimeTracker timeTracker,
                         final AtomicReference<FailureReason> replicationRunnableFailureRef,
                         final AtomicReference<FailureReason> destinationRunnableFailureRef,
                         final WorkerSourceConfig sourceConfig,
                         final ReplicationContext replicationContext,
                         final boolean commitStatesAsap,
                         final boolean shouldHandleStreamStatus) {
    final Map<String, String> mdc = MDC.getCopyOfContextMap();

    // note: resources are closed in the opposite order in which they are declared. thus source will be
    // closed first (which is what we want).
    try (recordSchemaValidator; syncPersistence; srcHeartbeatTimeoutChaperone; destination; source) {
      destination.start(destinationConfig, jobRoot);
      timeTracker.trackSourceReadStartTime();
      source.start(sourceConfig, jobRoot);
      timeTracker.trackDestinationWriteStartTime();
      replicationWorkerHelper.beforeReplication(sourceConfig.getCatalog());

      // note: `whenComplete` is used instead of `exceptionally` so that the original exception is still
      // thrown
      final CompletableFuture<?> readFromDstThread = CompletableFuture.runAsync(
          readFromDstRunnable(destination, cancelled, replicationWorkerHelper, replicationAirbyteMessageEventPublishingHelper, replicationContext,
              mdc, timeTracker, commitStatesAsap, shouldHandleStreamStatus),
          executors)
          .whenComplete((msg, ex) -> {
            if (ex != null) {
              ApmTraceUtils.addExceptionToTrace(ex);
              if (ex.getCause() instanceof DestinationException) {
                destinationRunnableFailureRef.set(FailureHelper.destinationFailure(ex, replicationContext.jobId(), replicationContext.attempt()));
              } else {
                destinationRunnableFailureRef.set(FailureHelper.replicationFailure(ex, replicationContext.jobId(), replicationContext.attempt()));
              }
            }
          });

      final CompletableFuture<Void> readSrcAndWriteDstThread = CompletableFuture.runAsync(readFromSrcAndWriteToDstRunnable(
          source,
          destination,
          replicationWorkerHelper,
          replicationAirbyteMessageEventPublishingHelper,
          replicationContext,
          cancelled,
          mdc,
          timeTracker,
          shouldHandleStreamStatus), executors)
          .whenComplete((msg, ex) -> {
            if (ex != null) {
              ApmTraceUtils.addExceptionToTrace(ex);
              replicationRunnableFailureRef.set(getFailureReason(ex.getCause(), replicationContext.jobId(), replicationContext.attempt()));
            }
          });

      try {
        srcHeartbeatTimeoutChaperone.runWithHeartbeatThread(readSrcAndWriteDstThread);
      } catch (final HeartbeatTimeoutChaperone.HeartbeatTimeoutException ex) {
        ApmTraceUtils.addExceptionToTrace(ex);
        replicationRunnableFailureRef.set(getFailureReason(ex, replicationContext.jobId(), replicationContext.attempt()));
      }

      LOGGER.info("Waiting for source and destination threads to complete.");
      // CompletableFuture#allOf waits until all futures finish before returning, even if one throws an
      // exception. So in order to handle exceptions from a future immediately without needing to wait for
      // the other future to finish, we first call CompletableFuture#anyOf.
      CompletableFuture.anyOf(readSrcAndWriteDstThread, readFromDstThread).get();
      LOGGER.info("One of source or destination thread complete. Waiting on the other.");
      CompletableFuture.allOf(readSrcAndWriteDstThread, readFromDstThread).get();
      LOGGER.info("Source and destination threads complete.");

      // Publish a complete status event for all streams associated with the connection.
      // This is to ensure that all streams end up in a complete state and is necessary for
      // connections with destinations that do not emit messages to trigger the completion.
      replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(new StreamDescriptor(), replicationContext,
          AirbyteMessageOrigin.INTERNAL);
    } catch (final Exception e) {
      hasFailed.set(true);
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.error("Sync worker failed.", e);
    } finally {
      executors.shutdownNow();
    }
  }

  @VisibleForTesting
  static FailureReason getFailureReason(final Throwable ex, final long jobId, final int attempt) {
    if (ex instanceof SourceException) {
      return FailureHelper.sourceFailure(ex, Long.valueOf(jobId), attempt);
    } else if (ex instanceof DestinationException) {
      return FailureHelper.destinationFailure(ex, Long.valueOf(jobId), attempt);
    } else if (ex instanceof HeartbeatTimeoutChaperone.HeartbeatTimeoutException) {
      return FailureHelper.sourceHeartbeatFailure(ex, Long.valueOf(jobId), attempt);
    } else {
      return FailureHelper.replicationFailure(ex, Long.valueOf(jobId), attempt);
    }
  }

  @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
  private static Runnable readFromDstRunnable(final AirbyteDestination destination,
                                              final AtomicBoolean cancelled,
                                              final ReplicationWorkerHelper replicationWorkerHelper,
                                              final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper,
                                              final ReplicationContext replicationContext,
                                              final Map<String, String> mdc,
                                              final ThreadedTimeTracker timeHolder,
                                              final boolean commitStatesAsap,
                                              final boolean handleStreamStatus) {
    return () -> {
      MDC.setContextMap(mdc);
      LOGGER.info("Destination output thread started.");
      try {
        while (!cancelled.get() && !destination.isFinished()) {
          final Optional<AirbyteMessage> messageOptional;
          try {
            messageOptional = destination.attemptRead();
          } catch (final Exception e) {
            throw new DestinationException("Destination process read attempt failed", e);
          }
          if (messageOptional.isPresent()) {
            replicationWorkerHelper.processMessageFromDestination(messageOptional.get(), commitStatesAsap, handleStreamStatus, replicationContext);
          }
        }
        timeHolder.trackDestinationWriteEndTime();
        if (!cancelled.get() && destination.getExitValue() != 0) {
          throw new DestinationException("Destination process exited with non-zero exit code " + destination.getExitValue());
        } else {
          // Publish the completed state for the last stream, if present
          final StreamDescriptor currentStream = replicationWorkerHelper.getCurrentDestinationStream();
          if (handleStreamStatus && currentStream != null) {
            replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(currentStream, replicationContext,
                AirbyteMessageOrigin.DESTINATION);
          }
        }
      } catch (final Exception e) {
        if (!cancelled.get()) {
          // Although this thread is closed first, it races with the destination's closure and can attempt one
          // final read after the destination is closed before it's terminated.
          // This read will fail and throw an exception. Because of this, throw exceptions only if the worker
          // was not cancelled.

          if (e instanceof DestinationException) {
            if (handleStreamStatus) {
              replicationAirbyteMessageEventPublishingHelper.publishIncompleteStatusEvent(replicationWorkerHelper.getCurrentDestinationStream(),
                  replicationContext,
                  AirbyteMessageOrigin.DESTINATION);
            }
            // Surface Destination exceptions directly so that they can be classified properly by the worker
            throw e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
  private static Runnable readFromSrcAndWriteToDstRunnable(final AirbyteSource source,
                                                           final AirbyteDestination destination,
                                                           final ReplicationWorkerHelper replicationWorkerHelper,
                                                           final ReplicationAirbyteMessageEventPublishingHelper replicationEventPublishingHelper,
                                                           final ReplicationContext replicationContext,
                                                           final AtomicBoolean cancelled,
                                                           final Map<String, String> mdc,
                                                           final ThreadedTimeTracker timeHolder,
                                                           final boolean shouldHandleStreamStatus) {
    return () -> {
      MDC.setContextMap(mdc);
      LOGGER.info("Replication thread started.");

      try {
        while (!cancelled.get() && !source.isFinished()) {
          final Optional<AirbyteMessage> messageOptional;
          try {
            messageOptional = source.attemptRead();
          } catch (final Exception e) {
            throw new SourceException("Source process read attempt failed", e);
          }

          if (messageOptional.isPresent()) {
            final AirbyteMessage airbyteMessage = messageOptional.get();
            final Optional<AirbyteMessage> processedAirbyteMessage =
                replicationWorkerHelper.processMessageFromSource(airbyteMessage, replicationContext, shouldHandleStreamStatus);

            if (processedAirbyteMessage.isPresent()) {
              final AirbyteMessage message = processedAirbyteMessage.get();
              try {
                if (message.getType() == Type.RECORD || message.getType() == Type.STATE) {
                  destination.accept(message);
                }
              } catch (final Exception e) {
                throw new DestinationException("Destination process message delivery failed", e);
              }
            }
          } else {
            LOGGER.info("Source has no more messages, closing connection.");
            try {
              source.close();
            } catch (final Exception e) {
              throw new SourceException("Source didn't exit properly - check the logs!", e);
            }
          }
        }
        timeHolder.trackSourceReadEndTime();
        replicationWorkerHelper.endOfSource(replicationContext);

        try {
          destination.notifyEndOfInput();
        } catch (final Exception e) {
          throw new DestinationException("Destination process end of stream notification failed", e);
        }
        if (!cancelled.get() && source.getExitValue() != 0) {
          throw new SourceException("Source process exited with non-zero exit code " + source.getExitValue());
        }
      } catch (final Exception e) {
        if (!cancelled.get()) {
          // Although this thread is closed first, it races with the source's closure and can attempt one
          // final read after the source is closed before it's terminated.
          // This read will fail and throw an exception. Because of this, throw exceptions only if the worker
          // was not cancelled.

          if (e instanceof SourceException || e instanceof DestinationException) {
            // Surface Source and Destination exceptions directly so that they can be classified properly by the
            // worker
            if (shouldHandleStreamStatus) {
              replicationEventPublishingHelper.publishIncompleteStatusEvent(replicationWorkerHelper.getCurrentSourceStream(),
                  replicationContext,
                  e instanceof SourceException ? AirbyteMessageOrigin.SOURCE : AirbyteMessageOrigin.DESTINATION);
            }
            throw e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  private ReplicationOutput getReplicationOutput(final StandardSyncInput syncInput,
                                                 final WorkerDestinationConfig destinationConfig,
                                                 final AtomicReference<FailureReason> replicationRunnableFailureRef,
                                                 final AtomicReference<FailureReason> destinationRunnableFailureRef,
                                                 final ThreadedTimeTracker timeTracker)
      throws JsonProcessingException {
    final ReplicationStatus outputStatus;
    // First check if the process was cancelled. Cancellation takes precedence over failures.
    if (cancelled.get()) {
      outputStatus = ReplicationStatus.CANCELLED;
      // if the process was not cancelled but still failed, then it's an actual failure
    } else if (hasFailed.get()) {
      outputStatus = ReplicationStatus.FAILED;
    } else {
      outputStatus = ReplicationStatus.COMPLETED;
    }

    final boolean hasReplicationCompleted = outputStatus == ReplicationStatus.COMPLETED;
    final SyncStats totalSyncStats = getTotalStats(timeTracker, hasReplicationCompleted);
    final List<StreamSyncStats> streamSyncStats = SyncStatsBuilder.getPerStreamStats(messageTracker.getSyncStatsTracker(),
        hasReplicationCompleted);

    if (!hasReplicationCompleted && messageTracker.getSyncStatsTracker().getUnreliableStateTimingMetrics()) {
      LOGGER.warn("Could not reliably determine committed record counts, committed record stats will be set to null");
    }

    final ReplicationAttemptSummary summary = new ReplicationAttemptSummary()
        .withStatus(outputStatus)
        // TODO records and bytes synced should no longer be used as we are consuming total stats, we should
        // make a pass to remove them.
        .withRecordsSynced(messageTracker.getSyncStatsTracker().getTotalRecordsEmitted())
        .withBytesSynced(messageTracker.getSyncStatsTracker().getTotalBytesEmitted())
        .withTotalStats(totalSyncStats)
        .withStreamStats(streamSyncStats)
        .withStartTime(timeTracker.getReplicationStartTime())
        .withEndTime(System.currentTimeMillis());

    final ReplicationOutput output = new ReplicationOutput()
        .withReplicationAttemptSummary(summary)
        .withOutputCatalog(destinationConfig.getCatalog());

    final List<FailureReason> failures = getFailureReasons(replicationRunnableFailureRef, destinationRunnableFailureRef,
        output);

    if (!shouldCommitStateAsap(syncInput)) {
      prepStateForLaterSaving(syncInput, output);
    }

    final ObjectMapper mapper = new ObjectMapper();
    LOGGER.info("sync summary: {}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
    LOGGER.info("failures: {}", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(failures));
    LineGobbler.endSection("REPLICATION");

    return output;
  }

  private SyncStats getTotalStats(final ThreadedTimeTracker timeTracker, final boolean hasReplicationCompleted) {
    final SyncStats totalSyncStats = SyncStatsBuilder.getTotalStats(messageTracker.getSyncStatsTracker(), hasReplicationCompleted);
    totalSyncStats.setReplicationStartTime(timeTracker.getReplicationStartTime());
    totalSyncStats.setReplicationEndTime(timeTracker.getReplicationEndTime());
    totalSyncStats.setSourceReadStartTime(timeTracker.getSourceReadStartTime());
    totalSyncStats.setSourceReadEndTime(timeTracker.getSourceReadEndTime());
    totalSyncStats.setDestinationWriteStartTime(timeTracker.getDestinationWriteStartTime());
    totalSyncStats.setDestinationWriteEndTime(timeTracker.getDestinationWriteEndTime());

    return totalSyncStats;
  }

  /**
   * Extracts state out to the {@link ReplicationOutput} so it can be later saved in the
   * PersistStateActivity - State is NOT SAVED here.
   *
   * @param syncInput sync input
   * @param output sync output
   */
  private void prepStateForLaterSaving(final StandardSyncInput syncInput, final ReplicationOutput output) {
    if (messageTracker.getSourceOutputState().isPresent()) {
      LOGGER.info("Source output at least one state message");
    } else {
      LOGGER.info("Source did not output any state messages");
    }

    if (messageTracker.getDestinationOutputState().isPresent()) {
      LOGGER.info("State capture: Updated state to: {}", messageTracker.getDestinationOutputState());
      final State state = messageTracker.getDestinationOutputState().get();
      output.withState(state);
    } else if (syncInput.getState() != null) {
      LOGGER.warn("State capture: No new state, falling back on input state: {}", syncInput.getState());
      output.withState(syncInput.getState());
    } else {
      LOGGER.warn("State capture: No state retained.");
    }

    if (messageTracker.getSyncStatsTracker().getUnreliableStateTimingMetrics()) {
      metricReporter.trackStateMetricTrackerError();
    }
  }

  private List<FailureReason> getFailureReasons(final AtomicReference<FailureReason> replicationRunnableFailureRef,
                                                final AtomicReference<FailureReason> destinationRunnableFailureRef,
                                                final ReplicationOutput output) {
    // only .setFailures() if a failure occurred or if there is an AirbyteErrorTraceMessage
    final FailureReason sourceFailure = replicationRunnableFailureRef.get();
    final FailureReason destinationFailure = destinationRunnableFailureRef.get();
    final FailureReason traceMessageFailure = messageTracker.errorTraceMessageFailure(Long.valueOf(jobId), attempt);

    final List<FailureReason> failures = new ArrayList<>();

    if (traceMessageFailure != null) {
      failures.add(traceMessageFailure);
    }

    if (sourceFailure != null) {
      failures.add(sourceFailure);
    }
    if (destinationFailure != null) {
      failures.add(destinationFailure);
    }
    if (!failures.isEmpty()) {
      output.setFailures(failures);
    }
    return failures;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void cancel() {
    // Resources are closed in the opposite order they are declared.
    LOGGER.info("Cancelling replication worker...");
    try {
      executors.awaitTermination(10, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.error("Unable to cancel due to interruption.", e);
    }
    cancelled.set(true);

    LOGGER.info("Cancelling destination...");
    try {
      destination.cancel();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.info("Error cancelling destination: ", e);
    }

    LOGGER.info("Cancelling source...");
    try {
      source.cancel();
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      LOGGER.info("Error cancelling source: ", e);
    }

  }

  /**
   * Helper function to read the shouldCommitStateAsap feature flag.
   */
  public static boolean shouldCommitStateAsap(final StandardSyncInput syncInput) {
    return syncInput.getCommitStateAsap() != null && syncInput.getCommitStateAsap();
  }

  /**
   * Helper function to read the shouldCommitStatsAsap feature flag.
   */
  public static boolean shouldCommitStatsAsap(final StandardSyncInput syncInput) {
    // For consistency, we should only be committing stats early if we are committing states early.
    // Otherwise, we are risking stats discrepancy as we are committing stats for states that haven't
    // been persisted yet.
    return shouldCommitStateAsap(syncInput) && syncInput.getCommitStatsAsap() != null && syncInput.getCommitStatsAsap();
  }

  /**
   * Helper function to read the status of the {@link HandleStreamStatus} feature flag once at the
   * start of the replication exection.
   *
   * @param standardSyncInput The {@link StandardSyncInput} that contains context information
   * @return The result of checking the status of the {@link HandleStreamStatus} feature flag.
   */
  private boolean shouldHandleStreamStatus(final StandardSyncInput standardSyncInput) {
    return featureFlagClient.boolVariation(HandleStreamStatus.INSTANCE, new Workspace(standardSyncInput.getWorkspaceId()));
  }

}
