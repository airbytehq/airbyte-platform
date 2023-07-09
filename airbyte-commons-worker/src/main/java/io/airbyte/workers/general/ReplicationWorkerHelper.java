/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.config.FailureReason;
import io.airbyte.config.PerformanceMetrics;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.context.ReplicationContext;
import io.airbyte.workers.context.ReplicationFeatureFlags;
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
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEvent;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the business logic that has been extracted from the DefaultReplicationWorker.
 * <p>
 * Expected lifecycle of this object is a sync.
 * <p>
 * This needs to be broken down further by responsibility. Until it happens, it holds the processing
 * of the DefaultReplicationWorker that isn't control flow related.
 */
class ReplicationWorkerHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationWorkerHelper.class);

  private final AirbyteMessageDataExtractor airbyteMessageDataExtractor;
  private final FieldSelector fieldSelector;
  private final AirbyteMapper mapper;
  private final MessageTracker messageTracker;
  private final SyncPersistence syncPersistence;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  private final ThreadedTimeTracker timeTracker;
  private long recordsRead;
  private StreamDescriptor currentDestinationStream = null;
  private ReplicationContext replicationContext = null;
  private ReplicationFeatureFlags replicationFeatureFlags = null; // NOPMD - keeping this as a placeholder
  private WorkerDestinationConfig destinationConfig = null;

  // We expect the number of operations on failures to be low, so synchronizedList should be
  // performant enough.
  private final List<FailureReason> replicationFailures = Collections.synchronizedList(new ArrayList<>());
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean hasFailed = new AtomicBoolean(false);

  public ReplicationWorkerHelper(
                                 final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                 final FieldSelector fieldSelector,
                                 final AirbyteMapper mapper,
                                 final MessageTracker messageTracker,
                                 final SyncPersistence syncPersistence,
                                 final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper,
                                 final ThreadedTimeTracker timeTracker) {
    this.airbyteMessageDataExtractor = airbyteMessageDataExtractor;
    this.fieldSelector = fieldSelector;
    this.mapper = mapper;
    this.messageTracker = messageTracker;
    this.syncPersistence = syncPersistence;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;
    this.timeTracker = timeTracker;

    this.recordsRead = 0L;
  }

  public void markCancelled() {
    cancelled.set(true);
  }

  public void markFailed() {
    hasFailed.set(true);
  }

  public void initialize(final ReplicationContext replicationContext, final ReplicationFeatureFlags replicationFeatureFlags) {
    this.replicationContext = replicationContext;
    this.replicationFeatureFlags = replicationFeatureFlags;
    this.timeTracker.trackReplicationStartTime();
  }

  public void startDestination(final AirbyteDestination destination, final StandardSyncInput syncInput, final Path jobRoot) {
    destinationConfig = WorkerUtils.syncToWorkerDestinationConfig(syncInput);
    destinationConfig.setCatalog(mapper.mapCatalog(destinationConfig.getCatalog()));
    timeTracker.trackDestinationWriteStartTime();

    try {
      destination.start(destinationConfig, jobRoot);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void startSource(final AirbyteSource source, final StandardSyncInput syncInput, final Path jobRoot) {
    final WorkerSourceConfig sourceConfig = WorkerUtils.syncToWorkerSourceConfig(syncInput);
    try {
      fieldSelector.populateFields(sourceConfig.getCatalog());
      timeTracker.trackSourceReadStartTime();
      source.start(sourceConfig, jobRoot);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void endOfReplication() {
    // Publish a complete status event for all streams associated with the connection.
    // This is to ensure that all streams end up in a terminal state and is necessary for
    // connections with destinations that do not emit messages to trigger the completion.
    publishEndOfReplicationStreamStatusEvent();
    timeTracker.trackReplicationEndTime();
  }

  public void endOfSource() {
    LOGGER.info("Total records read: {} ({})", recordsRead,
        FileUtils.byteCountToDisplaySize(messageTracker.getSyncStatsTracker().getTotalBytesEmitted()));

    fieldSelector.reportMetrics(replicationContext.sourceId());
    timeTracker.trackSourceReadEndTime();
  }

  public void endOfDestination() {
    // Publish the completed state for the last stream, if present
    final StreamDescriptor currentStream = getCurrentDestinationStream();
    if (currentStream != null) {
      replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(currentStream, replicationContext,
          AirbyteMessageOrigin.DESTINATION);
    }
    timeTracker.trackDestinationWriteEndTime();
  }

  public void trackFailure(final Throwable t) {
    replicationFailures.add(getFailureReason(t, replicationContext.jobId(), replicationContext.attempt()));
    this.handleStreamStatusFailure();
  }

  /**
   * Handles the reporting of errors that occur during replication.
   *
   * @param failureOrigin The origin of the failure.
   * @param streamSupplier A {@link Supplier} that provides the {@link StreamDescriptor} to be used in
   *        the reported incomplete replication event.
   */
  private void handleReplicationFailure(final AirbyteMessageOrigin failureOrigin, final Supplier<StreamDescriptor> streamSupplier) {
    replicationAirbyteMessageEventPublishingHelper.publishIncompleteStatusEvent(streamSupplier.get(), replicationContext, failureOrigin,
        Optional.of(StreamStatusIncompleteRunCause.FAILED));
  }

  /**
   * Handles a failure by associating it with the appropriate {@link AirbyteMessageOrigin} and active
   * stream.
   *
   */
  private void handleStreamStatusFailure() {
    handleReplicationFailure(AirbyteMessageOrigin.INTERNAL, StreamDescriptor::new);
  }

  public Optional<AirbyteMessage> processMessageFromSource(final AirbyteMessage airbyteMessage) {
    fieldSelector.filterSelectedFields(airbyteMessage);
    fieldSelector.validateSchema(airbyteMessage);

    final AirbyteMessage message = mapper.mapMessage(airbyteMessage);

    messageTracker.acceptFromSource(message);

    if (shouldPublishMessage(airbyteMessage)) {
      replicationAirbyteMessageEventPublishingHelper
          .publishStatusEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.SOURCE, message, replicationContext));
    }

    recordsRead += 1;

    if (recordsRead % 5000 == 0) {
      LOGGER.info("Records read: {} ({})", recordsRead,
          FileUtils.byteCountToDisplaySize(messageTracker.getSyncStatsTracker().getTotalBytesEmitted()));
    }

    return Optional.of(message);
  }

  public void processMessageFromDestination(final AirbyteMessage message) {
    LOGGER.info("State in DefaultReplicationWorker from destination: {}", message);
    final StreamDescriptor previousStream = currentDestinationStream;
    currentDestinationStream = airbyteMessageDataExtractor.extractStreamDescriptor(message, previousStream);
    if (currentDestinationStream != null) {
      LOGGER.debug("DESTINATION > The current stream is {}:{}", currentDestinationStream.getNamespace(), currentDestinationStream.getName());
    }

    // If the worker has moved on to the next stream, ensure that a completed status is sent
    // for the previously tracked stream.
    if (previousStream != null && !previousStream.equals(currentDestinationStream)) {
      replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(currentDestinationStream, replicationContext,
          AirbyteMessageOrigin.DESTINATION);
    }

    messageTracker.acceptFromDestination(message);
    if (message.getType() == Type.STATE) {
      syncPersistence.persist(replicationContext.connectionId(), message.getState());
    }

    if (shouldPublishMessage(message)) {
      LOGGER.debug("Publishing destination event for stream {}:{}...", currentDestinationStream.getNamespace(), currentDestinationStream.getName());
      replicationAirbyteMessageEventPublishingHelper
          .publishStatusEvent(new ReplicationAirbyteMessageEvent(AirbyteMessageOrigin.DESTINATION, message, replicationContext));
    }
  }

  private StreamDescriptor getCurrentDestinationStream() {
    return currentDestinationStream;
  }

  public ReplicationOutput getReplicationOutput() throws JsonProcessingException {
    return getReplicationOutput(null);
  }

  public ReplicationOutput getReplicationOutput(final PerformanceMetrics performanceMetrics)
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
        .withEndTime(System.currentTimeMillis())
        .withPerformanceMetrics(performanceMetrics);

    final ReplicationOutput output = new ReplicationOutput()
        .withReplicationAttemptSummary(summary)
        .withOutputCatalog(destinationConfig.getCatalog());

    final List<FailureReason> failures = getFailureReasons(replicationFailures, output);

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

  private List<FailureReason> getFailureReasons(final List<FailureReason> replicationFailures,
                                                final ReplicationOutput output) {
    // only .setFailures() if a failure occurred or if there is an AirbyteErrorTraceMessage
    final FailureReason traceMessageFailure = messageTracker.errorTraceMessageFailure(replicationContext.jobId(), replicationContext.attempt());

    final List<FailureReason> failures = new ArrayList<>();

    if (traceMessageFailure != null) {
      failures.add(traceMessageFailure);
    }
    failures.addAll(replicationFailures);

    if (!failures.isEmpty()) {
      output.setFailures(failures);
    }
    return failures;
  }

  private void publishEndOfReplicationStreamStatusEvent() {
    /*
     * If the sync has been cancelled, publish an incomplete event so that any streams in a non-terminal
     * status will be moved to incomplete/cancelled. Otherwise, publish a complete event to move those
     * streams to a complete status.
     */
    if (cancelled.get()) {
      replicationAirbyteMessageEventPublishingHelper.publishIncompleteStatusEvent(new StreamDescriptor(), replicationContext,
          AirbyteMessageOrigin.INTERNAL, Optional.of(StreamStatusIncompleteRunCause.CANCELED));
    } else {
      replicationAirbyteMessageEventPublishingHelper.publishCompleteStatusEvent(new StreamDescriptor(), replicationContext,
          AirbyteMessageOrigin.INTERNAL);
    }
  }

  /**
   * Tests whether the {@link AirbyteMessage} should be published via Micronaut's event publishing
   * mechanism.
   *
   * @param airbyteMessage The {@link AirbyteMessage} to be considered for event publishing.
   * @return {@code True} if the message should be published, false otherwise.
   */
  private static boolean shouldPublishMessage(final AirbyteMessage airbyteMessage) {
    return Type.CONTROL.equals(airbyteMessage.getType())
        || (Type.TRACE.equals(airbyteMessage.getType()) && AirbyteTraceMessage.Type.STREAM_STATUS.equals(airbyteMessage.getTrace().getType()));
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

}
