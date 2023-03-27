/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.sync_persistence;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.StateApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AttemptStats;
import io.airbyte.api.client.model.generated.AttemptStreamStats;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.SaveStatsRequestBody;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.config.State;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteEstimateTraceMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.workers.internal.book_keeping.DefaultSyncStatsTracker;
import io.airbyte.workers.internal.book_keeping.SyncStatsBuilder;
import io.airbyte.workers.internal.book_keeping.SyncStatsTracker;
import io.airbyte.workers.internal.state_aggregator.StateAggregator;
import io.airbyte.workers.internal.state_aggregator.StateAggregatorFactory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.annotation.Creator;
import jakarta.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation of the SyncPersistence
 * <p>
 * Persistence operations are delegated to an API and batched with a regular interval. Buffering is
 * handled in memory.
 * <p>
 * This implementation is meant to work for a single sync at time. Using it with data from different
 * connections will end up mixing the data. Given the scope, it is annotated as `@Prototype` which
 * tells micronaut to re-create a fresh instance everytime the SyncPersistence is requested.
 */
@Slf4j
@Prototype
public class SyncPersistenceImpl implements SyncPersistence {

  final long runImmediately = 0;
  final long flushTerminationTimeoutInSeconds = 60;

  private UUID connectionId;
  private Long jobId;
  private Integer attemptNumber;
  private ConfiguredAirbyteCatalog configuredAirbyteCatalog;
  private final StateApi stateApi;
  private final AttemptApi attemptApi;
  private final StateAggregatorFactory stateAggregatorFactory;

  private final SyncStatsTracker syncStatsTracker;
  private SaveStatsRequestBody statsToPersist;
  private boolean isReceivingStats;

  private StateAggregator stateBuffer;
  private StateAggregator stateToFlush;
  private final ScheduledExecutorService stateFlushExecutorService;
  private ScheduledFuture<?> stateFlushFuture;

  private boolean onlyFlushAtTheEnd;
  private final long stateFlushPeriodInSeconds;

  @Creator
  public SyncPersistenceImpl(final StateApi stateApi,
                             final AttemptApi attemptApi,
                             final StateAggregatorFactory stateAggregatorFactory,
                             @Named("syncPersistenceExecutorService") final ScheduledExecutorService scheduledExecutorService,
                             @Value("${airbyte.worker.replication.persistence-flush-period-sec}") final long stateFlushPeriodInSeconds) {
    this(stateApi, attemptApi, stateAggregatorFactory, new DefaultSyncStatsTracker(), scheduledExecutorService, stateFlushPeriodInSeconds);
  }

  public SyncPersistenceImpl(final StateApi stateApi,
                             final AttemptApi attemptApi,
                             final StateAggregatorFactory stateAggregatorFactory,
                             final SyncStatsTracker syncStatsTracker,
                             @Named("syncPersistenceExecutorService") final ScheduledExecutorService scheduledExecutorService,
                             @Value("${airbyte.worker.replication.persistence-flush-period-sec}") final long stateFlushPeriodInSeconds) {
    this.stateApi = stateApi;
    this.attemptApi = attemptApi;
    this.stateAggregatorFactory = stateAggregatorFactory;
    this.stateFlushExecutorService = scheduledExecutorService;
    this.stateBuffer = this.stateAggregatorFactory.create();
    this.stateFlushPeriodInSeconds = stateFlushPeriodInSeconds;
    this.syncStatsTracker = syncStatsTracker;
    this.onlyFlushAtTheEnd = false;
    this.isReceivingStats = false;
  }

  @Override
  public void setConnectionContext(final UUID connectionId,
                                   final Long jobId,
                                   final Integer attemptNumber,
                                   final ConfiguredAirbyteCatalog configuredAirbyteCatalog) {
    this.connectionId = connectionId;
    this.jobId = jobId;
    this.attemptNumber = attemptNumber;
    this.configuredAirbyteCatalog = configuredAirbyteCatalog;
  }

  @Override
  @Trace
  public void persist(final UUID connectionId, final AirbyteStateMessage stateMessage) {
    if (this.connectionId == null) {
      this.connectionId = connectionId;
    } else if (!this.connectionId.equals(connectionId)) {
      throw new IllegalArgumentException("Invalid connectionId " + connectionId + ", expected " + this.connectionId);
    }

    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_BUFFERING, 1);
    stateBuffer.ingest(stateMessage);
    startBackgroundFlushStateTask(connectionId, stateMessage);
  }

  private void startBackgroundFlushStateTask(final UUID connectionId, final AirbyteStateMessage stateMessage) {
    if (stateFlushFuture != null || onlyFlushAtTheEnd) {
      return;
    }

    // Fetch the current persisted state to see if it is a state migration.
    // In case of a state migration, we only flush at the end of the sync to avoid dropping states in
    // case of a sync failure
    final ConnectionState currentPersistedState;
    try {
      currentPersistedState = stateApi.getState(new ConnectionIdRequestBody().connectionId(connectionId));
    } catch (final ApiException e) {
      log.warn("Failed to check current state for connectionId {}, it will be retried next time we see a state", connectionId, e);
      return;
    }
    if (isMigration(currentPersistedState, stateMessage) && stateMessage.getType() == AirbyteStateType.STREAM) {
      log.info("State type migration from LEGACY to STREAM detected, all states will be persisted at the end of the sync");
      onlyFlushAtTheEnd = true;
      return;
    }

    // Making sure we only start one of background flush task
    synchronized (this) {
      if (stateFlushFuture == null) {
        log.info("starting state flush thread for connectionId " + connectionId);
        stateFlushFuture =
            stateFlushExecutorService.scheduleAtFixedRate(this::flush, runImmediately, stateFlushPeriodInSeconds, TimeUnit.SECONDS);
      }
    }
  }

  private boolean isMigration(final ConnectionState currentPersistedState, final AirbyteStateMessage stateMessage) {
    return (!isStateEmpty(currentPersistedState) && currentPersistedState.getStateType() == ConnectionStateType.LEGACY)
        && stateMessage.getType() != AirbyteStateType.LEGACY;
  }

  /**
   * Stop background data flush thread and attempt to flush pending data
   * <p>
   * If there is already flush in progress, wait for it to terminate. If it didn't terminate during
   * the allocated time, we exit rather than attempting a concurrent write that could lead to
   * non-deterministic behavior.
   * <p>
   * For the final flush, we will retry in case of failures since there is no more "scheduled" attempt
   * after this.
   */
  @Override
  public void close() {
    // stop the buffered refresh
    stateFlushExecutorService.shutdown();

    // Wait for previous running task to terminate
    try {
      final boolean terminated = stateFlushExecutorService.awaitTermination(flushTerminationTimeoutInSeconds, TimeUnit.SECONDS);
      if (!terminated) {
        if (stateToFlush != null && !stateToFlush.isEmpty()) {
          MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_NOT_ATTEMPTED, 1);
        }

        // Ongoing flush failed to terminate within the allocated time
        log.info("Pending persist operation took too long to complete, most recent states may have been lost");

        // This is the hard case, if the backend persisted the data, we may write duplicate
        // We exit to avoid non-deterministic write attempts
        return;
      }
    } catch (final InterruptedException e) {
      if (stateToFlush != null && !stateToFlush.isEmpty()) {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_NOT_ATTEMPTED, 1);
      }

      // The current thread is getting interrupted
      log.info("SyncPersistence has been interrupted while terminating, most recent states may have been lost", e);

      // This is also a hard case, if the backend persisted the data, we may write duplicate
      // We exit to avoid non-deterministic write attempts
      return;
    }

    if (hasStatesToFlush()) {
      // we still have data to flush
      prepareDataForFlush();

      if (onlyFlushAtTheEnd) {
        validateStreamMigration();
      }

      try {
        AirbyteApiClient.retryWithJitter(() -> {
          doFlushState();
          return null;
        }, "Flush States from SyncPersistenceImpl");
      } catch (final Exception e) {
        if (stateToFlush != null && !stateToFlush.isEmpty()) {
          MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_NOT_ATTEMPTED, 1);
        }
        throw e;
      }
    }

    // On close, this check is independent of hasDataToFlush. We could be in a state where state flush
    // was successful but stats flush failed, so we should check for stats to flush regardless of the
    // states.
    if (hasStatsToFlush()) {
      try {
        AirbyteApiClient.retryWithJitter(() -> {
          doFlushStats();
          return null;
        }, "Flush Stats from SyncPersistenceImpl");
      } catch (final Exception e) {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATS_COMMIT_NOT_ATTEMPTED, 1);
        throw e;
      }
    }
  }

  private boolean hasStatesToFlush() {
    return !stateBuffer.isEmpty() || stateToFlush != null;
  }

  private boolean hasStatsToFlush() {
    return isReceivingStats && statsToPersist != null;
  }

  /**
   * Flush state and stats for the ScheduledExecutorService
   * <p>
   * This method is swallowing exceptions on purpose. We do not want to fail or retry in a regular
   * run, the retry is deferred to the next run which will merge the data from the previous failed
   * attempt and the recent buffered data.
   */
  private void flush() {
    prepareDataForFlush();

    try {
      doFlushState();

      try {
        // We only flush stats if there was no state flush errors.
        // Even if there are no states to flush, we should still try to flush stats in case previous stats
        // flush failed
        doFlushStats();
      } catch (final Exception e) {
        log.warn("Failed to persist stats for connectionId {}, it will be retried as part of the next flush", connectionId, e);
      }
    } catch (final Exception e) {
      log.warn("Failed to persist state for connectionId {}, it will be retried as part of the next flush", connectionId, e);
    }
  }

  private void prepareDataForFlush() {
    final StateAggregator stateBufferToFlush = stateBuffer;
    stateBuffer = stateAggregatorFactory.create();

    if (stateToFlush == null) {
      // Happy path, previous flush was successful
      stateToFlush = stateBufferToFlush;
    } else {
      // Merging states from the previous attempt with the incoming buffer to flush
      stateToFlush.ingest(stateBufferToFlush);
    }

    // We prepare stats to commit. We generate the payload here to keep track as close as possible to
    // the states that are going to be persisted.
    // We also only want to generate the stats payload when roll-over state buffers. This is to avoid
    // updating the committed data counters ahead of the states because this counter is currently
    // decoupled from the state persistence.
    // This design favoring accuracy of committed data counters over freshness of emitted data counters.
    if (isReceivingStats && !stateToFlush.isEmpty()) {
      statsToPersist = buildSaveStatsRequest(syncStatsTracker, jobId, attemptNumber);
    }
  }

  private void doFlushState() throws ApiException {
    if (stateToFlush.isEmpty()) {
      return;
    }

    final State state = stateToFlush.getAggregated();
    final Optional<StateWrapper> maybeStateWrapper = StateMessageHelper.getTypedState(state.getState(), true);

    if (maybeStateWrapper.isEmpty()) {
      return;
    }

    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT, 1);

    final ConnectionStateCreateOrUpdate stateApiRequest = new ConnectionStateCreateOrUpdate()
        .connectionId(connectionId)
        .connectionState(StateConverter.toClient(connectionId, maybeStateWrapper.get()));

    try {
      stateApi.createOrUpdateState(stateApiRequest);
    } catch (final Exception e) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT_FAILED, 1);
      throw e;
    }

    // Only reset stateToFlush if the API call was successful
    stateToFlush = null;
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATE_COMMIT_ATTEMPT_SUCCESSFUL, 1);
  }

  private void validateStreamMigration() {
    final State state = stateToFlush.getAggregated();
    final Optional<StateWrapper> maybeStateWrapper = StateMessageHelper.getTypedState(state.getState(), true);

    if (maybeStateWrapper.isPresent() && maybeStateWrapper.get().getStateType() == StateType.STREAM) {
      Preconditions.checkNotNull(configuredAirbyteCatalog);
      validateStreamStates(maybeStateWrapper.get(), configuredAirbyteCatalog);
    }
  }

  //
  // NOTE:
  // The following methods are public because currently shared with PersistStateActivityImpl
  // Once PersistStateActivityImpl has been deleted, they should become private

  /**
   * Test whether the connection state is empty.
   *
   * @param connectionState The connection state.
   * @return {@code true} if the connection state is null or empty, {@code false} otherwise.
   */
  public static boolean isStateEmpty(final ConnectionState connectionState) {
    return connectionState == null || connectionState.getState() == null || connectionState.getState().isEmpty();
  }

  /**
   * Validate that the LEGACY -> STREAM migration is correct
   * <p>
   * During the migration, we will lose any previous stream state that isn't in the new state. To
   * avoid a potential loss of state, we ensure that all the incremental streams are present in the
   * new state.
   *
   * @param state the new state we want to persist
   * @param configuredCatalog the configured catalog of the connection of state
   */
  @VisibleForTesting
  public static void validateStreamStates(final StateWrapper state, final ConfiguredAirbyteCatalog configuredCatalog) {
    final List<StreamDescriptor> stateStreamDescriptors =
        state.getStateMessages().stream().map(stateMessage -> stateMessage.getStream().getStreamDescriptor()).toList();
    final List<StreamDescriptor> catalogStreamDescriptors = CatalogHelpers.extractIncrementalStreamDescriptors(configuredCatalog);
    catalogStreamDescriptors.forEach(streamDescriptor -> {
      if (!stateStreamDescriptors.contains(streamDescriptor)) {
        throw new IllegalStateException(
            "Job ran during migration from Legacy State to Per Stream State. One of the streams that did not have state is: (namespace:"
                + (streamDescriptor.getNamespace() != null ? streamDescriptor.getNamespace() : "") + ", name:" + streamDescriptor.getName()
                + "). Job must be retried in order to properly store state.");
      }
    });
  }

  @Override
  public void updateStats(AirbyteRecordMessage recordMessage) {
    // Stats persistence is dependent on State persistence, so we defer the start of the background task
    // to the state flow.
    isReceivingStats = true;
    syncStatsTracker.updateStats(recordMessage);
  }

  @Override
  public void updateEstimates(AirbyteEstimateTraceMessage estimate) {
    // Stats persistence is dependent on State persistence, so we defer the start of the background task
    // to the state flow.
    isReceivingStats = true;
    syncStatsTracker.updateEstimates(estimate);
  }

  @Override
  public void updateSourceStatesStats(AirbyteStateMessage stateMessage) {
    // Stats persistence is dependent on State persistence, so we defer the start of the background task
    // to the state flow.
    isReceivingStats = true;
    syncStatsTracker.updateSourceStatesStats(stateMessage);
  }

  @Override
  public void updateDestinationStateStats(AirbyteStateMessage stateMessage) {
    // Stats persistence is dependent on State persistence, so we defer the start of the background task
    // to the state flow.
    isReceivingStats = true;
    syncStatsTracker.updateDestinationStateStats(stateMessage);
  }

  private void doFlushStats() throws ApiException {
    if (!hasStatsToFlush()) {
      return;
    }

    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT, 1);

    try {
      attemptApi.saveStats(statsToPersist);
    } catch (final Exception e) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT_FAILED, 1);
      throw e;
    }

    statsToPersist = null;
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.STATS_COMMIT_ATTEMPT_SUCCESSFUL, 1);
  }

  private static SaveStatsRequestBody buildSaveStatsRequest(final SyncStatsTracker syncStatsTracker, final Long jobId, final Integer attemptNumber) {
    final SyncStats totalSyncStats = SyncStatsBuilder.getTotalStats(syncStatsTracker, false);
    final List<StreamSyncStats> streamSyncStats = SyncStatsBuilder.getPerStreamStats(syncStatsTracker, false);
    return new SaveStatsRequestBody()
        .jobId(jobId)
        .attemptNumber(attemptNumber)
        .stats(convertSyncStatsToAttemptStats(totalSyncStats))
        .streamStats(
            streamSyncStats.stream().map(
                s -> new AttemptStreamStats()
                    .streamName(s.getStreamName())
                    .streamNamespace(s.getStreamNamespace())
                    .stats(convertSyncStatsToAttemptStats(s.getStats())))
                .toList());
  }

  private static AttemptStats convertSyncStatsToAttemptStats(final SyncStats syncStats) {
    return new AttemptStats()
        .bytesEmitted(syncStats.getBytesEmitted())
        .recordsEmitted(syncStats.getRecordsEmitted())
        .estimatedBytes(syncStats.getEstimatedBytes())
        .estimatedRecords(syncStats.getEstimatedRecords())
        // TODO add support for bytesCommitted
        .recordsCommitted(syncStats.getRecordsCommitted());
  }

  // The methods below are from the wrapping of SyncStatsTracker interface. The interface should be
  // rewritten to return the SyncStats objects
  // directly rather explicitly exposing each field.

  @Override
  public Optional<Map<AirbyteStreamNameNamespacePair, Long>> getStreamToCommittedRecords() {
    return syncStatsTracker.getStreamToCommittedRecords();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedRecords() {
    return syncStatsTracker.getStreamToEmittedRecords();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedRecords() {
    return syncStatsTracker.getStreamToEstimatedRecords();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEmittedBytes() {
    return syncStatsTracker.getStreamToEmittedBytes();
  }

  @Override
  public Map<AirbyteStreamNameNamespacePair, Long> getStreamToEstimatedBytes() {
    return syncStatsTracker.getStreamToEstimatedBytes();
  }

  @Override
  public long getTotalRecordsEmitted() {
    return syncStatsTracker.getTotalRecordsEmitted();
  }

  @Override
  public long getTotalRecordsEstimated() {
    return syncStatsTracker.getTotalRecordsEstimated();
  }

  @Override
  public long getTotalBytesEmitted() {
    return syncStatsTracker.getTotalBytesEmitted();
  }

  @Override
  public long getTotalBytesEstimated() {
    return syncStatsTracker.getTotalBytesEstimated();
  }

  @Override
  public Optional<Long> getTotalRecordsCommitted() {
    return syncStatsTracker.getTotalRecordsCommitted();
  }

  @Override
  public Long getTotalSourceStateMessagesEmitted() {
    return syncStatsTracker.getTotalSourceStateMessagesEmitted();
  }

  @Override
  public Long getTotalDestinationStateMessagesEmitted() {
    return syncStatsTracker.getTotalDestinationStateMessagesEmitted();
  }

  @Override
  public Long getMaxSecondsToReceiveSourceStateMessage() {
    return syncStatsTracker.getMaxSecondsToReceiveSourceStateMessage();
  }

  @Override
  public Long getMeanSecondsToReceiveSourceStateMessage() {
    return syncStatsTracker.getMeanSecondsToReceiveSourceStateMessage();
  }

  @Override
  public Optional<Long> getMaxSecondsBetweenStateMessageEmittedAndCommitted() {
    return syncStatsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted();
  }

  @Override
  public Optional<Long> getMeanSecondsBetweenStateMessageEmittedAndCommitted() {
    return syncStatsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted();
  }

  @Override
  public Boolean getUnreliableStateTimingMetrics() {
    return syncStatsTracker.getUnreliableStateTimingMetrics();
  }

}
