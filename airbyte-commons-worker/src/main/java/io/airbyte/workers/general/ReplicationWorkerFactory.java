/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.SourceDefinitionApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RemoveValidationLimit;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.EmptyAirbyteSource;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.book_keeping.MessageTracker;
import io.airbyte.workers.internal.book_keeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import io.airbyte.workers.internal.sync_persistence.SyncPersistenceFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncherFactory;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for the DefaultReplicationWorker.
 * <p>
 * This factory is bridging the gap between what services can be directly injected and the ones that
 * currently depend on input data in their constructor. This factory would disappear if all the
 * dependencies of the DefaultReplicationWorker were stateless.
 */
@Singleton
@Slf4j
public class ReplicationWorkerFactory {

  private final AirbyteIntegrationLauncherFactory airbyteIntegrationLauncherFactory;
  private final ConnectorConfigUpdater connectorConfigUpdater;
  private final SourceApi sourceApi;
  private final SourceDefinitionApi sourceDefinitionApi;
  private final DestinationApi destinationApi;
  private final SyncPersistenceFactory syncPersistenceFactory;
  private final AirbyteMessageDataExtractor airbyteMessageDataExtractor;
  private final FeatureFlagClient featureFlagClient;
  private final FeatureFlags featureFlags;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;

  public ReplicationWorkerFactory(
                                  final AirbyteIntegrationLauncherFactory airbyteIntegrationLauncherFactory,
                                  final ConnectorConfigUpdater connectorConfigUpdater,
                                  final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                  final SourceApi sourceApi,
                                  final SourceDefinitionApi sourceDefinitionApi,
                                  final DestinationApi destinationApi,
                                  final SyncPersistenceFactory syncPersistenceFactory,
                                  final FeatureFlagClient featureFlagClient,
                                  final FeatureFlags featureFlags,
                                  final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper) {
    this.airbyteIntegrationLauncherFactory = airbyteIntegrationLauncherFactory;
    this.connectorConfigUpdater = connectorConfigUpdater;
    this.sourceApi = sourceApi;
    this.sourceDefinitionApi = sourceDefinitionApi;
    this.destinationApi = destinationApi;
    this.syncPersistenceFactory = syncPersistenceFactory;
    this.airbyteMessageDataExtractor = airbyteMessageDataExtractor;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;

    this.featureFlagClient = featureFlagClient;
    this.featureFlags = featureFlags;
  }

  /**
   * Create a ReplicationWorker.
   */
  public ReplicationWorker create(final StandardSyncInput syncInput,
                                  final JobRunConfig jobRunConfig,
                                  final IntegrationLauncherConfig sourceLauncherConfig,
                                  final IntegrationLauncherConfig destinationLauncherConfig)
      throws ApiException {
    final UUID sourceDefinitionId = AirbyteApiClient.retryWithJitter(
        () -> sourceApi.getSource(
            new SourceIdRequestBody().sourceId(syncInput.getSourceId())).getSourceDefinitionId(),
        "get the source definition for feature flag checks");
    final HeartbeatMonitor heartbeatMonitor = createHeartbeatMonitor(sourceDefinitionId, sourceDefinitionApi);
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = createHeartbeatTimeoutChaperone(heartbeatMonitor,
        featureFlagClient, syncInput);
    final RecordSchemaValidator recordSchemaValidator = createRecordSchemaValidator(syncInput);

    log.info("Setting up source...");
    // reset jobs use an empty source to induce resetting all data in destination.
    final var airbyteSource = syncInput.getIsReset()
        ? new EmptyAirbyteSource(featureFlags.useStreamCapableState())
        : airbyteIntegrationLauncherFactory.createAirbyteSource(sourceLauncherConfig, syncInput.getSourceResourceRequirements(),
            syncInput.getCatalog(), heartbeatMonitor);

    log.info("Setting up destination...");
    final var airbyteDestination = airbyteIntegrationLauncherFactory.createAirbyteDestination(destinationLauncherConfig,
        syncInput.getDestinationResourceRequirements(), syncInput.getCatalog());

    // TODO MetricClient should be injectable
    MetricClientFactory.initialize(MetricEmittingApps.WORKER);
    final MetricClient metricClient = MetricClientFactory.getMetricClient();
    final WorkerMetricReporter metricReporter = new WorkerMetricReporter(metricClient, sourceLauncherConfig.getDockerImage());

    final FieldSelector fieldSelector =
        createFieldSelector(recordSchemaValidator, metricReporter, featureFlagClient, syncInput.getWorkspaceId(), sourceDefinitionId);

    log.info("Setting up replication worker...");
    final SyncPersistence syncPersistence = createSyncPersistence(syncPersistenceFactory, syncInput, sourceLauncherConfig);
    final MessageTracker messageTracker = createMessageTracker(syncPersistence, featureFlags, syncInput);

    return createReplicationWorker(airbyteSource, airbyteDestination, messageTracker,
        syncPersistence, recordSchemaValidator, fieldSelector, heartbeatTimeoutChaperone, connectorConfigUpdater, featureFlagClient,
        jobRunConfig,
        syncInput, airbyteMessageDataExtractor, replicationAirbyteMessageEventPublishingHelper);
  }

  /**
   * Create HeartbeatMonitor.
   */
  private static HeartbeatMonitor createHeartbeatMonitor(final UUID sourceDefinitionId,
                                                         final SourceDefinitionApi sourceDefinitionApi) {
    final Long maxSecondsBetweenMessages = sourceDefinitionId != null ? AirbyteApiClient.retryWithJitter(() -> sourceDefinitionApi
        .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId)), "get the source definition")
        .getMaxSecondsBetweenMessages() : null;
    if (maxSecondsBetweenMessages != null) {
      // reset jobs use an empty source to induce resetting all data in destination.
      return new HeartbeatMonitor(Duration.ofSeconds(maxSecondsBetweenMessages));
    }
    log.warn("An error occurred while fetch the max seconds between messages for this source. We are using a default of 24 hours");
    return new HeartbeatMonitor(Duration.ofSeconds(TimeUnit.HOURS.toSeconds(24)));
  }

  /**
   * Get HeartbeatTimeoutChaperone.
   */
  private static HeartbeatTimeoutChaperone createHeartbeatTimeoutChaperone(final HeartbeatMonitor heartbeatMonitor,
                                                                           final FeatureFlagClient featureFlagClient,
                                                                           final StandardSyncInput syncInput) {
    return new HeartbeatTimeoutChaperone(heartbeatMonitor,
        HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
        featureFlagClient,
        syncInput.getWorkspaceId(),
        syncInput.getConnectionId(),
        MetricClientFactory.getMetricClient());
  }

  /**
   * Create MessageTracker.
   */
  private static MessageTracker createMessageTracker(final SyncPersistence syncPersistence,
                                                     final FeatureFlags featureFlags,
                                                     final StandardSyncInput syncInput) {
    final boolean commitStatsAsap = ReplicationFeatureFlagReader.shouldCommitStatsAsap(syncInput);
    final MessageTracker messageTracker =
        commitStatsAsap ? new AirbyteMessageTracker(syncPersistence, featureFlags) : new AirbyteMessageTracker(featureFlags);
    return messageTracker;
  }

  /**
   * Create RecordSchemaValidator.
   */
  private static RecordSchemaValidator createRecordSchemaValidator(final StandardSyncInput syncInput) {
    return new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(syncInput));
  }

  private static FieldSelector createFieldSelector(final RecordSchemaValidator recordSchemaValidator,
                                                   final WorkerMetricReporter metricReporter,
                                                   final FeatureFlagClient featureFlagClient,
                                                   final UUID workspaceId,
                                                   final UUID sourceDefinitionId) {
    final boolean fieldSelectionEnabled = workspaceId != null && featureFlagClient.boolVariation(FieldSelectionEnabled.INSTANCE, new Multi(
        List.of(new Workspace(workspaceId), new SourceDefinition(sourceDefinitionId))));
    final boolean removeValidationLimit =
        workspaceId != null && featureFlagClient.boolVariation(RemoveValidationLimit.INSTANCE, new Workspace(workspaceId));
    return new FieldSelector(recordSchemaValidator, metricReporter, fieldSelectionEnabled, removeValidationLimit);
  }

  /**
   * Create ReplicationWorker.
   */
  private static ReplicationWorker createReplicationWorker(final AirbyteSource source,
                                                           final AirbyteDestination destination,
                                                           final MessageTracker messageTracker,
                                                           final SyncPersistence syncPersistence,
                                                           final RecordSchemaValidator recordSchemaValidator,
                                                           final FieldSelector fieldSelector,
                                                           final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone,
                                                           final ConnectorConfigUpdater connectorConfigUpdater,
                                                           final FeatureFlagClient featureFlagClient,
                                                           final JobRunConfig jobRunConfig,
                                                           final StandardSyncInput syncInput,
                                                           final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                           final ReplicationAirbyteMessageEventPublishingHelper replicationEventPublishingHelper) {
    return new DefaultReplicationWorker(
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        source,
        new NamespacingMapper(syncInput.getNamespaceDefinition(), syncInput.getNamespaceFormat(), syncInput.getPrefix()),
        destination,
        messageTracker,
        syncPersistence,
        recordSchemaValidator,
        fieldSelector,
        connectorConfigUpdater,
        heartbeatTimeoutChaperone,
        new ReplicationFeatureFlagReader(featureFlagClient),
        airbyteMessageDataExtractor,
        replicationEventPublishingHelper);
  }

  /**
   * Create SyncPersistence.
   */
  private static SyncPersistence createSyncPersistence(final SyncPersistenceFactory syncPersistenceFactory,
                                                       final StandardSyncInput syncInput,
                                                       final IntegrationLauncherConfig sourceLauncherConfig) {
    // TODO clean up the feature flag init once commitStates and commitStats have been rolled out
    final boolean commitStatesAsap = ReplicationFeatureFlagReader.shouldCommitStateAsap(syncInput);
    final SyncPersistence syncPersistence = commitStatesAsap
        ? syncPersistenceFactory.get(syncInput.getConnectionId(), Long.parseLong(sourceLauncherConfig.getJobId()),
            sourceLauncherConfig.getAttemptId().intValue(), syncInput.getCatalog())
        : null;
    return syncPersistence;
  }

}
