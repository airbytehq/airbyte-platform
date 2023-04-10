/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.SourceDefinitionApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.features.FeatureFlagHelper;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.general.DefaultReplicationWorker;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.book_keeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.book_keeping.MessageTracker;
import io.airbyte.workers.internal.sync_persistence.SyncPersistence;
import io.airbyte.workers.internal.sync_persistence.SyncPersistenceFactory;
import java.time.Duration;
import java.util.UUID;

/**
 * Helper Functions to build the dependencies of the Orchestrator.
 * <p>
 * The primary goal of this class is to reduce code duplication while we refactor the init and
 * introduce dependency injection in this part of the codebase.
 */
public class OrchestratorFactoryHelpers {

  /**
   * Create ConnectorConfigUpdater.
   */
  public static ConnectorConfigUpdater createConnectorConfigUpdater(final SourceApi sourceApi, final DestinationApi destinationApi) {
    return new ConnectorConfigUpdater(sourceApi, destinationApi);
  }

  /**
   * Create HeartbeatMonitor.
   */
  public static HeartbeatMonitor createHeartbeatMonitor(final SourceApi sourceApi,
                                                        final SourceDefinitionApi sourceDefinitionApi,
                                                        final StandardSyncInput syncInput)
      throws ApiException {
    final UUID sourceDefinitionId = sourceApi.getSource(new SourceIdRequestBody().sourceId(syncInput.getSourceId())).getSourceDefinitionId();

    final long maxSecondsBetweenMessages = AirbyteApiClient.retryWithJitter(() -> sourceDefinitionApi
        .getSourceDefinition(new SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId)), "get the source definition")
        .getMaxSecondsBetweenMessages();
    // reset jobs use an empty source to induce resetting all data in destination.
    return new HeartbeatMonitor(Duration.ofSeconds(maxSecondsBetweenMessages));
  }

  /**
   * Get HeartbeatTimeoutChaperone.
   */
  public static HeartbeatTimeoutChaperone createHeartbeatTimeoutChaperone(final HeartbeatMonitor heartbeatMonitor,
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
  public static MessageTracker createMessageTracker(final SyncPersistence syncPersistence,
                                                    final FeatureFlags featureFlags,
                                                    final StandardSyncInput syncInput) {
    final boolean commitStatsAsap = DefaultReplicationWorker.shouldCommitStatsAsap(syncInput);
    final MessageTracker messageTracker =
        commitStatsAsap ? new AirbyteMessageTracker(syncPersistence, featureFlags) : new AirbyteMessageTracker(featureFlags);
    return messageTracker;
  }

  /**
   * Create ReplicationWorker.
   */
  public static ReplicationWorker createReplicationWorker(final AirbyteSource source,
                                                          final AirbyteDestination destination,
                                                          final MessageTracker messageTracker,
                                                          final SyncPersistence syncPersistence,
                                                          final WorkerMetricReporter metricReporter,
                                                          final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone,
                                                          final ConnectorConfigUpdater connectorConfigUpdater,
                                                          final FeatureFlagClient featureFlagClient,
                                                          final FeatureFlags featureFlags,
                                                          final JobRunConfig jobRunConfig,
                                                          final StandardSyncInput syncInput) {
    // NOTE: we apply field selection if the feature flag client says so (recommended) or the old
    // environment-variable flags say so (deprecated).
    // The latter FeatureFlagHelper will be removed once the flag client is fully deployed.
    final UUID workspaceId = syncInput.getWorkspaceId();
    final boolean fieldSelectionEnabled = workspaceId != null
        && (featureFlagClient.enabled(FieldSelectionEnabled.INSTANCE, new Workspace(workspaceId))
            || FeatureFlagHelper.isFieldSelectionEnabledForWorkspace(featureFlags, workspaceId));

    final var replicationWorker = new DefaultReplicationWorker(
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        source,
        new NamespacingMapper(syncInput.getNamespaceDefinition(), syncInput.getNamespaceFormat(), syncInput.getPrefix()),
        destination,
        messageTracker,
        syncPersistence,
        new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(syncInput)),
        metricReporter,
        connectorConfigUpdater,
        fieldSelectionEnabled,
        heartbeatTimeoutChaperone);
    return replicationWorker;
  }

  /**
   * Create SyncPersistence.
   */
  public static SyncPersistence createSyncPersistence(final SyncPersistenceFactory syncPersistenceFactory,
                                                      final StandardSyncInput syncInput,
                                                      final IntegrationLauncherConfig sourceLauncherConfig) {
    // TODO clean up the feature flag init once commitStates and commitStats have been rolled out
    final boolean commitStatesAsap = DefaultReplicationWorker.shouldCommitStateAsap(syncInput);
    final SyncPersistence syncPersistence = commitStatesAsap
        ? syncPersistenceFactory.get(syncInput.getConnectionId(), Long.parseLong(sourceLauncherConfig.getJobId()),
            sourceLauncherConfig.getAttemptId().intValue(), syncInput.getCatalog())
        : null;
    return syncPersistence;
  }

}
