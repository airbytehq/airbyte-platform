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
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.featureflag.ConcurrentSourceStreamRead;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationTimeoutSeconds;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.RemoveValidationLimit;
import io.airbyte.featureflag.ReplicationWorkerImpl;
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.SourceType;
import io.airbyte.featureflag.TrackCommittedStatsWhenUsingGlobalState;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.EmptyAirbyteSource;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.internal.syncpersistence.SyncPersistenceFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncherFactory;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.micronaut.context.annotation.Value;
import io.micronaut.core.util.CollectionUtils;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private final SourceApi sourceApi;
  private final SourceDefinitionApi sourceDefinitionApi;
  private final DestinationApi destinationApi;
  private final SyncPersistenceFactory syncPersistenceFactory;
  private final AirbyteMessageDataExtractor airbyteMessageDataExtractor;
  private final FeatureFlagClient featureFlagClient;
  private final FeatureFlags featureFlags;
  private final MetricClient metricClient;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  private final AnalyticsMessageTracker analyticsMessageTracker;
  private final WorkloadApi workloadApi;

  private final WorkloadIdGenerator workloadIdGenerator;
  private final boolean workloadEnabled;

  public ReplicationWorkerFactory(
                                  final AirbyteIntegrationLauncherFactory airbyteIntegrationLauncherFactory,
                                  final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                  final SourceApi sourceApi,
                                  final SourceDefinitionApi sourceDefinitionApi,
                                  final DestinationApi destinationApi,
                                  final SyncPersistenceFactory syncPersistenceFactory,
                                  final FeatureFlagClient featureFlagClient,
                                  final FeatureFlags featureFlags,
                                  final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper,
                                  final MetricClient metricClient,
                                  final WorkloadApi workloadApi,
                                  final WorkloadIdGenerator workloadIdGenerator,
                                  final AnalyticsMessageTracker analyticsMessageTracker,
                                  @Value("${airbyte.workload.enabled}") final boolean workloadEnabled) {
    this.airbyteIntegrationLauncherFactory = airbyteIntegrationLauncherFactory;
    this.sourceApi = sourceApi;
    this.sourceDefinitionApi = sourceDefinitionApi;
    this.destinationApi = destinationApi;
    this.syncPersistenceFactory = syncPersistenceFactory;
    this.airbyteMessageDataExtractor = airbyteMessageDataExtractor;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;

    this.featureFlagClient = featureFlagClient;
    this.featureFlags = featureFlags;
    this.metricClient = metricClient;
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.workloadEnabled = workloadEnabled;
    this.analyticsMessageTracker = analyticsMessageTracker;
  }

  /**
   * Create a ReplicationWorker.
   */
  public ReplicationWorker create(final ReplicationInput replicationInput,
                                  final JobRunConfig jobRunConfig,
                                  final IntegrationLauncherConfig sourceLauncherConfig,
                                  final IntegrationLauncherConfig destinationLauncherConfig,
                                  final VoidCallable onReplicationRunning)
      throws ApiException {
    final UUID sourceDefinitionId = AirbyteApiClient.retryWithJitter(
        () -> sourceApi.getSource(
            new SourceIdRequestBody().sourceId(replicationInput.getSourceId())).getSourceDefinitionId(),
        "get the source definition for feature flag checks");
    final HeartbeatMonitor heartbeatMonitor = createHeartbeatMonitor(sourceDefinitionId, sourceDefinitionApi);
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = createHeartbeatTimeoutChaperone(heartbeatMonitor,
        featureFlagClient, replicationInput, metricClient);
    final DestinationTimeoutMonitor destinationTimeout = createDestinationTimeout(featureFlagClient, replicationInput, metricClient);
    final RecordSchemaValidator recordSchemaValidator = createRecordSchemaValidator(replicationInput);

    // Enable concurrent stream reads for testing purposes
    maybeEnableConcurrentStreamReads(sourceLauncherConfig, replicationInput);

    log.info("Setting up source...");
    // reset jobs use an empty source to induce resetting all data in destination.
    final var airbyteSource = replicationInput.getIsReset()
        ? new EmptyAirbyteSource()
        : airbyteIntegrationLauncherFactory.createAirbyteSource(sourceLauncherConfig,
            replicationInput.getSyncResourceRequirements(), replicationInput.getCatalog(), heartbeatMonitor);

    log.info("Setting up destination...");
    final var airbyteDestination = airbyteIntegrationLauncherFactory.createAirbyteDestination(destinationLauncherConfig,
        replicationInput.getSyncResourceRequirements(), replicationInput.getCatalog(), destinationTimeout);

    final WorkerMetricReporter metricReporter = new WorkerMetricReporter(metricClient, sourceLauncherConfig.getDockerImage());

    final FieldSelector fieldSelector =
        createFieldSelector(recordSchemaValidator, metricReporter, featureFlagClient, replicationInput.getWorkspaceId(), sourceDefinitionId);

    log.info("Setting up replication worker...");
    final SyncPersistence syncPersistence = createSyncPersistence(syncPersistenceFactory, replicationInput, sourceLauncherConfig);
    final AirbyteMessageTracker messageTracker = createMessageTracker(featureFlagClient, syncPersistence, featureFlags, replicationInput);

    return createReplicationWorker(airbyteSource, airbyteDestination, messageTracker,
        syncPersistence, recordSchemaValidator, fieldSelector, heartbeatTimeoutChaperone,
        featureFlagClient, jobRunConfig, replicationInput, airbyteMessageDataExtractor, replicationAirbyteMessageEventPublishingHelper,
        onReplicationRunning, metricClient, destinationTimeout, workloadApi, workloadIdGenerator, workloadEnabled, analyticsMessageTracker);
  }

  /**
   * Enables concurrent stream reads for the current sync if the correct configuration is present. If
   * present, a environment variable ({@code CONCURRENT_SOURCE_STREAM_READ}) is added to the map of
   * environment variables passed to the source.
   *
   * @param sourceLauncherConfig The {@link IntegrationLauncherConfig} for the source.
   * @param replicationInput The input for the current sync.
   */
  private void maybeEnableConcurrentStreamReads(final IntegrationLauncherConfig sourceLauncherConfig, final ReplicationInput replicationInput) {
    final Boolean isEnabled = shouldEnableConcurrentSourceRead(sourceLauncherConfig, replicationInput.getConnectionId());
    final Map<String, String> concurrentReadEnvVars = Map.of("CONCURRENT_SOURCE_STREAM_READ", isEnabled.toString());
    log.info("Concurrent stream read enabled? {}", isEnabled);
    if (CollectionUtils.isNotEmpty(sourceLauncherConfig.getAdditionalEnvironmentVariables())) {
      sourceLauncherConfig.getAdditionalEnvironmentVariables().putAll(concurrentReadEnvVars);
    } else {
      sourceLauncherConfig.setAdditionalEnvironmentVariables(concurrentReadEnvVars);
    }
  }

  /**
   * Tests whether the concurrent source reads are enabled by interpreting a feature flag for the
   * feature, connection ID associated with the current sync and the associated source Docker image.
   *
   * @param sourceLauncherConfig The {@link IntegrationLauncherConfig} for the source.
   * @param connectionId The id of the connection being synced.
   * @return {@code true} if concurrent source reads should be enabled or {@code false} otherwise.
   */
  private Boolean shouldEnableConcurrentSourceRead(final IntegrationLauncherConfig sourceLauncherConfig, final UUID connectionId) {
    if (sourceLauncherConfig.getDockerImage().startsWith("airbyte/source-mysql")) {
      return featureFlagClient.boolVariation(ConcurrentSourceStreamRead.INSTANCE, new Connection(connectionId));
    } else {
      return false;
    }
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
                                                                           final ReplicationInput replicationInput,
                                                                           final MetricClient metricClient) {
    return new HeartbeatTimeoutChaperone(heartbeatMonitor,
        HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
        featureFlagClient,
        replicationInput.getWorkspaceId(),
        replicationInput.getConnectionId(),
        metricClient);
  }

  private static DestinationTimeoutMonitor createDestinationTimeout(final FeatureFlagClient featureFlagClient,
                                                                    final ReplicationInput replicationInput,
                                                                    final MetricClient metricClient) {
    final Context context = new Multi(List.of(new Workspace(replicationInput.getWorkspaceId()), new Connection(replicationInput.getConnectionId())));
    final boolean throwExceptionOnDestinationTimeout = featureFlagClient.boolVariation(ShouldFailSyncOnDestinationTimeout.INSTANCE, context);
    final int destinationTimeoutSeconds = featureFlagClient.intVariation(DestinationTimeoutSeconds.INSTANCE, context);

    return new DestinationTimeoutMonitor(
        replicationInput.getWorkspaceId(),
        replicationInput.getConnectionId(),
        metricClient,
        Duration.ofSeconds(destinationTimeoutSeconds),
        throwExceptionOnDestinationTimeout);
  }

  /**
   * Create MessageTracker.
   */
  private static AirbyteMessageTracker createMessageTracker(final FeatureFlagClient featureFlagClient,
                                                            final SyncPersistence syncPersistence,
                                                            final FeatureFlags featureFlags,
                                                            final ReplicationInput replicationInput) {
    Context context = new Multi(List.of(new Workspace(replicationInput.getWorkspaceId()), new Connection(replicationInput.getConnectionId())));
    boolean trackCommittedStatsWhenUsingGlobalState = featureFlagClient.boolVariation(TrackCommittedStatsWhenUsingGlobalState.INSTANCE, context);
    return new AirbyteMessageTracker(syncPersistence, featureFlags, replicationInput.getSourceLauncherConfig().getDockerImage(),
        replicationInput.getDestinationLauncherConfig().getDockerImage(), trackCommittedStatsWhenUsingGlobalState);
  }

  /**
   * Create RecordSchemaValidator.
   */
  private static RecordSchemaValidator createRecordSchemaValidator(final ReplicationInput replicationInput) {
    return new RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.getCatalog()));
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
                                                           final AirbyteMessageTracker messageTracker,
                                                           final SyncPersistence syncPersistence,
                                                           final RecordSchemaValidator recordSchemaValidator,
                                                           final FieldSelector fieldSelector,
                                                           final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone,
                                                           final FeatureFlagClient featureFlagClient,
                                                           final JobRunConfig jobRunConfig,
                                                           final ReplicationInput replicationInput,
                                                           final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                           final ReplicationAirbyteMessageEventPublishingHelper replicationEventPublishingHelper,
                                                           final VoidCallable onReplicationRunning,
                                                           final MetricClient metricClient,
                                                           final DestinationTimeoutMonitor destinationTimeout,
                                                           final WorkloadApi workloadApi,
                                                           final WorkloadIdGenerator workloadIdGenerator,
                                                           final boolean workloadEnabled,
                                                           final AnalyticsMessageTracker analyticsMessageTracker) {
    final Context flagContext = getFeatureFlagContext(replicationInput);
    final String workerImpl = featureFlagClient.stringVariation(ReplicationWorkerImpl.INSTANCE, flagContext);
    return buildReplicationWorkerInstance(
        workerImpl,
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        source,
        new NamespacingMapper(
            replicationInput.getNamespaceDefinition(),
            replicationInput.getNamespaceFormat(),
            replicationInput.getPrefix()),
        destination,
        messageTracker,
        syncPersistence,
        recordSchemaValidator,
        fieldSelector,
        heartbeatTimeoutChaperone,
        new ReplicationFeatureFlagReader(featureFlagClient, flagContext),
        airbyteMessageDataExtractor,
        replicationEventPublishingHelper,
        onReplicationRunning,
        metricClient,
        destinationTimeout,
        workloadApi,
        workloadIdGenerator,
        workloadEnabled, analyticsMessageTracker);
  }

  private static Context getFeatureFlagContext(final ReplicationInput replicationInput) {
    final List<Context> contexts = new ArrayList<>();
    if (replicationInput.getWorkspaceId() != null) {
      contexts.add(new Workspace(replicationInput.getWorkspaceId()));
    }
    if (replicationInput.getConnectionId() != null) {
      contexts.add(new Connection(replicationInput.getConnectionId()));
    }
    if (replicationInput.getSourceId() != null) {
      contexts.add(new Source(replicationInput.getSourceId()));
    }
    if (replicationInput.getDestinationId() != null) {
      contexts.add(new Destination(replicationInput.getDestinationId()));
    }
    if (replicationInput.getSyncResourceRequirements() != null
        && replicationInput.getSyncResourceRequirements().getConfigKey() != null
        && replicationInput.getSyncResourceRequirements().getConfigKey().getSubType() != null) {
      contexts.add(new SourceType(replicationInput.getSyncResourceRequirements().getConfigKey().getSubType()));
    }
    return new Multi(contexts);
  }

  private static ReplicationWorker buildReplicationWorkerInstance(final String workerImpl,
                                                                  final String jobId,
                                                                  final int attempt,
                                                                  final AirbyteSource source,
                                                                  final AirbyteMapper mapper,
                                                                  final AirbyteDestination destination,
                                                                  final AirbyteMessageTracker messageTracker,
                                                                  final SyncPersistence syncPersistence,
                                                                  final RecordSchemaValidator recordSchemaValidator,
                                                                  final FieldSelector fieldSelector,
                                                                  final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                                                  final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                                                  final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                                  final ReplicationAirbyteMessageEventPublishingHelper messageEventPublishingHelper,
                                                                  final VoidCallable onReplicationRunning,
                                                                  final MetricClient metricClient,
                                                                  final DestinationTimeoutMonitor destinationTimeout,
                                                                  final WorkloadApi workloadApi,
                                                                  final WorkloadIdGenerator workloadIdGenerator,
                                                                  final boolean workloadEnabled,
                                                                  final AnalyticsMessageTracker analyticsMessageTracker) {
    final ReplicationWorkerHelper replicationWorkerHelper =
        new ReplicationWorkerHelper(airbyteMessageDataExtractor, fieldSelector, mapper, messageTracker, syncPersistence,
            messageEventPublishingHelper, new ThreadedTimeTracker(), onReplicationRunning, workloadApi, workloadIdGenerator,
            workloadEnabled, analyticsMessageTracker);
    if ("buffered".equals(workerImpl)) {
      metricClient.count(OssMetricsRegistry.REPLICATION_WORKER_CREATED, 1, new MetricAttribute(MetricTags.IMPLEMENTATION, workerImpl));
      return new BufferedReplicationWorker(jobId, attempt, source, destination, syncPersistence, recordSchemaValidator,
          srcHeartbeatTimeoutChaperone, replicationFeatureFlagReader, replicationWorkerHelper, destinationTimeout);
    } else {
      metricClient.count(OssMetricsRegistry.REPLICATION_WORKER_CREATED, 1, new MetricAttribute(MetricTags.IMPLEMENTATION, "default"));
      return new DefaultReplicationWorker(jobId, attempt, source, destination, syncPersistence, recordSchemaValidator,
          srcHeartbeatTimeoutChaperone, replicationFeatureFlagReader, replicationWorkerHelper, destinationTimeout);
    }
  }

  /**
   * Create SyncPersistence.
   */
  private static SyncPersistence createSyncPersistence(final SyncPersistenceFactory syncPersistenceFactory,
                                                       final ReplicationInput replicationInput,
                                                       final IntegrationLauncherConfig sourceLauncherConfig) {
    return syncPersistenceFactory.get(replicationInput.getConnectionId(), Long.parseLong(sourceLauncherConfig.getJobId()),
        sourceLauncherConfig.getAttemptId().intValue(), replicationInput.getCatalog());
  }

}
