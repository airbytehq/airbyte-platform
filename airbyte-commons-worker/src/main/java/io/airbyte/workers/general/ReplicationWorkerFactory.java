/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.SourceDefinitionApi;
import io.airbyte.api.client.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.client.model.generated.SourceIdRequestBody;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.commons.logging.LogSource;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.logging.MdcScope.Builder;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationTimeoutSeconds;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionEnabled;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.PrintLongRecordPks;
import io.airbyte.featureflag.RemoveValidationLimit;
import io.airbyte.featureflag.ReplicationBufferOverride;
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.SourceType;
import io.airbyte.featureflag.Workspace;
import io.airbyte.mappers.application.RecordMapper;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteMessageBufferedWriterFactory;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.ContainerIOHandle;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.EmptyAirbyteSource;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.LocalContainerAirbyteDestination;
import io.airbyte.workers.internal.LocalContainerAirbyteSource;
import io.airbyte.workers.internal.MessageMetricsTracker;
import io.airbyte.workers.internal.NamespacingMapper;
import io.airbyte.workers.internal.VersionedAirbyteMessageBufferedWriterFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.internal.syncpersistence.SyncPersistenceFactory;
import io.airbyte.workload.api.client.WorkloadApiClient;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for the BufferedReplicationWorker.
 * <p>
 * This factory is bridging the gap between what services can be directly injected and the ones that
 * currently depend on input data in their constructor. This factory would disappear if all the
 * dependencies of the DefaultReplicationWorker were stateless.
 */
@Singleton
public class ReplicationWorkerFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final GsonPksExtractor gsonPksExtractor;
  private final AirbyteApiClient airbyteApiClient;
  private final SyncPersistenceFactory syncPersistenceFactory;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;
  private final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper;
  private final TrackingClient trackingClient;
  private final WorkloadApiClient workloadApiClient;
  private final StreamStatusCompletionTracker streamStatusCompletionTracker;
  private final StreamStatusTrackerFactory streamStatusTrackerFactory;
  private final RecordMapper recordMapper;
  private final DestinationCatalogGenerator destinationCatalogGenerator;

  public static final MdcScope.Builder DESTINATION_LOG_MDC_BUILDER = new Builder()
      .setExtraMdcEntries(LogSource.DESTINATION.toMdc());

  public static final MdcScope.Builder SOURCE_LOG_MDC_BUILDER = new Builder()
      .setExtraMdcEntries(LogSource.SOURCE.toMdc());

  public ReplicationWorkerFactory(
                                  final AirbyteMessageSerDeProvider serDeProvider,
                                  final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                  final GsonPksExtractor gsonPksExtractor,
                                  final AirbyteApiClient airbyteApiClient,
                                  final SyncPersistenceFactory syncPersistenceFactory,
                                  final FeatureFlagClient featureFlagClient,
                                  final ReplicationAirbyteMessageEventPublishingHelper replicationAirbyteMessageEventPublishingHelper,
                                  final MetricClient metricClient,
                                  final WorkloadApiClient workloadApiClient,
                                  final TrackingClient trackingClient,
                                  final StreamStatusCompletionTracker streamStatusCompletionTracker,
                                  final StreamStatusTrackerFactory streamStatusTrackerFactory,
                                  final RecordMapper recordMapper,
                                  final DestinationCatalogGenerator destinationCatalogGenerator) {
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.gsonPksExtractor = gsonPksExtractor;
    this.airbyteApiClient = airbyteApiClient;
    this.syncPersistenceFactory = syncPersistenceFactory;
    this.replicationAirbyteMessageEventPublishingHelper = replicationAirbyteMessageEventPublishingHelper;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
    this.workloadApiClient = workloadApiClient;
    this.trackingClient = trackingClient;
    this.streamStatusCompletionTracker = streamStatusCompletionTracker;
    this.streamStatusTrackerFactory = streamStatusTrackerFactory;
    this.recordMapper = recordMapper;
    this.destinationCatalogGenerator = destinationCatalogGenerator;
  }

  /**
   * Create a ReplicationWorker.
   */
  public BufferedReplicationWorker create(final ReplicationInput replicationInput,
                                          final JobRunConfig jobRunConfig,
                                          final IntegrationLauncherConfig sourceLauncherConfig,
                                          final IntegrationLauncherConfig destinationLauncherConfig,
                                          final VoidCallable onReplicationRunning,
                                          final String workloadId)
      throws IOException {
    final UUID sourceDefinitionId = airbyteApiClient.getSourceApi().getSource(
        new SourceIdRequestBody(replicationInput.getSourceId())).getSourceDefinitionId();
    final HeartbeatMonitor heartbeatMonitor = createHeartbeatMonitor(sourceDefinitionId, airbyteApiClient.getSourceDefinitionApi());
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = createHeartbeatTimeoutChaperone(heartbeatMonitor,
        featureFlagClient, replicationInput, sourceLauncherConfig.getDockerImage(), metricClient);
    final DestinationTimeoutMonitor destinationTimeout = createDestinationTimeout(featureFlagClient, replicationInput, metricClient);
    final RecordSchemaValidator recordSchemaValidator = createRecordSchemaValidator(replicationInput);

    log.info("Setting up source with image {}.", replicationInput.getSourceLauncherConfig().getDockerImage());
    final boolean printLongRecordPks = featureFlagClient.boolVariation(PrintLongRecordPks.INSTANCE,
        new Multi(List.of(
            new Connection(sourceLauncherConfig.getConnectionId()),
            new Workspace(sourceLauncherConfig.getWorkspaceId()))));
    final var invalidLineConfig = new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(printLongRecordPks);

    // reset jobs use an empty source to induce resetting all data in destination.
    final var airbyteSource = replicationInput.getIsReset()
        ? new EmptyAirbyteSource(replicationInput.getNamespaceDefinition() == JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
        : new LocalContainerAirbyteSource(
            heartbeatMonitor,
            getStreamFactory(sourceLauncherConfig, replicationInput.getCatalog(), SOURCE_LOG_MDC_BUILDER, invalidLineConfig),
            new MessageMetricsTracker(metricClient),
            ContainerIOHandle.source());

    log.info("Setting up destination with image {}.", replicationInput.getDestinationLauncherConfig().getDockerImage());
    final AirbyteMessageBufferedWriterFactory messageWriterFactory =
        new VersionedAirbyteMessageBufferedWriterFactory(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
            Optional.of(replicationInput.getCatalog()));

    final var airbyteDestination = new LocalContainerAirbyteDestination(
        getStreamFactory(destinationLauncherConfig, replicationInput.getCatalog(), DESTINATION_LOG_MDC_BUILDER, invalidLineConfig),
        new MessageMetricsTracker(metricClient),
        messageWriterFactory,
        destinationTimeout,
        ContainerIOHandle.dest(),
        replicationInput.getUseFileTransfer());

    final WorkerMetricReporter metricReporter = new WorkerMetricReporter(metricClient, sourceLauncherConfig.getDockerImage());

    final AnalyticsMessageTracker analyticsMessageTracker = new AnalyticsMessageTracker(trackingClient);

    final FieldSelector fieldSelector =
        createFieldSelector(recordSchemaValidator, metricReporter, featureFlagClient, replicationInput.getWorkspaceId(), sourceDefinitionId);

    log.info("Setting up replication worker...");
    final SyncPersistence syncPersistence = createSyncPersistence(syncPersistenceFactory, replicationInput, sourceLauncherConfig);
    final AirbyteMessageTracker messageTracker = createMessageTracker(syncPersistence, replicationInput, featureFlagClient);

    return createReplicationWorker(airbyteSource, airbyteDestination, messageTracker,
        syncPersistence, recordSchemaValidator, fieldSelector, heartbeatTimeoutChaperone,
        featureFlagClient, jobRunConfig, replicationInput, replicationAirbyteMessageEventPublishingHelper,
        onReplicationRunning, destinationTimeout, workloadApiClient, analyticsMessageTracker,
        workloadId, airbyteApiClient, streamStatusCompletionTracker, streamStatusTrackerFactory, metricClient, recordMapper,
        destinationCatalogGenerator);
  }

  /**
   * Create HeartbeatMonitor.
   */
  private static HeartbeatMonitor createHeartbeatMonitor(final UUID sourceDefinitionId,
                                                         final SourceDefinitionApi sourceDefinitionApi)
      throws IOException {
    final Long maxSecondsBetweenMessages = sourceDefinitionId != null ? sourceDefinitionApi
        .getSourceDefinition(new SourceDefinitionIdRequestBody(sourceDefinitionId))
        .getMaxSecondsBetweenMessages() : null;

    if (maxSecondsBetweenMessages != null) {
      return new HeartbeatMonitor(Duration.ofSeconds(maxSecondsBetweenMessages));
    }

    // reset jobs use an empty source to induce resetting all data in destination.
    log.warn("An error occurred while fetch the max seconds between messages for this source. We are using a default of 24 hours");
    return new HeartbeatMonitor(Duration.ofSeconds(TimeUnit.HOURS.toSeconds(24)));
  }

  /**
   * Get HeartbeatTimeoutChaperone.
   */
  private static HeartbeatTimeoutChaperone createHeartbeatTimeoutChaperone(final HeartbeatMonitor heartbeatMonitor,
                                                                           final FeatureFlagClient featureFlagClient,
                                                                           final ReplicationInput replicationInput,
                                                                           final String sourceDockerImage,
                                                                           final MetricClient metricClient) {
    return new HeartbeatTimeoutChaperone(heartbeatMonitor,
        HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
        featureFlagClient,
        replicationInput.getWorkspaceId(),
        replicationInput.getConnectionId(),
        sourceDockerImage,
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
  private AirbyteMessageTracker createMessageTracker(final SyncPersistence syncPersistence,
                                                     final ReplicationInput replicationInput,
                                                     final FeatureFlagClient featureFlagClient) {
    final Context flagContext = getFeatureFlagContext(replicationInput);
    final ReplicationFeatureFlagReader replicationFeatureFlagReader = new ReplicationFeatureFlagReader(featureFlagClient, flagContext);
    final var ffs = replicationFeatureFlagReader.readReplicationFeatureFlags();
    syncPersistence.setReplicationFeatureFlags(ffs);

    return new AirbyteMessageTracker(syncPersistence, ffs.logStateMsgs(), ffs.logConnectorMsgs(),
        replicationInput.getSourceLauncherConfig().getDockerImage(),
        replicationInput.getDestinationLauncherConfig().getDockerImage());
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
  private static BufferedReplicationWorker createReplicationWorker(final AirbyteSource source,
                                                                   final AirbyteDestination destination,
                                                                   final AirbyteMessageTracker messageTracker,
                                                                   final SyncPersistence syncPersistence,
                                                                   final RecordSchemaValidator recordSchemaValidator,
                                                                   final FieldSelector fieldSelector,
                                                                   final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone,
                                                                   final FeatureFlagClient featureFlagClient,
                                                                   final JobRunConfig jobRunConfig,
                                                                   final ReplicationInput replicationInput,
                                                                   final ReplicationAirbyteMessageEventPublishingHelper replEventPublishingHelper,
                                                                   final VoidCallable onReplicationRunning,
                                                                   final DestinationTimeoutMonitor destinationTimeout,
                                                                   final WorkloadApiClient workloadApiClient,
                                                                   final AnalyticsMessageTracker analyticsMessageTracker,
                                                                   final String workloadId,
                                                                   final AirbyteApiClient airbyteApiClient,
                                                                   final StreamStatusCompletionTracker streamStatusCompletionTracker,
                                                                   final StreamStatusTrackerFactory streamStatusTrackerFactory,
                                                                   final MetricClient metricClient,
                                                                   final RecordMapper recordMapper,
                                                                   final DestinationCatalogGenerator destinationCatalogGenerator) {
    final Context flagContext = getFeatureFlagContext(replicationInput);

    final int bufferSize = featureFlagClient.intVariation(ReplicationBufferOverride.INSTANCE, flagContext);
    final BufferConfiguration bufferConfiguration =
        bufferSize > 0 ? BufferConfiguration.withBufferSize(bufferSize) : BufferConfiguration.withDefaultConfiguration();

    return buildReplicationWorkerInstance(
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
        replEventPublishingHelper,
        onReplicationRunning,
        destinationTimeout,
        workloadApiClient,
        analyticsMessageTracker,
        workloadId,
        airbyteApiClient,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory,
        bufferConfiguration,
        metricClient,
        replicationInput,
        recordMapper,
        featureFlagClient,
        destinationCatalogGenerator);
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

  private static BufferedReplicationWorker buildReplicationWorkerInstance(final String jobId,
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
                                                                          final ReplicationAirbyteMessageEventPublishingHelper msgEventPublisher,
                                                                          final VoidCallable onReplicationRunning,
                                                                          final DestinationTimeoutMonitor destinationTimeout,
                                                                          final WorkloadApiClient workloadApiClient,
                                                                          final AnalyticsMessageTracker analyticsMessageTracker,
                                                                          final String workloadId,
                                                                          final AirbyteApiClient airbyteApiClient,
                                                                          final StreamStatusCompletionTracker streamStatusCompletionTracker,
                                                                          final StreamStatusTrackerFactory streamStatusTrackerFactory,
                                                                          final BufferConfiguration bufferConfiguration,
                                                                          final MetricClient metricClient,
                                                                          final ReplicationInput replicationInput,
                                                                          final RecordMapper recordMapper,
                                                                          final FeatureFlagClient featureFlagClient,
                                                                          final DestinationCatalogGenerator destinationCatalogGenerator) {
    final ReplicationWorkerHelper replicationWorkerHelper =
        new ReplicationWorkerHelper(fieldSelector, mapper, messageTracker, syncPersistence,
            msgEventPublisher, new ThreadedTimeTracker(), onReplicationRunning, workloadApiClient,
            analyticsMessageTracker, workloadId, airbyteApiClient, streamStatusCompletionTracker,
            streamStatusTrackerFactory, recordMapper, featureFlagClient, destinationCatalogGenerator);

    return new BufferedReplicationWorker(jobId, attempt, source, destination, syncPersistence, recordSchemaValidator,
        srcHeartbeatTimeoutChaperone, replicationFeatureFlagReader, replicationWorkerHelper, destinationTimeout, streamStatusCompletionTracker,
        bufferConfiguration, metricClient, replicationInput);

  }

  /**
   * Create SyncPersistence.
   */
  private static SyncPersistence createSyncPersistence(final SyncPersistenceFactory syncPersistenceFactory,
                                                       final ReplicationInput replicationInput,
                                                       final IntegrationLauncherConfig sourceLauncherConfig) {
    return syncPersistenceFactory.get(replicationInput.getConnectionId(), replicationInput.getWorkspaceId(),
        Long.parseLong(sourceLauncherConfig.getJobId()),
        sourceLauncherConfig.getAttemptId().intValue(), replicationInput.getCatalog());
  }

  private AirbyteStreamFactory getStreamFactory(final IntegrationLauncherConfig launcherConfig,
                                                final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                final MdcScope.Builder mdcScopeBuilder,
                                                final VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration invalidLineFailureConfiguration) {
    return new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, launcherConfig.getProtocolVersion(),
        Optional.of(launcherConfig.getConnectionId()), Optional.of(configuredAirbyteCatalog), mdcScopeBuilder,
        invalidLineFailureConfiguration, gsonPksExtractor);
  }

}
