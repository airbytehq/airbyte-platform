/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.protocol.VersionedProtocolSerializer;
import io.airbyte.config.SyncResourceRequirements;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.DestinationCallsElapsedTimeTrackingEnabled;
import io.airbyte.featureflag.FailSyncIfTooBig;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.DefaultAirbyteDestination;
import io.airbyte.workers.internal.DefaultAirbyteSource;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.VersionedAirbyteMessageBufferedWriterFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Factory to help create IntegrationLaunchers.
 */
@Singleton
public class AirbyteIntegrationLauncherFactory {

  private final ProcessFactory processFactory;
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final FeatureFlags featureFlags;
  private final FeatureFlagClient featureFlagClient;

  public AirbyteIntegrationLauncherFactory(final ProcessFactory processFactory,
                                           final AirbyteMessageSerDeProvider serDeProvider,
                                           final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                           final FeatureFlags featureFlags,
                                           final FeatureFlagClient featureFlagClient) {
    this.processFactory = processFactory;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
  }

  /**
   * Create an IntegrationLauncher from a given configuration.
   *
   * @param launcherConfig the configuration of the integration.
   * @param syncResourceRequirements the resource requirements for the integration.
   * @return an AirbyteIntegrationLauncher.
   */
  public IntegrationLauncher createIntegrationLauncher(final IntegrationLauncherConfig launcherConfig,
                                                       final SyncResourceRequirements syncResourceRequirements) {
    return new AirbyteIntegrationLauncher(
        launcherConfig.getJobId(),
        Math.toIntExact(launcherConfig.getAttemptId()),
        launcherConfig.getConnectionId(),
        launcherConfig.getWorkspaceId(),
        launcherConfig.getDockerImage(),
        processFactory,
        null,
        syncResourceRequirements,
        launcherConfig.getAllowedHosts(),
        // At this moment, if either source or destination is from custom connector image, we will put all
        // jobs into isolated pool to run.
        launcherConfig.getIsCustomConnector(),
        featureFlags,
        Optional.ofNullable(launcherConfig.getAdditionalEnvironmentVariables())
            .orElse(Collections.emptyMap()));
  }

  /**
   * Create an AirbyteSource from a given configuration. *
   *
   * @param sourceLauncherConfig the configuration of the source.
   * @param configuredAirbyteCatalog the configuredAirbyteCatalog of the Connection the source.
   * @param heartbeatMonitor an instance of HeartbeatMonitor to use for the AirbyteSource.
   * @return an AirbyteSource.
   */
  public AirbyteSource createAirbyteSource(final IntegrationLauncherConfig sourceLauncherConfig,
                                           final SyncResourceRequirements syncResourceRequirements,
                                           final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                           final HeartbeatMonitor heartbeatMonitor) {
    final IntegrationLauncher sourceLauncher = createIntegrationLauncher(sourceLauncherConfig, syncResourceRequirements);

    final boolean failTooLongRecords = featureFlagClient.boolVariation(FailSyncIfTooBig.INSTANCE,
        new Multi(List.of(
            new Connection(sourceLauncherConfig.getConnectionId()),
            new Workspace(sourceLauncherConfig.getWorkspaceId()))));

    return new DefaultAirbyteSource(sourceLauncher,
        getStreamFactory(sourceLauncherConfig, configuredAirbyteCatalog, SourceException.class, DefaultAirbyteSource.CONTAINER_LOG_MDC_BUILDER,
            failTooLongRecords),
        heartbeatMonitor,
        getProtocolSerializer(sourceLauncherConfig),
        featureFlags);
  }

  /**
   * Create an AirbyteDestination from a given configuration.
   *
   * @param destinationLauncherConfig the configuration of the destination.
   * @param configuredAirbyteCatalog the configuredAirbyteCatalog of the Connection the destination.
   * @return an AirbyteDestination.
   */
  public AirbyteDestination createAirbyteDestination(final IntegrationLauncherConfig destinationLauncherConfig,
                                                     final SyncResourceRequirements syncResourceRequirements,
                                                     final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                     final MetricClient metricClient,
                                                     final ReplicationInput replicationInput,
                                                     final FeatureFlagClient featureFlagClient) {
    final boolean destinationElapsedTimeTrackingEnabled = featureFlagClient.boolVariation(DestinationCallsElapsedTimeTrackingEnabled.INSTANCE,
        new Workspace(replicationInput.getWorkspaceId()));

    final IntegrationLauncher destinationLauncher = createIntegrationLauncher(destinationLauncherConfig, syncResourceRequirements);
    return new DefaultAirbyteDestination(destinationLauncher,
        getStreamFactory(destinationLauncherConfig, configuredAirbyteCatalog, DestinationException.class,
            DefaultAirbyteDestination.CONTAINER_LOG_MDC_BUILDER, false),
        new VersionedAirbyteMessageBufferedWriterFactory(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
            Optional.of(configuredAirbyteCatalog)),
        getProtocolSerializer(destinationLauncherConfig), destinationElapsedTimeTrackingEnabled, metricClient, toConnectionAttrs(replicationInput));
  }

  private MetricAttribute[] toConnectionAttrs(final ReplicationInput replicationInput) {
    final var attrs = new ArrayList<MetricAttribute>();
    if (replicationInput.getConnectionId() != null) {
      attrs.add(new MetricAttribute(MetricTags.CONNECTION_ID, replicationInput.getConnectionId().toString()));
    }

    if (replicationInput.getDestinationId() != null) {
      attrs.add(new MetricAttribute(MetricTags.DESTINATION_ID, replicationInput.getDestinationId().toString()));
    }

    if (replicationInput.getSourceId() != null) {
      attrs.add(new MetricAttribute(MetricTags.SOURCE_ID, replicationInput.getSourceId().toString()));
    }

    if (replicationInput.getWorkspaceId() != null) {
      attrs.add(new MetricAttribute(MetricTags.WORKSPACE_ID, replicationInput.getWorkspaceId().toString()));
    }

    return attrs.toArray(new MetricAttribute[0]);
  }

  private VersionedProtocolSerializer getProtocolSerializer(final IntegrationLauncherConfig launcherConfig) {
    return migratorFactory.getProtocolSerializer(launcherConfig.getProtocolVersion());
  }

  private AirbyteStreamFactory getStreamFactory(final IntegrationLauncherConfig launcherConfig,
                                                final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                final Class<? extends RuntimeException> exceptionClass,
                                                final MdcScope.Builder mdcScopeBuilder,
                                                final boolean failTooLongRecords) {
    return new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, launcherConfig.getProtocolVersion(),
        Optional.of(configuredAirbyteCatalog), mdcScopeBuilder, Optional.of(exceptionClass), failTooLongRecords);
  }

}
