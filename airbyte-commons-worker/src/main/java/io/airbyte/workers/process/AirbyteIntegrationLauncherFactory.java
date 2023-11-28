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
import io.airbyte.featureflag.FailMissingPks;
import io.airbyte.featureflag.FailSyncIfTooBig;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.PrintLongRecordPks;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.DefaultAirbyteDestination;
import io.airbyte.workers.internal.DefaultAirbyteSource;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.VersionedAirbyteMessageBufferedWriterFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import jakarta.inject.Singleton;
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
  private final GsonPksExtractor gsonPksExtractor;

  public AirbyteIntegrationLauncherFactory(final ProcessFactory processFactory,
                                           final AirbyteMessageSerDeProvider serDeProvider,
                                           final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                           final FeatureFlags featureFlags,
                                           final FeatureFlagClient featureFlagClient,
                                           final GsonPksExtractor gsonPksExtractor) {
    this.processFactory = processFactory;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
    this.featureFlagClient = featureFlagClient;
    this.gsonPksExtractor = gsonPksExtractor;
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
        launcherConfig.getAdditionalEnvironmentVariables() == null ? Collections.emptyMap() : launcherConfig.getAdditionalEnvironmentVariables(),
        launcherConfig.getAdditionalLabels() == null ? Collections.emptyMap() : launcherConfig.getAdditionalLabels());
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

    final boolean failMissingPks = featureFlagClient.boolVariation(FailMissingPks.INSTANCE,
        new Multi(List.of(
            new Connection(sourceLauncherConfig.getConnectionId()),
            new Workspace(sourceLauncherConfig.getWorkspaceId()))));

    final boolean printLongRecordPks = featureFlagClient.boolVariation(PrintLongRecordPks.INSTANCE,
        new Multi(List.of(
            new Connection(sourceLauncherConfig.getConnectionId()),
            new Workspace(sourceLauncherConfig.getWorkspaceId()))));

    return new DefaultAirbyteSource(sourceLauncher,
        getStreamFactory(sourceLauncherConfig, configuredAirbyteCatalog, SourceException.class, DefaultAirbyteSource.CONTAINER_LOG_MDC_BUILDER,
            new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(
                failTooLongRecords,
                failMissingPks,
                printLongRecordPks)),
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
                                                     final DestinationTimeoutMonitor destinationTimeoutMonitor) {

    final IntegrationLauncher destinationLauncher = createIntegrationLauncher(destinationLauncherConfig, syncResourceRequirements);
    return new DefaultAirbyteDestination(destinationLauncher,
        getStreamFactory(destinationLauncherConfig,
            configuredAirbyteCatalog,
            DestinationException.class,
            DefaultAirbyteDestination.CONTAINER_LOG_MDC_BUILDER,
            new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false, false, false)),
        new VersionedAirbyteMessageBufferedWriterFactory(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
            Optional.of(configuredAirbyteCatalog)),
        getProtocolSerializer(destinationLauncherConfig), destinationTimeoutMonitor);
  }

  private VersionedProtocolSerializer getProtocolSerializer(final IntegrationLauncherConfig launcherConfig) {
    return migratorFactory.getProtocolSerializer(launcherConfig.getProtocolVersion());
  }

  private AirbyteStreamFactory getStreamFactory(final IntegrationLauncherConfig launcherConfig,
                                                final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                final Class<? extends RuntimeException> exceptionClass,
                                                final MdcScope.Builder mdcScopeBuilder,
                                                final VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration invalidLineFailureConfiguration) {
    return new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, launcherConfig.getProtocolVersion(),
        Optional.of(configuredAirbyteCatalog), mdcScopeBuilder, Optional.of(exceptionClass), invalidLineFailureConfiguration, gsonPksExtractor);
  }

}
