/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.protocol.VersionedProtocolSerializer;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
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
import java.util.Collections;
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

  public AirbyteIntegrationLauncherFactory(final ProcessFactory processFactory,
                                           final AirbyteMessageSerDeProvider serDeProvider,
                                           final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                           final FeatureFlags featureFlags) {
    this.processFactory = processFactory;
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.featureFlags = featureFlags;
  }

  /**
   * Create an IntegrationLauncher from a given configuration.
   *
   * @param launcherConfig the configuration of the integration.
   * @param resourceRequirements the resource requirements for the integration.
   * @return an AirbyteIntegrationLauncher.
   */
  public IntegrationLauncher createIntegrationLauncher(final IntegrationLauncherConfig launcherConfig,
                                                       final ResourceRequirements resourceRequirements) {
    return new AirbyteIntegrationLauncher(
        launcherConfig.getJobId(),
        Math.toIntExact(launcherConfig.getAttemptId()),
        launcherConfig.getConnectionId(),
        launcherConfig.getWorkspaceId(),
        launcherConfig.getDockerImage(),
        processFactory,
        resourceRequirements,
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
   * @param resourceRequirements the resource requirements for the source.
   * @param configuredAirbyteCatalog the configuredAirbyteCatalog of the Connection the source.
   * @param heartbeatMonitor an instance of HeartbeatMonitor to use for the AirbyteSource.
   * @return an AirbyteSource.
   */
  public AirbyteSource createAirbyteSource(final IntegrationLauncherConfig sourceLauncherConfig,
                                           final ResourceRequirements resourceRequirements,
                                           final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                           final HeartbeatMonitor heartbeatMonitor) {
    final IntegrationLauncher sourceLauncher = createIntegrationLauncher(sourceLauncherConfig, resourceRequirements);

    return new DefaultAirbyteSource(sourceLauncher,
        getStreamFactory(sourceLauncherConfig, configuredAirbyteCatalog, SourceException.class, DefaultAirbyteSource.CONTAINER_LOG_MDC_BUILDER),
        heartbeatMonitor,
        getProtocolSerializer(sourceLauncherConfig),
        featureFlags);
  }

  /**
   * Create an AirbyteDestination from a given configuration.
   *
   * @param destinationLauncherConfig the configuration of the destination.
   * @param resourceRequirements the resource requirements for the destination.
   * @param configuredAirbyteCatalog the configuredAirbyteCatalog of the Connection the destination.
   * @return an AirbyteDestination.
   */
  public AirbyteDestination createAirbyteDestination(final IntegrationLauncherConfig destinationLauncherConfig,
                                                     final ResourceRequirements resourceRequirements,
                                                     final ConfiguredAirbyteCatalog configuredAirbyteCatalog) {
    final IntegrationLauncher destinationLauncher = createIntegrationLauncher(destinationLauncherConfig, resourceRequirements);
    return new DefaultAirbyteDestination(destinationLauncher,
        getStreamFactory(destinationLauncherConfig, configuredAirbyteCatalog, DestinationException.class,
            DefaultAirbyteDestination.CONTAINER_LOG_MDC_BUILDER),
        new VersionedAirbyteMessageBufferedWriterFactory(serDeProvider, migratorFactory, destinationLauncherConfig.getProtocolVersion(),
            Optional.of(configuredAirbyteCatalog)),
        getProtocolSerializer(destinationLauncherConfig));
  }

  private VersionedProtocolSerializer getProtocolSerializer(final IntegrationLauncherConfig launcherConfig) {
    return migratorFactory.getProtocolSerializer(launcherConfig.getProtocolVersion());
  }

  private AirbyteStreamFactory getStreamFactory(final IntegrationLauncherConfig launcherConfig,
                                                final ConfiguredAirbyteCatalog configuredAirbyteCatalog,
                                                final Class<? extends RuntimeException> exceptionClass,
                                                final MdcScope.Builder mdcScopeBuilder) {
    return new VersionedAirbyteStreamFactory<>(serDeProvider, migratorFactory, launcherConfig.getProtocolVersion(),
        Optional.of(configuredAirbyteCatalog), mdcScopeBuilder, Optional.of(exceptionClass));
  }

}
