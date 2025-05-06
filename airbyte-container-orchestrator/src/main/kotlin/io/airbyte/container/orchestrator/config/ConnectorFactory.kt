/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.config.JobSyncConfig
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.dest
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.source
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.InMemoryDummyAirbyteDestination
import io.airbyte.container.orchestrator.worker.io.InMemoryDummyAirbyteSource
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteDestination
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteSource
import io.airbyte.featureflag.PrintLongRecordPks
import io.airbyte.featureflag.SingleContainerTest
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteMessageBufferedWriterFactory
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.EmptyAirbyteSource
import io.airbyte.workers.internal.HeartbeatMonitor
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone
import io.airbyte.workers.internal.MessageMetricsTracker
import io.airbyte.workers.internal.VersionedAirbyteMessageBufferedWriterFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Optional

@Factory
class ConnectorFactory {
  @Singleton
  @Named("destinationStreamFactory")
  fun destinationStreamFactory(
    gsonPksExtractor: GsonPksExtractor,
    invalidLineFailureConfiguration: VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration,
    @Named("destinationMdcScopeBuilder") mdcScopeBuilder: MdcScope.Builder,
    metricClient: MetricClient,
    migratorFactory: AirbyteProtocolVersionedMigratorFactory,
    replicationInput: ReplicationInput,
    serDeProvider: AirbyteMessageSerDeProvider,
  ): AirbyteStreamFactory =
    VersionedAirbyteStreamFactory<Any>(
      serDeProvider,
      migratorFactory,
      replicationInput.destinationLauncherConfig.protocolVersion ?: AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
      Optional.of(replicationInput.destinationLauncherConfig.connectionId),
      Optional.of(replicationInput.catalog),
      mdcScopeBuilder,
      invalidLineFailureConfiguration,
      gsonPksExtractor,
      metricClient,
    )

  @Singleton
  fun airbyteDestination(
    airbyteDestinationMonitor: DestinationTimeoutMonitor,
    @Named("destinationStreamFactory") destinationStreamFactory: AirbyteStreamFactory,
    messageWriterFactory: AirbyteMessageBufferedWriterFactory,
    metricClient: MetricClient,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  ): AirbyteDestination =
    if (replicationInputFeatureFlagReader.read(SingleContainerTest)) {
      InMemoryDummyAirbyteDestination()
    } else {
      LocalContainerAirbyteDestination(
        streamFactory = destinationStreamFactory,
        messageMetricsTracker = MessageMetricsTracker(metricClient),
        messageWriterFactory = messageWriterFactory,
        destinationTimeoutMonitor = airbyteDestinationMonitor,
        containerIOHandle = dest(),
        flushImmediately = replicationInput.useFileTransfer,
      )
    }

  @Singleton
  fun airbyteSource(
    heartbeatMonitor: HeartbeatMonitor,
    metricClient: MetricClient,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
    @Named("sourceStreamFactory") sourceStreamFactory: AirbyteStreamFactory,
  ): AirbyteSource =
    if (replicationInputFeatureFlagReader.read(SingleContainerTest)) {
      InMemoryDummyAirbyteSource()
    } else if (replicationInput.isReset) {
      EmptyAirbyteSource(replicationInput.getNamespaceDefinition() == JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
    } else {
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        streamFactory = sourceStreamFactory,
        messageMetricsTracker = MessageMetricsTracker(metricClient),
        containerIOHandle = source(),
      )
    }

  @Singleton
  fun airbyteSourceMonitor(replicationInput: ReplicationInput) =
    HeartbeatMonitor(Duration.ofSeconds(replicationInput.heartbeatConfig.maxSecondsBetweenMessages))

  @Singleton
  @Named("destinationMdcScopeBuilder")
  fun destinationMdcScopeBuilder(): MdcScope.Builder =
    MdcScope
      .Builder()
      .setExtraMdcEntries(LogSource.DESTINATION.toMdc())

  @Singleton
  fun invalidLineFailureConfiguration(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader) =
    VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(
      replicationInputFeatureFlagReader.read(PrintLongRecordPks),
    )

  @Singleton
  fun messageWriterFactory(
    replicationInput: ReplicationInput,
    serDeProvider: AirbyteMessageSerDeProvider,
    migratorFactory: AirbyteProtocolVersionedMigratorFactory,
  ): AirbyteMessageBufferedWriterFactory =
    VersionedAirbyteMessageBufferedWriterFactory(
      serDeProvider,
      migratorFactory,
      replicationInput.destinationLauncherConfig.getProtocolVersion(),
      Optional.of(replicationInput.getCatalog()),
    )

  @Singleton
  @Named("sourceMdcScopeBuilder")
  fun sourceMdcScopeBuilder(): MdcScope.Builder =
    MdcScope
      .Builder()
      .setExtraMdcEntries(LogSource.SOURCE.toMdc())

  @Singleton
  fun sourceHeartbeatTimeoutChaperone(
    heartbeatMonitor: HeartbeatMonitor,
    metricClient: MetricClient,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  ) = HeartbeatTimeoutChaperone(
    heartbeatMonitor,
    HeartbeatTimeoutChaperone.DEFAULT_TIMEOUT_CHECK_DURATION,
    replicationInputFeatureFlagReader,
    replicationInput.connectionId,
    replicationInput.sourceLauncherConfig.dockerImage,
    metricClient,
  )

  @Singleton
  @Named("sourceStreamFactory")
  fun sourceStreamFactory(
    gsonPksExtractor: GsonPksExtractor,
    invalidLineFailureConfiguration: VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration,
    @Named("sourceMdcScopeBuilder") mdcScopeBuilder: MdcScope.Builder,
    metricClient: MetricClient,
    migratorFactory: AirbyteProtocolVersionedMigratorFactory,
    replicationInput: ReplicationInput,
    serDeProvider: AirbyteMessageSerDeProvider,
  ): AirbyteStreamFactory =
    VersionedAirbyteStreamFactory<Any>(
      serDeProvider,
      migratorFactory,
      replicationInput.sourceLauncherConfig.protocolVersion ?: AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION,
      Optional.of(replicationInput.sourceLauncherConfig.connectionId),
      Optional.of(replicationInput.catalog),
      mdcScopeBuilder,
      invalidLineFailureConfiguration,
      gsonPksExtractor,
      metricClient,
    )
}
