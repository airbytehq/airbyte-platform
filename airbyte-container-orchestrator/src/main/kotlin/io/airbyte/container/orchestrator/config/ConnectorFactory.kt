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
import io.airbyte.container.orchestrator.tracker.MessageMetricsTracker
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteMessageBufferedWriterFactory
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.dest
import io.airbyte.container.orchestrator.worker.io.ContainerIOHandle.Companion.source
import io.airbyte.container.orchestrator.worker.io.DestinationTimeoutMonitor
import io.airbyte.container.orchestrator.worker.io.EmptyAirbyteSource
import io.airbyte.container.orchestrator.worker.io.HeartbeatMonitor
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteDestination
import io.airbyte.container.orchestrator.worker.io.LocalContainerAirbyteSource
import io.airbyte.featureflag.PrintLongRecordPks
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
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
    messageMetricsTracker: MessageMetricsTracker,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  ): AirbyteDestination =
    LocalContainerAirbyteDestination(
      streamFactory = destinationStreamFactory,
      messageMetricsTracker = messageMetricsTracker,
      messageWriterFactory = messageWriterFactory,
      destinationTimeoutMonitor = airbyteDestinationMonitor,
      containerIOHandle = dest(),
      flushImmediately = replicationInput.useFileTransfer,
    )

  @Singleton
  fun airbyteSource(
    heartbeatMonitor: HeartbeatMonitor,
    messageMetricsTracker: MessageMetricsTracker,
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
    @Named("sourceStreamFactory") sourceStreamFactory: AirbyteStreamFactory,
  ): AirbyteSource =
    if (replicationInput.isReset) {
      EmptyAirbyteSource(replicationInput.namespaceDefinition == JobSyncConfig.NamespaceDefinitionType.CUSTOMFORMAT)
    } else {
      LocalContainerAirbyteSource(
        heartbeatMonitor = heartbeatMonitor,
        streamFactory = sourceStreamFactory,
        messageMetricsTracker = messageMetricsTracker,
        containerIOHandle = source(),
      )
    }

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
  @Named("sourceMdcScopeBuilder")
  fun sourceMdcScopeBuilder(): MdcScope.Builder =
    MdcScope
      .Builder()
      .setExtraMdcEntries(LogSource.SOURCE.toMdc())

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
