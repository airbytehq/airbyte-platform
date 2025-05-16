/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.worker.BufferConfiguration
import io.airbyte.container.orchestrator.worker.DestinationReader
import io.airbyte.container.orchestrator.worker.DestinationStarter
import io.airbyte.container.orchestrator.worker.DestinationWriter
import io.airbyte.container.orchestrator.worker.MessageProcessor
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.ReplicationWorkerContext
import io.airbyte.container.orchestrator.worker.ReplicationWorkerHelper
import io.airbyte.container.orchestrator.worker.ReplicationWorkerState
import io.airbyte.container.orchestrator.worker.SourceReader
import io.airbyte.container.orchestrator.worker.SourceStarter
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.container.orchestrator.worker.withBufferSize
import io.airbyte.container.orchestrator.worker.withDefaultConfiguration
import io.airbyte.featureflag.ReplicationBufferOverride
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.NamespacingMapper
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path

@Factory
class ReplicationFactory {
  @Singleton
  fun bufferConfiguration(replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader): BufferConfiguration {
    val bufferSize = replicationInputFeatureFlagReader.read(ReplicationBufferOverride)
    return if (bufferSize > 0) withBufferSize(bufferSize) else withDefaultConfiguration()
  }

  @Singleton
  fun namespaceMapper(replicationInput: ReplicationInput) =
    NamespacingMapper(
      replicationInput.namespaceDefinition,
      replicationInput.namespaceFormat,
      replicationInput.prefix,
    )

  @Singleton
  fun replicationContext(
    replicationContextProvider: ReplicationContextProvider,
    replicationInput: ReplicationInput,
  ) = replicationContextProvider.provideContext(replicationInput)

  @Singleton
  @Named("onReplicationRunning")
  fun replicationRunningCallback(
    @Named("workloadId") workloadId: String,
  ): VoidCallable = VoidCallable { workloadId }

  @Singleton
  @Named("startReplicationJobs")
  fun startReplicationJobs(
    destination: AirbyteDestination,
    @Named("jobRoot") jobRoot: Path,
    replicationInput: ReplicationInput,
    replicationWorkerContext: ReplicationWorkerContext,
    source: AirbyteSource,
  ) = listOf(
    DestinationStarter(
      destination = destination,
      jobRoot = jobRoot,
      context = replicationWorkerContext,
    ),
    SourceStarter(
      source = source,
      jobRoot = jobRoot,
      replicationInput = replicationInput,
      context = replicationWorkerContext,
    ),
  )

  @Singleton
  @Named("syncReplicationJobs")
  fun syncReplicationJobs(
    destination: AirbyteDestination,
    @Named("destinationMessageQueue") destinationMessageQueue: ClosableChannelQueue<AirbyteMessage>,
    replicationWorkerHelper: ReplicationWorkerHelper,
    replicationWorkerState: ReplicationWorkerState,
    source: AirbyteSource,
    @Named("sourceMessageQueue") sourceMessageQueue: ClosableChannelQueue<AirbyteMessage>,
    streamStatusCompletionTracker: StreamStatusCompletionTracker,
  ) = listOf(
    SourceReader(
      messagesFromSourceQueue = sourceMessageQueue,
      replicationWorkerState = replicationWorkerState,
      replicationWorkerHelper = replicationWorkerHelper,
      source = source,
      streamStatusCompletionTracker = streamStatusCompletionTracker,
    ),
    MessageProcessor(
      destinationQueue = destinationMessageQueue,
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
      sourceQueue = sourceMessageQueue,
    ),
    DestinationWriter(
      source = source,
      destination = destination,
      replicationWorkerState = replicationWorkerState,
      replicationWorkerHelper = replicationWorkerHelper,
      destinationQueue = destinationMessageQueue,
    ),
    DestinationReader(
      destination = destination,
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
    ),
  )

  @Singleton
  @Named("destinationMessageQueue")
  fun destinationMessageQueue(context: ReplicationWorkerContext) =
    ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.destinationMaxBufferSize)

  @Singleton
  @Named("sourceMessageQueue")
  fun sourceMessageQueue(context: ReplicationWorkerContext) = ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.sourceMaxBufferSize)
}
