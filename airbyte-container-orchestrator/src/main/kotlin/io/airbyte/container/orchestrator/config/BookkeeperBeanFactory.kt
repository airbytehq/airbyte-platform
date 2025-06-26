/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.worker.DestinationReader
import io.airbyte.container.orchestrator.worker.MessageProcessor
import io.airbyte.container.orchestrator.worker.ReplicationWorkerHelper
import io.airbyte.container.orchestrator.worker.ReplicationWorkerState
import io.airbyte.container.orchestrator.worker.SourceReader
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.models.ArchitectureConstants.BOOKKEEPER
import io.airbyte.workers.models.ArchitectureConstants.PLATFORM_MODE
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * Defines and creates any singletons that are only required when running in [BOOKKEEPER] mode.
 * <p />
 * Any singletons defined/created in this factory will only be available if the [PLATFORM_MODE]
 * environment variable contains the value [BOOKKEEPER].
 */
@Factory
@Requires(property = PLATFORM_MODE, value = BOOKKEEPER)
class BookkeeperBeanFactory {
  @Singleton
  @Named("syncReplicationJobs")
  fun syncReplicationJobs(
    destination: AirbyteDestination,
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
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
      sourceQueue = sourceMessageQueue,
    ),
    DestinationReader(
      destination = destination,
      replicationWorkerHelper = replicationWorkerHelper,
      replicationWorkerState = replicationWorkerState,
    ),
  )
}
