/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Singleton
data class ReplicationWorkerContext(
  @Value("\${airbyte.job-id}") val jobId: Long,
  @Named("attemptId") val attempt: Int,
  val bufferConfiguration: BufferConfiguration,
  val replicationWorkerHelper: ReplicationWorkerHelper,
  val replicationWorkerState: ReplicationWorkerState,
  val streamStatusCompletionTracker: StreamStatusCompletionTracker,
)
