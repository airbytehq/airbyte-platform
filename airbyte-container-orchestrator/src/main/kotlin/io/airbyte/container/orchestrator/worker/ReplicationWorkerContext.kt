/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.micronaut.runtime.AirbyteContextConfig
import jakarta.inject.Singleton

@Singleton
data class ReplicationWorkerContext(
  val airbyteContextConfig: AirbyteContextConfig,
  val bufferConfiguration: BufferConfiguration,
  val replicationWorkerHelper: ReplicationWorkerHelper,
  val replicationWorkerState: ReplicationWorkerState,
  val streamStatusCompletionTracker: StreamStatusCompletionTracker,
) {
  fun getAttempt() = airbyteContextConfig.attemptId

  fun getJobId() = airbyteContextConfig.jobId
}
